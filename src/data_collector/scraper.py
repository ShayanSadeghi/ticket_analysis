import logging
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List

import requests
from clickhouse_driver import Client
from config import Config, TableConfig
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AirlinePricingCollector:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = Config.from_yaml_with_env(config_path)
        self.flight_config = TableConfig.from_yaml("./config_flight.yml")
        self.session = self._create_session()
        self.ch_client = None  # Lazy initialization

    def __enter__(self):
        self.ch_client = Client(**self.config.clickhouse.model_dump())
        self.create_tables()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ch_client:
            self.ch_client.disconnect()
        self.session.close()
        return False  # Don't suppress exceptions

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def create_tables(self):
        """Create necessary tables in ClickHouse"""

        # Raw data table
        fields = ", ".join(
            [f"{item.name} {item.field_type}" for item in self.flight_config.fields]
        )
        order_by = ", ".join(self.flight_config.order_by)
        self.ch_client.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.flight_config.table_name}
        ({fields})
        ENGINE = {self.flight_config.engine}
        ORDER BY ({order_by})
        PARTITION BY {self.flight_config.partition_by}
        """)

        logger.info("Tables created/verified")

    def fetch_from_api(self, route: Dict, departure_date: str) -> List[Dict]:
        """
        get flights data for a date
        """
        url = self.config.sources["mrbilit_flights"]["url"]
        headers = self.config.sources["mrbilit_flights"]["headers"]
        payload = self.config.sources["mrbilit_flights"]["default_payload"]

        payload["Routes"] = [
            {
                "OriginCode": route["origin"]["name"],
                "DestinationCode": route["destination"]["name"],
                "DepartureDate": departure_date,
            }
        ]

        response = self.session.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        response = response.json()["Flights"]

        flights = []
        for res in response:
            segment = res[
                "Segments"
            ][
                0
            ]  # I think all routes for now are single route and there is no stop for now :)
            leg = segment["Legs"][0]

            flight_data_main = {
                "timestamp": datetime.now(UTC),
                "route_id": f"{route['origin']['name']}-{route['destination']['name']}",
                "origin": route["origin"]["name"],
                "destination": route["destination"]["name"],
                "flight_id": res["Id"],
                "score": res["Score"],
                "journey_time": segment["TotalTime"],
                "flight_number": leg["FlightNumber"],
                "airline_code": leg["AirlineCode"],
                "airline_name": leg["Airline"]["EnglishTitle"],
                "airline_logo": leg["Airline"]["Logo"],
                "departure_terminal": leg["DepartureTerminal"],
                "departure_datetime": datetime.fromisoformat(leg["DepartureTime"]),
                "flight_status": leg["FlightStatus"],
                "aircraft_title": leg["AirCraft"]["EnglishTitle"],
                "aircraft_short_title": leg["AirCraft"]["ShortTitle"],
                "aircraft_score": leg["AirCraft"]["Score"],
                "source_url": url,
            }
            for flight_class in res["Prices"]:
                adult_fare = next(
                    (
                        fare
                        for fare in flight_class["PassengerFares"]
                        if fare.get("PaxType") == "ADL"
                    ),
                    None,
                )
                if adult_fare:
                    price = adult_fare.get("TotalFare")
                    discount = adult_fare.get("Discount")
                    discount_percent = adult_fare.get("DiscountPercent")

                fligth_data = {
                    **flight_data_main,
                    "price": price,
                    "discount": discount,
                    "discount_percent": discount_percent,
                    "available_seats": flight_class["Capacity"],
                    "booking_class": flight_class["BookingClass"],
                }
                flights.append(fligth_data)

        return flights

    def _batch_insert(self, flight_buffer: List[Dict[str, Any]]) -> None:
        if not flight_buffer:
            return

        columns = [item.name for item in self.flight_config.fields]
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {self.flight_config.table_name} ({columns_str}) VALUES"

        values = [tuple(flight.get(col) for col in columns) for flight in flight_buffer]

        try:
            self.ch_client.execute(query, values)
            logger.info(f"Successfully inserted {len(flight_buffer)} flight records.")
        except Exception as e:
            logger.error(f"Failed to insert batch into ClickHouse: {e}")
            # Optional: Re-raise or handle specific retry logic here
            raise

    def collect_data(self, batch_size: int = 100):
        """Main collection function"""
        scrape_id = str(uuid.uuid4())
        flights_buffer = []

        for route in self.config.routes:
            for days_ahead in self.config.departure_windows:
                departure_date = (datetime.now() + timedelta(days=days_ahead)).strftime(
                    "%Y-%m-%d"
                )
                # departure_date_jalali = jdatetime.datetime.fromgregorian(datetime=departure_date).strftime('%Y-%m-%d')
                try:
                    flights = self.fetch_from_api(route, departure_date)
                    # Fetch data from API
                    if route["reverse"]:
                        route_rev = {
                            "origin": route["destination"],
                            "destination": route["origin"],
                        }
                        flights.extend(self.fetch_from_api(route_rev, departure_date))

                    # Insert into ClickHouse
                    for flight in flights:
                        flight["scrape_id"] = scrape_id
                        flights_buffer.append(flight)

                    if len(flights_buffer) >= batch_size:
                        self._batch_insert(flights_buffer)
                        flights_buffer.clear()

                    time.sleep(self.config.rate_limit_delay)

                    logger.info(
                        f"Collected data for {route['origin']['name']}-{route['destination']['name']} on {departure_date}"
                    )

                    # Respect rate limits
                    time.sleep(self.config.rate_limit_delay)

                except Exception as e:
                    logger.error(
                        f"Error collecting data for route {route['origin']['name']}-{route['destination']['name']}: {e}"
                    )

        if flights_buffer:
            self._batch_insert(flights_buffer)

        logger.info(f"Data collection complete. Scrape ID: {scrape_id}")


if __name__ == "__main__":
    with AirlinePricingCollector() as collector:
        collector.collect_data()
