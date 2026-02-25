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


class TrainPricingCollector:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = Config.from_yaml_with_env(config_path)
        self.train_config = TableConfig.from_yaml("./config_train.yml")
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
            [f"{item.name} {item.field_type}" for item in self.train_config.fields]
        )
        order_by = ", ".join(self.train_config.order_by)
        self.ch_client.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.train_config.table_name}
        ({fields})
        ENGINE = {self.train_config.engine}
        ORDER BY ({order_by})
        PARTITION BY {self.train_config.partition_by}
        """)

        logger.info("Tables created/verified")

    def fetch_from_api(self, route: Dict, departure_date: str) -> List[Dict]:
        """
        get trains data for a date
        """
        url = self.config.sources["mrbilit_trains"]["url"].format(
            origin=route["origin"]["codes"]["mrbilit_trains"],
            destination=route["destination"]["codes"]["mrbilit_trains"],
            date=departure_date,
        )
        headers = self.config.sources["mrbilit_trains"]["headers"]
        response = self.session.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        response = response.json()["trains"]
        trains = []
        for res in response:
            # print(route)
            # print(50*'*')
            # print(res)
            # exit()
            train_data_main = {
                "timestamp": datetime.now(UTC),
                "route_id": f"{route['origin']['name']}-{route['destination']['name']}",
                "origin": route["origin"]["name"],
                "destination": route["destination"]["name"],
                "train_id": res["id"],
                "departure_datetime": datetime.fromisoformat(res["departureTime"]),
                "arrival_datetime": datetime.fromisoformat(res["arrivalTime"]),
                "provider": res["provider"],
                "providerName": res["providerName"],
                "corporationID": res["corporationID"],
                "corporationName": res["corporationName"],
                "source_url": url,
            }
            for train_class in res["prices"][0]["classes"]:
                train_data = {
                    **train_data_main,
                    "price": train_class["price"],
                    "available_seats": train_class["capacity"],
                    "compartment_capacity": train_class["compartmentCapacity"],
                }
                trains.append(train_data)

        return trains

    def _batch_insert(self, train_buffer: List[Dict[str, Any]]) -> None:
        if not train_buffer:
            return

        columns = [item.name for item in self.train_config.fields]
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {self.train_config.table_name} ({columns_str}) VALUES"

        values = [tuple(train.get(col) for col in columns) for train in train_buffer]

        try:
            self.ch_client.execute(query, values)
            logger.info(f"Successfully inserted {len(train_buffer)} train records.")
        except Exception as e:
            logger.error(f"Failed to insert batch into ClickHouse: {e}")
            # Optional: Re-raise or handle specific retry logic here
            raise

    def collect_data(self, batch_size: int = 100):
        """Main collection function"""
        scrape_id = str(uuid.uuid4())
        trains_buffer = []

        for route in self.config.routes:
            for days_ahead in self.config.departure_windows:
                departure_date = (datetime.now() + timedelta(days=days_ahead)).strftime(
                    "%Y-%m-%d"
                )
                # departure_date_jalali = jdatetime.datetime.fromgregorian(datetime=departure_date).strftime('%Y-%m-%d')
                try:
                    trains = self.fetch_from_api(route, departure_date)
                    # Fetch data from API
                    if route["reverse"]:
                        route_rev = {
                            "origin": route["destination"],
                            "destination": route["origin"],
                        }
                        trains.extend(self.fetch_from_api(route_rev, departure_date))

                    # Insert into ClickHouse
                    for train in trains:
                        train["scrape_id"] = scrape_id
                        trains_buffer.append(train)

                    if len(trains_buffer) >= batch_size:
                        self._batch_insert(trains_buffer)
                        trains_buffer.clear()

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

        if trains_buffer:
            self._batch_insert(trains_buffer)

        logger.info(f"Data collection complete. Scrape ID: {scrape_id}")


if __name__ == "__main__":
    with TrainPricingCollector() as collector:
        collector.collect_data()
