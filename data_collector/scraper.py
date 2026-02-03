import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta, UTC
import jdatetime
import time
from typing import Dict, List, Optional
import logging
from clickhouse_driver import Client
import yaml
import json
import random
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AirlinePricingCollector:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize ClickHouse client
        self.ch_client = Client(
            host=self.config['clickhouse']['host'],
            port=self.config['clickhouse']['port'],
            user=self.config['clickhouse']['user'],
            password=self.config['clickhouse']['password'],
            database=self.config['clickhouse']['database']
        )
        
        self.create_tables()
    
    def create_tables(self):
        """Create necessary tables in ClickHouse"""
        
        # Raw data table
        self.ch_client.execute('''
        CREATE TABLE IF NOT EXISTS raw_prices
        (
            timestamp DateTime,
            scrape_id String,
            route_id String,
            origin String,
            destination String,
            flight_id String,
            score Nullable(Float64),
            journey_time String,
            flight_number String ,
            airline_code String,
            airline_name String,
            airline_logo String,
            departure_terminal Nullable(String),
            departure_datetime DateTime,
            flight_status Nullable(String),
            aircraft_title Nullable(String),
            aircraft_short_title Nullable(String),
            aircraft_score Nullable(Float64),
            source_url String,
            available_seats INT,
            booking_class String,
            price INT,
            discount Nullable(Float64),
            discount_percent Nullable(Float64)
            
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, route_id, airline_code, departure_datetime)
        PARTITION BY toYYYYMM(timestamp)
        ''')
        
        logger.info("Tables created/verified")
    
    def fetch_from_api(self, route: Dict, departure_date: str) -> List[Dict]:
        """
        get flights data for a date
        """
        url = self.config['sources']['mrbilit_flights']['url']
        headers = self.config['sources']['mrbilit_flights']['headers']
        payload = self.config['sources']['mrbilit_flights']['default_payload']
        
        payload['Routes'] = [{
            'OriginCode': route['origin'],
            'DestinationCode': route['destination'],
            'DepartureDate': departure_date
        }]

        response = requests.post(url, headers=headers, json=payload).json()['Flights']

        flights=[]
        for res in response:
            segment = res['Segments'][0] # I think all routes for now are single route and there is no stop for now :)
            leg = segment['Legs'][0]
            
            flight_data_main = {
                        'timestamp': datetime.now(UTC),
                        'route_id': f"{route['origin']}-{route['destination']}",
                        'origin': route['origin'],
                        'destination': route['destination'],
                        'flight_id': res['Id'],
                        'score': res['Score'],
                        'journey_time': segment['TotalTime'], 
                        'flight_number':leg['FlightNumber'],
                        'airline_code': leg['AirlineCode'],
                        'airline_name': leg['Airline']['EnglishTitle'],
                        'airline_logo': leg['Airline']['Logo'],
                        'departure_terminal': leg['DepartureTerminal'], 
                        'departure_datetime': datetime.fromisoformat(leg['DepartureTime']),
                        'flight_status': leg['FlightStatus'],
                        'aircraft_title':leg['AirCraft']['EnglishTitle'],
                        'aircraft_short_title':leg['AirCraft']['ShortTitle'],
                        'aircraft_score':leg['AirCraft']['Score'],
                        'source_url': url,

 
            }
            for flight_class in res['Prices']:
                fligth_data = {**flight_data_main, 
                'available_seats': flight_class['Capacity'],
                'booking_class': flight_class['BookingClass'],
                'price': flight_class['PassengerFares']['PaxType'=='ADL'].get('TotalFare'),
                'discount':flight_class['PassengerFares']['PaxType'=='ADL'].get('Discount'),
                'discount_percent':flight_class['PassengerFares']['PaxType'=='ADL'].get('DiscountPercent'),

                }
                flights.append(fligth_data)
        
        return flights
    
    def collect_data(self):
        """Main collection function"""
        scrape_id = str(uuid.uuid4())
        
        for route in self.config['routes']:
            for days_ahead in self.config['departure_windows']:
                departure_date = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
                # departure_date_jalali = jdatetime.datetime.fromgregorian(datetime=departure_date).strftime('%Y-%m-%d')
                try:
                    # Fetch data from API
                    flights = self.fetch_from_api(route, departure_date)
                    
                    # Insert into ClickHouse
                    for flight in flights:
                        self.ch_client.execute(
                            '''
                            INSERT INTO raw_prices (
                                timestamp,
                                scrape_id,
                                route_id,
                                origin,
                                destination,
                                flight_id,
                                score,
                                journey_time,
                                flight_number,
                                airline_code,
                                airline_name,
                                airline_logo,
                                departure_terminal,
                                departure_datetime,
                                flight_status,
                                aircraft_title,
                                aircraft_short_title,
                                aircraft_score,
                                source_url,
                                available_seats,
                                booking_class,
                                price,
                                discount,
                                discount_percent
                            ) VALUES
                            ''',
                            [(
                                flight['timestamp'],
                                scrape_id,
                                flight['route_id'],
                                flight['origin'],
                                flight['destination'],
                                flight['flight_id'],
                                flight['score'],
                                flight['journey_time'],
                                flight['flight_number'],
                                flight['airline_code'],
                                flight['airline_name'],
                                flight['airline_logo'],
                                flight['departure_terminal'],
                                flight['departure_datetime'],
                                flight['flight_status'],
                                flight['aircraft_title'],
                                flight['aircraft_short_title'],
                                flight['aircraft_score'],
                                flight['source_url'],
                                flight['available_seats'],
                                flight['booking_class'],
                                flight['price'],
                                flight['discount'],
                                flight['discount_percent']
                            )]
                        )
                    
                    logger.info(f"Collected data for {route['origin']}-{route['destination']} on {departure_date}")
                    
                    # Respect rate limits
                    time.sleep(self.config.get('rate_limit_delay', 1))
                    
                except Exception as e:
                    logger.error(f"Error collecting data for route {route}: {e}")
                    exit()
        
        logger.info(f"Data collection complete. Scrape ID: {scrape_id}")

if __name__ == "__main__":
    collector = AirlinePricingCollector()
    collector.collect_data()