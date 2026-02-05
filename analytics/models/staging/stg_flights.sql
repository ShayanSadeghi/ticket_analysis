{{
    config(
        materialized='incremental',
        unique_key='flight_id',
        partition_by='toDate(timestamp)',
        order_by='(route_id, departure_datetime, price)'
    )
}}

SELECT
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
    discount_percent,

    toDate(timestamp) as scrape_date,
    toHour(timestamp) as scrape_hour,
    toDate(departure_datetime) as departure_date,
    toHour(departure_datetime) as departure_hour
FROM {{ source('raw', 'flight_tickets_raw') }}

{% if is_incremental() %}
WHERE timestamp > (SELECT max(timestamp) FROM {{ this }})
{% endif %}