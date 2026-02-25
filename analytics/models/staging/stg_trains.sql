{{
    config(
        materialized='incremental',
        unique_key='train_id',
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
    train_id,
    providerName,
    corporationName,
    departure_datetime,
    available_seats,
    price,
    compartment_capacity,
    toDate(timestamp) as scrape_date,
    toHour(timestamp) as scrape_hour,
    toDate(departure_datetime) as departure_date,
    toHour(departure_datetime) as departure_hour
FROM train_tickets_raw

{% if is_incremental() %}
WHERE timestamp > (SELECT max(timestamp) FROM {{ this }})
{% endif %}