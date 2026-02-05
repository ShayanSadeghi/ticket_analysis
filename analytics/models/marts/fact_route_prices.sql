{{
    config(
        materialized='table',
        unique_key='flight_id',
        partition_by='departure_date'
    )
}}


WITH best_price as (
    SELECT 
        route_id, 
        airline_name,
        available_seats, 
        price, 
        departure_date, 
        departure_hour, 
        flight_id,
        DENSE_RANK() OVER (
            PARTITION BY route_id, departure_date, departure_hour
            ORDER BY price ASC
        ) as price_rank
    FROM {{ref('stg_flights')}}
)

SELECT * 
FROM best_price
WHERE price_rank=1