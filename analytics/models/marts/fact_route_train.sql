{{
    config(
        materialized='table',
        unique_key='train_id',
        partition_by='departure_date'
    )
}}


WITH best_price as (
    SELECT 
        route_id, 
        corporationName,
        available_seats, 
        price, 
        departure_date, 
        departure_hour, 
        train_id,
        compartment_capacity,
        DENSE_RANK() OVER (
            PARTITION BY route_id, departure_date, departure_hour, compartment_capacity
            ORDER BY price ASC
        ) as price_rank
    FROM {{ref('stg_trains')}}
)

SELECT * 
FROM best_price
WHERE price_rank=1