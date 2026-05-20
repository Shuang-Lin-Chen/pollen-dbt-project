{{ config(materialized='table') }}

with hourly_forecast as (

    select
        region,
        latitude,
        longitude,
        forecast_time::date as forecast_date,
        alder_pollen,
        birch_pollen,
        grass_pollen,
        mugwort_pollen,
        olive_pollen,
        ragweed_pollen

    from {{ ref('stg_pollen_forecast') }}

),

daily_summary as (

    select
        region,
        latitude,
        longitude,
        forecast_date,

        avg(alder_pollen) as avg_alder_pollen,
        max(alder_pollen) as max_alder_pollen,

        avg(birch_pollen) as avg_birch_pollen,
        max(birch_pollen) as max_birch_pollen,

        avg(grass_pollen) as avg_grass_pollen,
        max(grass_pollen) as max_grass_pollen,

        avg(mugwort_pollen) as avg_mugwort_pollen,
        max(mugwort_pollen) as max_mugwort_pollen,

        avg(olive_pollen) as avg_olive_pollen,
        max(olive_pollen) as max_olive_pollen,

        avg(ragweed_pollen) as avg_ragweed_pollen,
        max(ragweed_pollen) as max_ragweed_pollen,

        count(*) as hourly_record_count

    from hourly_forecast
    group by
        region,
        latitude,
        longitude,
        forecast_date

)

select *
from daily_summary