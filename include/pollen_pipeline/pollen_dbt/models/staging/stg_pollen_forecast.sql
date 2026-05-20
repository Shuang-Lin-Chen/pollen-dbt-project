with source as (

    select
        md5(to_varchar(raw_json) || to_varchar(loaded_at)) as raw_record_id,
        loaded_at,
        raw_json
    from {{ source('raw', 'pollen_forecast_json') }}
    where raw_json:metadata:source_system::varchar = 'Open-Meteo Air Quality API'

),

locations as (

    select
        source.raw_record_id,
        source.loaded_at,

        location.value:region::varchar as region,
        location.value:latitude::float as latitude,
        location.value:longitude::float as longitude,

        location.value:api_response:timezone::varchar as timezone,
        location.value:api_response:hourly as hourly_json

    from source,
    lateral flatten(
        input => source.raw_json:payload:locations
    ) as location

),

hourly_forecasts as (

    select
        locations.raw_record_id,
        locations.loaded_at,
        locations.region,
        locations.latitude,
        locations.longitude,
        locations.timezone,

        time_item.index as hourly_index,
        time_item.value::varchar as forecast_time_raw,
        try_to_timestamp_ntz(time_item.value::varchar) as forecast_time,

        get(locations.hourly_json:alder_pollen, time_item.index)::float as alder_pollen,
        get(locations.hourly_json:birch_pollen, time_item.index)::float as birch_pollen,
        get(locations.hourly_json:grass_pollen, time_item.index)::float as grass_pollen,
        get(locations.hourly_json:mugwort_pollen, time_item.index)::float as mugwort_pollen,
        get(locations.hourly_json:olive_pollen, time_item.index)::float as olive_pollen,
        get(locations.hourly_json:ragweed_pollen, time_item.index)::float as ragweed_pollen

    from locations,
    lateral flatten(
        input => locations.hourly_json:time
    ) as time_item

)

select
    raw_record_id,
    loaded_at,
    region,
    latitude,
    longitude,
    timezone,
    hourly_index,
    forecast_time_raw,
    forecast_time,
    alder_pollen,
    birch_pollen,
    grass_pollen,
    mugwort_pollen,
    olive_pollen,
    ragweed_pollen
from hourly_forecasts