with economia as (

    select
        _airbyte_raw_id,
        gdp_usd,
        location_key,
        gdp_per_capita_usd,
        human_capital_index
    from {{ source('airbyte_economia') }}

)

select * from economia