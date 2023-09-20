with economia as (

    select
        _airbyte_raw_id,
        gdp_usd,
        location_key,
        gdp_per_capita_usd,
        human_capital_index
    from {{ source('raw_epidemia_c19', 'airbyte_economia') }}

)

select * from economia