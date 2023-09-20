with epidemiologia as (

    select
        _airbyte_raw_id,
        date,
        location_key,
        new_confirmed,
        new_deceased,
        new_recovered,
        new_tested,
        cumulative_confirmed,
        cumulative_deceased,
        cumulative_recovered,
        cumulative_tested 
    from {{ source('raw_epidemia_c19', 'airbyte_epidemiologia') }}
)

select * from epidemiologia