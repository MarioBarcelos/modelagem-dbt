with dados_demograficos as (

    select
        _airbyte_raw_id,
        location_key,
        population,
        population_male,
        population_female,
        population_rural,
        population_urban,
        population_largest_city,
        population_clustered,
        population_density,
        human_development_index,
        population_age_00_09,
        population_age_10_19,
        population_age_20_29,
        population_age_30_39,
        population_age_40_49,
        population_age_50_59,
        population_age_60_69,
        population_age_70_79,
        population_age_80_and_older
    from {{ source('raw_epidemia_c19', 'airbyte_dados_demograficos') }}
)

select * from dados_demograficos