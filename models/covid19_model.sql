{{ config(materialized = 'table')}}

-- Denormalizing table to daily
with demographics as (

    select
    *
    from {{ ref('base_demographics')}}

),
economy as (

    select
        *
    from {{ ref('base_economy')}}

    ),
index as (

    select
        *
    from {{ ref('base_index')}}

    ),

demographics_join as (

    select
        economy.gdp_usd,
        economy.gdp_per_capita_usd,
        economy.human_capital_index,
        demographics.population,
        demographics.population_male,
        demographics.population_female,
        demographics.population_rural,
        demographics.population_urban,
        demographics.population_largest_city,
        demographics.population_clustered,
        demographics.population_density,
        demographics.human_development_index,
        demographics.population_age_00_09,
        demographics.population_age_10_19,
        demographics.population_age_20_29,
        demographics.population_age_30_39,
        demographics.population_age_40_49,
        demographics.population_age_50_59,
        demographics.population_age_60_69,
        demographics.population_age_70_79,
        demographics.population_age_80_and_older,
        index.place_id, 
        index.wikidata_id, 
        index.country_code,
        index.country_name,
        index.locality_code, 
        index.locality_name, 
        index.datacommons_id, 
        index.subregion1_code, 
        index.subregion1_name, 
        index.subregion2_code, 
        index.subregion2_name, 
        index.aggregation_level, 
        index.iso_3166_1_alpha_2,
        index.iso_3166_1_alpha_3
    from demographics
    left join economy on demographics.location_key = economy.location_key
    left join index on demographics.location_key = index.location_key

    )

select * from demographics_join