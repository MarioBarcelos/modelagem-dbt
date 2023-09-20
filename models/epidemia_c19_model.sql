{{ config(materialized = 'table')}}

with epidemiologia as (

    select * 
    from {{ ref('epidemiologia')}}
),
dados_demograficos as (

    select *
    from {{ ref('dados_demograficos')}}
),
economia as (

    select * 
    from {{ ref('economia')}}
),
index as (

    select * 
    from {{ ref('index')}}
),

epidemiologia_join as (

    select
        epidemiologia.date,
        epidemiologia.location_key,
        iff(epidemiologia.new_confirmed = 'NaN', 0, epidemiologia.new_confirmed) as new_confirmed,
        iff(epidemiologia.new_deceased = 'NaN', 0, epidemiologia.new_deceased) as new_deceased,
        iff(epidemiologia.new_recovered = 'NaN', 0, epidemiologia.new_recovered) as new_recovered,
        iff(epidemiologia.new_tested = 'NaN', 0, epidemiologia.new_tested) as new_tested,
        iff(epidemiologia.cumulative_confirmed = 'NaN', 0, epidemiologia.cumulative_confirmed) as cumulative_confirmed,
        iff(epidemiologia.cumulative_deceased = 'NaN', 0, epidemiologia.cumulative_deceased) as cumulative_deceased,
        iff(epidemiologia.cumulative_recovered = 'NaN', 0, epidemiologia.cumulative_recovered) as cumulative_recovered,
        iff(epidemiologia.cumulative_tested = 'NaN', 0, epidemiologia.cumulative_tested) as cumulative_tested,
        economia.gdp_usd,
        economia.gdp_per_capita_usd,
        economia.human_capital_index,
        dados_demograficos.population,
        dados_demograficos.population_male,
        dados_demograficos.population_female,
        dados_demograficos.population_rural,
        dados_demograficos.population_urban,
        dados_demograficos.population_largest_city,
        dados_demograficos.population_clustered,
        dados_demograficos.population_density,
        dados_demograficos.human_development_index,
        dados_demograficos.population_age_00_09,
        dados_demograficos.population_age_10_19,
        dados_demograficos.population_age_20_29,
        dados_demograficos.population_age_30_39,
        dados_demograficos.population_age_40_49,
        dados_demograficos.population_age_50_59,
        dados_demograficos.population_age_60_69,
        dados_demograficos.population_age_70_79,
        dados_demograficos.population_age_80_and_older,
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
    from epidemiologia
    left join dados_demograficos on epidemiologia.location_key = dados_demograficos.location_key
    left join economia on epidemiologia.location_key = economia.location_key
    left join index on epidemiologia.location_key = index.location_key
)

select * from epidemiologia_join