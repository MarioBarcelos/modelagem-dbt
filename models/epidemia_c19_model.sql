{{ config(materialized = 'table')}}

-- Por fora (após o with) segue as tables do dw e por dentro (após o as) segue as views
-- Denormalizing table to daily
with airbyte_epidemiologia as (

    select
    *
    from {{ ref('epidemiologia')}}

),
airbyte_dados_demograficos as (

    select
    *
    from {{ ref('dados_demograficos')}}

),
airbyte_economia as (

    select
        *
    from {{ ref('economia')}}

    ),
airbyte_index as (

    select
        *
    from {{ ref('index')}}

    ),

epidemiologia_join as (

    select        
        airbyte_epidemiologia.date,
        airbyte_epidemiologia.location_key,
        iff(airbyte_epidemiologia.new_confirmed = 'NaN', 0, airbyte_epidemiologia.new_confirmed) as new_confirmed,
        iff(airbyte_epidemiologia.new_deceased = 'NaN', 0, airbyte_epidemiologia.new_deceased) as new_deceased,
        iff(airbyte_epidemiologia.new_recovered = 'NaN', 0, airbyte_epidemiologia.new_recovered) as new_recovered,
        iff(airbyte_epidemiologia.new_tested = 'NaN', 0, airbyte_epidemiologia.new_tested) as new_tested,
        iff(airbyte_epidemiologia.cumulative_confirmed = 'NaN', 0, airbyte_epidemiologia.cumulative_confirmed) as cumulative_confirmed,
        iff(airbyte_epidemiologia.cumulative_deceased = 'NaN', 0, airbyte_epidemiologia.cumulative_deceased) as cumulative_deceased,
        iff(airbyte_epidemiologia.cumulative_recovered = 'NaN', 0, airbyte_epidemiologia.cumulative_recovered) as cumulative_recovered,
        iff(airbyte_epidemiologia.cumulative_tested = 'NaN', 0, airbyte_epidemiologia.cumulative_tested) as cumulative_tested,
        airbyte_economia.gdp_usd,
        airbyte_economia.gdp_per_capita_usd,
        airbyte_economia.human_capital_index,
        airbyte_dados_demograficos.population,
        airbyte_dados_demograficos.population_male,
        airbyte_dados_demograficos.population_female,
        airbyte_dados_demograficos.population_rural,
        airbyte_dados_demograficos.population_urban,
        airbyte_dados_demograficos.population_largest_city,
        airbyte_dados_demograficos.population_clustered,
        airbyte_dados_demograficos.population_density,
        airbyte_dados_demograficos.human_development_index,
        airbyte_dados_demograficos.population_age_00_09,
        airbyte_dados_demograficos.population_age_10_19,
        airbyte_dados_demograficos.population_age_20_29,
        airbyte_dados_demograficos.population_age_30_39,
        airbyte_dados_demograficos.population_age_40_49,
        airbyte_dados_demograficos.population_age_50_59,
        airbyte_dados_demograficos.population_age_60_69,
        airbyte_dados_demograficos.population_age_70_79,
        airbyte_dados_demograficos.population_age_80_and_older,
        airbyte_index.place_id, 
        airbyte_index.wikidata_id, 
        airbyte_index.country_code,
        airbyte_index.country_name,
        airbyte_index.locality_code, 
        airbyte_index.locality_name, 
        airbyte_index.datacommons_id, 
        airbyte_index.subregion1_code, 
        airbyte_index.subregion1_name, 
        airbyte_index.subregion2_code, 
        airbyte_index.subregion2_name, 
        airbyte_index.aggregation_level, 
        airbyte_index.iso_3166_1_alpha_2,
        airbyte_index.iso_3166_1_alpha_3
    from airbyte_epidemiologia
    left join airbyte_dados_demograficos on airbyte_epidemiologia.location_key = airbyte_dados_demograficos.location_key
    left join airbyte_economia on airbyte_epidemiologia.location_key = airbyte_economia.location_key
    left join airbyte_index on airbyte_epidemiologia.location_key = airbyte_index.location_key

    )

select * from epidemiologia_join