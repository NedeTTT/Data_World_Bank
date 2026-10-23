WITH paired AS (
    SELECT
        population.country_code,
        countries.country_name,
        CAST(calendar_year.year AS VARCHAR) AS year,
        calendar_year.year_start,
        gdp.value AS gdp_usd,
        gdp_per_capita.value AS gdp_per_capita_usd,
        population.value AS population,
        gdp_per_capita.value * population.value AS implied_gdp_usd,
        gdp.source_path AS gdp_source_path,
        gdp.source_page AS gdp_source_page,
        gdp.source_record_index AS gdp_source_record_index,
        gdp.source_response_sha256 AS gdp_source_response_sha256,
        gdp.source_url AS gdp_source_url,
        gdp_per_capita.source_path AS gdp_per_capita_source_path,
        gdp_per_capita.source_page AS gdp_per_capita_source_page,
        gdp_per_capita.source_record_index AS gdp_per_capita_source_record_index,
        gdp_per_capita.source_response_sha256 AS gdp_per_capita_source_response_sha256,
        gdp_per_capita.source_url AS gdp_per_capita_source_url,
        population.source_path AS population_source_path,
        population.source_page AS population_source_page,
        population.source_record_index AS population_source_record_index,
        population.source_response_sha256 AS population_source_response_sha256,
        population.source_url AS population_source_url,
        country_metadata.source_path AS country_metadata_source_path,
        country_metadata.source_page AS country_metadata_source_page,
        country_metadata.source_record_index AS country_metadata_source_record_index,
        country_metadata.source_response_sha256 AS country_metadata_source_response_sha256,
        country_metadata.source_url AS country_metadata_source_url
    FROM mod.fact_indicator_observation AS population
    JOIN mod.fact_indicator_observation AS gdp
      ON gdp.country_code = population.country_code AND gdp.year = population.year
     AND gdp.indicator_code = 'NY.GDP.MKTP.CD'
    JOIN mod.fact_indicator_observation AS gdp_per_capita
      ON gdp_per_capita.country_code = population.country_code AND gdp_per_capita.year = population.year
     AND gdp_per_capita.indicator_code = 'NY.GDP.PCAP.CD'
    JOIN mod.dim_country AS countries ON countries.country_code = population.country_code
    LEFT JOIN stg.vw_country_source AS country_metadata
      ON country_metadata.run_id = countries.run_id
     AND country_metadata.country_code = countries.country_code
    JOIN mod.dim_year AS calendar_year ON calendar_year.year = population.year
    WHERE population.indicator_code = 'SP.POP.TOTL'
      AND population.value IS NOT NULL AND gdp.value IS NOT NULL AND gdp_per_capita.value IS NOT NULL
)
SELECT
    *,
    ABS(gdp_usd - implied_gdp_usd) / NULLIF(ABS(gdp_usd), 0) AS relative_difference,
    ABS(gdp_usd - implied_gdp_usd) / NULLIF(ABS(gdp_usd), 0) > 0.10 AS review_flag
FROM paired
ORDER BY country_code, year;