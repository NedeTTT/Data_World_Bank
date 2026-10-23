WITH annual AS (
    SELECT
        o.country_code,
        c.country_name,
        o.indicator_code,
        i.indicator_name,
        CAST(y.year AS VARCHAR) AS year,
        y.year_start,
        o.value,
        o.observation_status,
        o.source_path,
        o.source_page,
        o.source_record_index,
        o.source_response_sha256,
        o.source_url,
        c.source_path AS country_metadata_source_path,
        c.source_page AS country_metadata_source_page,
        c.source_record_index AS country_metadata_source_record_index,
        c.source_response_sha256 AS country_metadata_source_response_sha256,
        c.source_url AS country_metadata_source_url,
        LAG(o.value) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS prior_year_value,
        CAST(LAG(y.year) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS VARCHAR) AS prior_year,
        LAG(y.year_start) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS prior_year_start,
        LAG(o.source_path) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS prior_source_path,
        LAG(o.source_page) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS prior_source_page,
        LAG(o.source_record_index) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS prior_source_record_index,
        LAG(o.source_response_sha256) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS prior_source_response_sha256,
        LAG(o.source_url) OVER (
            PARTITION BY o.country_code, o.indicator_code ORDER BY o.year
        ) AS prior_source_url
    FROM mod.fact_indicator_observation AS o
    JOIN mod.dim_country AS c USING (country_code)
    JOIN mod.dim_indicator AS i USING (indicator_code)
    JOIN mod.dim_year AS y USING (year)
)
SELECT
    country_code,
    country_name,
    indicator_code,
    indicator_name,
    year,
    year_start,
    prior_year,
    prior_year_start,
    value,
    prior_year_value,
    absolute_change,
    percent_change,
    observation_status,
    source_path,
    source_page,
    source_record_index,
    source_response_sha256,
    source_url,
    prior_source_path,
    prior_source_page,
    prior_source_record_index,
    prior_source_response_sha256,
    prior_source_url,
    country_metadata_source_path,
    country_metadata_source_page,
    country_metadata_source_record_index,
    country_metadata_source_response_sha256,
    country_metadata_source_url
FROM (
    SELECT
        annual.*,
        CASE
            WHEN value IS NOT NULL AND prior_year_value IS NOT NULL
            THEN value - prior_year_value
        END AS absolute_change,
        CASE
            WHEN value IS NOT NULL AND prior_year_value IS NOT NULL AND prior_year_value <> 0
            THEN 100.0 * (value - prior_year_value) / prior_year_value
        END AS percent_change
    FROM annual
) AS calculated
ORDER BY country_code, indicator_code, year;