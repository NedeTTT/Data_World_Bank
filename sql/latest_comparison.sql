WITH latest AS (
    SELECT indicator_code, MAX(year) AS latest_year
    FROM mod.fact_indicator_observation
    WHERE value IS NOT NULL
    GROUP BY indicator_code
)
SELECT
    o.indicator_code,
    i.indicator_name,
    CAST(latest.latest_year AS VARCHAR) AS year,
    y.year_start,
    o.country_code,
    c.country_name,
    o.value,
    o.observation_status,
    o.source_path,
    o.source_page,
    o.source_record_index,
    o.source_response_sha256,
    o.source_url,
    country_metadata.source_path AS country_metadata_source_path,
    country_metadata.source_page AS country_metadata_source_page,
    country_metadata.source_record_index AS country_metadata_source_record_index,
    country_metadata.source_response_sha256 AS country_metadata_source_response_sha256,
    country_metadata.source_url AS country_metadata_source_url
FROM latest
JOIN mod.fact_indicator_observation AS o ON o.indicator_code = latest.indicator_code AND o.year = latest.latest_year
JOIN mod.dim_country AS c USING (country_code)
LEFT JOIN stg.vw_country_source AS country_metadata
    ON country_metadata.run_id = c.run_id AND country_metadata.country_code = c.country_code
JOIN mod.dim_indicator AS i ON i.indicator_code = o.indicator_code
JOIN mod.dim_year AS y ON y.year = o.year
ORDER BY o.indicator_code, o.value DESC NULLS LAST, o.country_code;