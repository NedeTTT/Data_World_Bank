SELECT
    o.country_code,
    c.country_name,
    o.indicator_code,
    i.indicator_name,
    CAST(y.year AS VARCHAR) AS year,
    y.year_start,
    o.observation_status,
    o.value,
    CASE
        WHEN o.observation_status = 'complete' AND o.value IS NULL
            THEN 'complete_status_with_null_value'
        ELSE NULL
    END AS data_quality_issue,
    o.source_path,
    o.source_page,
    o.source_record_index,
    o.source_response_sha256,
    o.source_url,
    c.source_path AS country_metadata_source_path,
    c.source_page AS country_metadata_source_page,
    c.source_record_index AS country_metadata_source_record_index,
    c.source_response_sha256 AS country_metadata_source_response_sha256,
    c.source_url AS country_metadata_source_url
FROM mod.fact_indicator_observation AS o
JOIN mod.dim_country AS c USING (country_code)
JOIN mod.dim_indicator AS i USING (indicator_code)
JOIN mod.dim_year AS y USING (year)
WHERE o.observation_status <> 'complete'
    OR o.value IS NULL
ORDER BY o.country_code, o.indicator_code, o.year;