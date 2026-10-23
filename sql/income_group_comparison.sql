SELECT
    o.indicator_code,
    i.indicator_name,
    CAST(y.year AS VARCHAR) AS year,
    y.year_start,
    c.income_group_id,
    c.income_group_name,
    COUNT(*) FILTER (WHERE o.value IS NOT NULL) AS country_count,
    AVG(o.value) AS average_value,
    MEDIAN(o.value) AS median_value,
    MIN(o.value) AS minimum_value,
    MAX(o.value) AS maximum_value,
    STRING_AGG(
        DISTINCT concat(o.country_code, ':', o.source_path, '#', coalesce(CAST(o.source_record_index AS VARCHAR), 'missing'), '@', o.source_response_sha256),
        ';' ORDER BY concat(o.country_code, ':', o.source_path, '#', coalesce(CAST(o.source_record_index AS VARCHAR), 'missing'), '@', o.source_response_sha256)
    ) FILTER (WHERE o.value IS NOT NULL) AS contributing_observation_lineage,
    STRING_AGG(
        DISTINCT concat(c.country_code, ':', c.source_path, '#', c.source_page, ':', c.source_record_index, '@', c.source_response_sha256, '(', c.source_url, ')'),
        ';' ORDER BY concat(c.country_code, ':', c.source_path, '#', c.source_page, ':', c.source_record_index, '@', c.source_response_sha256, '(', c.source_url, ')')
    ) AS income_group_metadata_lineage
FROM mod.fact_indicator_observation AS o
JOIN mod.dim_country AS c USING (country_code)
JOIN mod.dim_indicator AS i USING (indicator_code)
JOIN mod.dim_year AS y USING (year)
GROUP BY o.indicator_code, i.indicator_name, o.year, y.year, y.year_start, c.income_group_id, c.income_group_name
ORDER BY o.indicator_code, o.year, c.income_group_id;