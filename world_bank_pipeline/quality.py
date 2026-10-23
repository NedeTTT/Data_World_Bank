from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from .config import PipelineConfig


LOGGER = logging.getLogger(__name__)


def run_quality_checks(connection: duckdb.DuckDBPyConnection, config: PipelineConfig) -> None:
    expected = len(config.countries) * len(config.indicators) * (config.end_year - config.start_year + 1)
    actual = connection.execute("SELECT COUNT(*) FROM mod.fact_indicator_observation").fetchone()[0]
    if actual != expected:
        raise ValueError(f"Expected {expected} observation keys, found {actual}")

    duplicate_count = connection.execute("""
        SELECT COUNT(*) FROM (
            SELECT country_code, indicator_code, year
            FROM mod.fact_indicator_observation GROUP BY ALL HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if duplicate_count:
        raise ValueError(f"Found {duplicate_count} duplicate analytical keys")

    invalid_status_count = connection.execute("""
        SELECT COUNT(*) FROM mod.fact_indicator_observation
        WHERE (observation_status = 'complete' AND value IS NULL)
           OR (observation_status <> 'complete' AND value IS NOT NULL)
    """).fetchone()[0]
    if invalid_status_count:
        raise ValueError(f"Found {invalid_status_count} status/value inconsistencies")

    bad_references = connection.execute("""
        SELECT COUNT(*) FROM mod.fact_indicator_observation AS o
        LEFT JOIN mod.dim_country AS c USING (country_code)
        LEFT JOIN mod.dim_indicator AS i USING (indicator_code)
        LEFT JOIN mod.dim_year AS y USING (year)
        WHERE c.country_code IS NULL OR i.indicator_code IS NULL OR y.year IS NULL
    """).fetchone()[0]
    if bad_references:
        raise ValueError(f"Found {bad_references} observations without dimension records")

    for (raw_path,) in connection.execute("SELECT DISTINCT source_path FROM mod.fact_indicator_observation").fetchall():
        source_path = Path(raw_path)
        if not source_path.is_absolute():
            source_path = config.root / source_path
        if not source_path.is_file():
            raise ValueError(f"Raw source file is missing: {source_path}")

    anomaly_count = connection.execute("""
        WITH paired AS (
            SELECT gdp.value AS gdp, per_capita.value * population.value AS implied_gdp
            FROM mod.fact_indicator_observation AS population
            JOIN mod.fact_indicator_observation AS gdp USING (country_code, year)
            JOIN mod.fact_indicator_observation AS per_capita USING (country_code, year)
            WHERE population.indicator_code = 'SP.POP.TOTL'
              AND gdp.indicator_code = 'NY.GDP.MKTP.CD'
              AND per_capita.indicator_code = 'NY.GDP.PCAP.CD'
              AND population.value IS NOT NULL AND gdp.value IS NOT NULL AND per_capita.value IS NOT NULL
        )
        SELECT COUNT(*) FROM paired
        WHERE ABS(gdp - implied_gdp) / NULLIF(ABS(gdp), 0) > 0.10
    """).fetchone()[0]
    if anomaly_count:
        LOGGER.warning("GDP consistency review: %s rows differ by more than 10%%", anomaly_count)