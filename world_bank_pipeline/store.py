from __future__ import annotations

from collections.abc import Callable
import json

import duckdb


SCHEMA = """
CREATE SCHEMA IF NOT EXISTS ing;
CREATE SCHEMA IF NOT EXISTS sys;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mod;
CREATE SEQUENCE IF NOT EXISTS sys.pipeline_run_id_seq START 1;
CREATE TABLE IF NOT EXISTS sys.pipeline_runs (
    run_id BIGINT PRIMARY KEY,
    load_start_date TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status VARCHAR NOT NULL,
    error_message VARCHAR
);
CREATE TABLE IF NOT EXISTS sys.ingestion_quality_metrics (
    run_id BIGINT NOT NULL,
    table_name VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    sql_null_count BIGINT NOT NULL,
    sql_null_counts JSON NOT NULL,
    json_value_null_count BIGINT NOT NULL,
    measured_at TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, table_name)
);
CREATE TABLE IF NOT EXISTS ing.raw_responses (
    run_id BIGINT NOT NULL,
    endpoint_type VARCHAR NOT NULL,
    country_code VARCHAR,
    indicator_code VARCHAR,
    page_number INTEGER NOT NULL,
    request_url VARCHAR NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    response_sha256 VARCHAR NOT NULL,
    raw_path VARCHAR NOT NULL,
    PRIMARY KEY (run_id, raw_path)
);
CREATE TABLE IF NOT EXISTS ing.raw_country_metadata (
    run_id BIGINT NOT NULL,
    country_code VARCHAR NOT NULL,
    source_page INTEGER NOT NULL,
    source_record_index INTEGER NOT NULL,
    payload_json JSON NOT NULL,
    source_path VARCHAR NOT NULL,
    source_response_sha256 VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, country_code, source_page, source_record_index)
);
CREATE TABLE IF NOT EXISTS ing.raw_indicator_observations (
    run_id BIGINT NOT NULL,
    country_code VARCHAR NOT NULL,
    indicator_code VARCHAR NOT NULL,
    source_page INTEGER NOT NULL,
    source_record_index INTEGER NOT NULL,
    payload_json JSON NOT NULL,
    source_path VARCHAR NOT NULL,
    source_response_sha256 VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (run_id, country_code, indicator_code, source_page, source_record_index)
);
CREATE TABLE IF NOT EXISTS ing.run_observation_scope (
    run_id BIGINT NOT NULL,
    country_code VARCHAR NOT NULL,
    indicator_code VARCHAR NOT NULL,
    year VARCHAR NOT NULL,
    PRIMARY KEY (run_id, country_code, indicator_code, year)
);
CREATE TABLE IF NOT EXISTS mod.dim_country (
    country_code VARCHAR PRIMARY KEY,
    country_name VARCHAR NOT NULL,
    region_id VARCHAR,
    region_name VARCHAR,
    income_group_id VARCHAR,
    income_group_name VARCHAR,
    lending_type_id VARCHAR,
    lending_type_name VARCHAR,
    capital_city VARCHAR,
    longitude DOUBLE,
    latitude DOUBLE,
    run_id BIGINT NOT NULL,
    load_start_date TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS mod.dim_indicator (
    indicator_code VARCHAR PRIMARY KEY,
    indicator_name VARCHAR NOT NULL,
    run_id BIGINT NOT NULL,
    load_start_date TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS mod.dim_year (
    year VARCHAR PRIMARY KEY,
    decade VARCHAR NOT NULL,
    year_start DATE NOT NULL,
    run_id BIGINT NOT NULL,
    load_start_date TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS mod.fact_indicator_observation (
    country_code VARCHAR NOT NULL,
    indicator_code VARCHAR NOT NULL,
    year VARCHAR NOT NULL,
    value DOUBLE,
    observation_status VARCHAR NOT NULL CHECK (observation_status IN ('complete', 'incomplete', 'missing')),
    source_run_id BIGINT NOT NULL,
    source_path VARCHAR NOT NULL,
    source_page INTEGER NOT NULL,
    source_record_index INTEGER,
    source_response_sha256 VARCHAR NOT NULL,
    source_url VARCHAR NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (country_code, indicator_code, year)
);
"""

STAGING_VIEWS = """
CREATE OR REPLACE VIEW stg.vw_current_run AS
SELECT run_id
FROM sys.pipeline_runs
WHERE status IN ('running', 'succeeded')
QUALIFY ROW_NUMBER() OVER (ORDER BY load_start_date DESC, run_id DESC) = 1;

CREATE OR REPLACE VIEW stg.vw_country AS
SELECT
    run_id,
    country_code,
    json_extract_string(payload_json, '$.name') AS country_name,
    json_extract_string(payload_json, '$.region.id') AS region_id,
    json_extract_string(payload_json, '$.region.value') AS region_name,
    json_extract_string(payload_json, '$.incomeLevel.id') AS income_group_id,
    json_extract_string(payload_json, '$.incomeLevel.value') AS income_group_name,
    json_extract_string(payload_json, '$.lendingType.id') AS lending_type_id,
    json_extract_string(payload_json, '$.lendingType.value') AS lending_type_name,
    json_extract_string(payload_json, '$.capitalCity') AS capital_city,
    TRY_CAST(NULLIF(json_extract_string(payload_json, '$.longitude'), '') AS DOUBLE) AS longitude,
    TRY_CAST(NULLIF(json_extract_string(payload_json, '$.latitude'), '') AS DOUBLE) AS latitude,
    run_id AS source_run_id,
    source_path,
    source_page,
    source_record_index,
    source_response_sha256,
    source_url
FROM ing.raw_country_metadata AS raw
JOIN stg.vw_current_run AS current_run USING (run_id)
WHERE json_extract_string(payload_json, '$.id') = country_code
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY country_code
    ORDER BY fetched_at DESC, source_page, source_record_index, source_response_sha256
) = 1;

CREATE OR REPLACE VIEW stg.vw_country_source AS
SELECT
    run_id,
    country_code,
    source_path,
    source_page,
    source_record_index,
    source_response_sha256,
    source_url,
    fetched_at
FROM ing.raw_country_metadata AS raw
WHERE json_extract_string(payload_json, '$.id') = country_code
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY run_id, country_code
    ORDER BY source_page, source_record_index, source_response_sha256
) = 1;

CREATE OR REPLACE VIEW stg.vw_indicator_observation AS
SELECT
    run_id,
    country_code,
    indicator_code,
    CAST(TRY_CAST(json_extract_string(payload_json, '$.date') AS INTEGER) AS VARCHAR) AS year,
    make_date(TRY_CAST(json_extract_string(payload_json, '$.date') AS INTEGER), 1, 1) AS year_start,
    TRY_CAST(json_extract_string(payload_json, '$.value') AS DOUBLE) AS value,
    source_page,
    source_record_index,
    source_path,
    source_response_sha256,
    source_url,
    fetched_at
FROM ing.raw_indicator_observations AS raw
JOIN stg.vw_current_run AS current_run USING (run_id)
WHERE TRY_CAST(json_extract_string(payload_json, '$.date') AS INTEGER) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        country_code,
        indicator_code,
        TRY_CAST(json_extract_string(payload_json, '$.date') AS INTEGER)
    ORDER BY fetched_at DESC, source_page, source_record_index, source_response_sha256
) = 1;

CREATE OR REPLACE VIEW stg.vw_observation AS
SELECT
    scope.run_id,
    scope.country_code,
    scope.indicator_code,
    CAST(scope.year AS VARCHAR) AS year,
    make_date(TRY_CAST(scope.year AS INTEGER), 1, 1) AS year_start,
    observation.value,
    CASE
        WHEN observation.source_record_index IS NULL THEN 'missing'
        WHEN observation.value IS NULL THEN 'incomplete'
        ELSE 'complete'
    END AS observation_status,
    scope.run_id AS source_run_id,
    COALESCE(observation.source_path, response.raw_path) AS source_path,
    COALESCE(observation.source_page, response.page_number) AS source_page,
    observation.source_record_index,
    COALESCE(observation.source_response_sha256, response.response_sha256) AS source_response_sha256,
    COALESCE(observation.source_url, response.request_url) AS source_url,
    COALESCE(observation.fetched_at, response.fetched_at) AS fetched_at
FROM ing.run_observation_scope AS scope
JOIN stg.vw_current_run AS current_run USING (run_id)
LEFT JOIN stg.vw_indicator_observation AS observation
    ON observation.country_code = scope.country_code
   AND observation.indicator_code = scope.indicator_code
    AND observation.year = scope.year
LEFT JOIN ing.raw_responses AS response
    ON response.run_id = scope.run_id
   AND response.endpoint_type = 'indicators'
   AND response.country_code = scope.country_code
   AND response.indicator_code = scope.indicator_code
   AND response.page_number = (
       SELECT MIN(first_page.page_number)
       FROM ing.raw_responses AS first_page
       WHERE first_page.run_id = scope.run_id
         AND first_page.endpoint_type = 'indicators'
         AND first_page.country_code = scope.country_code
         AND first_page.indicator_code = scope.indicator_code
   );
"""


def initialize_database(database_path: str) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(database_path)
    connection.execute(SCHEMA)
    _migrate_legacy_run_table(connection)
    _migrate_sys_pipeline_runs(connection)
    _migrate_legacy_objects(connection)
    _remove_materialized_staging_tables(connection)
    _migrate_dimensions(connection)
    _migrate_year_columns(connection)
    _migrate_fact_foreign_keys(connection)
    _restore_source_lineage(connection)
    connection.execute(STAGING_VIEWS)
    return connection


def _migrate_sys_pipeline_runs(connection: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(connection, "sys", "pipeline_runs"):
        return
    columns = {
        row[0] for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='sys' AND table_name='pipeline_runs'"
        ).fetchall()
    }
    if "load_start_date" in columns:
        return
    if "started_at" not in columns:
        raise RuntimeError("sys.pipeline_runs needs started_at or load_start_date to migrate")
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute("""
            CREATE TABLE sys.pipeline_runs__load_start_migration (
                run_id BIGINT PRIMARY KEY,
                load_start_date TIMESTAMP NOT NULL,
                finished_at TIMESTAMP,
                status VARCHAR NOT NULL,
                error_message VARCHAR
            )
        """)
        connection.execute("""
            INSERT INTO sys.pipeline_runs__load_start_migration
            SELECT CAST(run_id AS BIGINT), started_at, finished_at, status, error_message
            FROM sys.pipeline_runs
        """)
        connection.execute("DROP TABLE sys.pipeline_runs")
        connection.execute("ALTER TABLE sys.pipeline_runs__load_start_migration RENAME TO pipeline_runs")
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _migrate_legacy_objects(connection: duckdb.DuckDBPyConnection) -> None:
    destinations = {
        "raw_responses": "ing",
        "raw_country_metadata": "ing",
        "raw_indicator_observations": "ing",
    }
    connection.execute("CREATE SCHEMA IF NOT EXISTS sys")
    connection.execute("CREATE SCHEMA IF NOT EXISTS ing")
    connection.execute("CREATE SCHEMA IF NOT EXISTS stg")
    connection.execute("CREATE SCHEMA IF NOT EXISTS mod")
    connection.execute("CREATE SCHEMA IF NOT EXISTS prl")
    for table_name, schema_name in destinations.items():
        exists_in_main = connection.execute(
            """SELECT COUNT(*) FROM information_schema.tables
               WHERE table_schema = 'main' AND table_name = ?""",
            [table_name],
        ).fetchone()[0]
        if exists_in_main:
            exists_in_target = connection.execute(
                """SELECT COUNT(*) FROM information_schema.tables
                   WHERE table_schema = ? AND table_name = ?""",
                [schema_name, table_name],
            ).fetchone()[0]
            if not exists_in_target:
                raise RuntimeError(f"Target table {schema_name}.{table_name} was not initialized")
            connection.execute(
                f"INSERT INTO {schema_name}.{table_name} SELECT * FROM main.{table_name} ON CONFLICT DO NOTHING"
            )
            connection.execute(f"DROP TABLE main.{table_name}")

    _migrate_legacy_model_tables(connection)


def _migrate_legacy_run_table(connection: duckdb.DuckDBPyConnection) -> None:
    for source_schema in ("main", "ing"):
        if not _table_exists(connection, source_schema, "pipeline_runs"):
            continue
        connection.execute(
            f"INSERT INTO sys.pipeline_runs SELECT * FROM {source_schema}.pipeline_runs ON CONFLICT (run_id) DO NOTHING"
        )
        connection.execute(f"DROP TABLE {source_schema}.pipeline_runs")


def _remove_materialized_staging_tables(connection: duckdb.DuckDBPyConnection) -> None:
    for schema_name in ("main", "stg"):
        for table_name in ("stg_countries", "stg_observations"):
            if _table_exists(connection, schema_name, table_name):
                connection.execute(f"DROP TABLE {schema_name}.{table_name}")


def _migrate_dimensions(connection: duckdb.DuckDBPyConnection) -> None:
    expected_columns = {
        "dim_country": {"country_code", "country_name", "run_id", "load_start_date"},
        "dim_indicator": {"indicator_code", "indicator_name", "run_id", "load_start_date"},
        "dim_year": {"year", "decade", "year_start", "run_id", "load_start_date"},
    }
    migrations = {}
    for table_name, required in expected_columns.items():
        if not _table_exists(connection, "mod", table_name):
            continue
        columns = {
            row[0] for row in connection.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='mod' AND table_name=?",
                [table_name],
            ).fetchall()
        }
        audit_null_rows = 0
        if {"run_id", "load_start_date"}.issubset(columns):
            audit_null_rows = connection.execute(
                f"SELECT COUNT(*) FROM mod.{table_name} WHERE run_id IS NULL OR load_start_date IS NULL"
            ).fetchone()[0]
        year_type = None
        if table_name == "dim_year":
            year_type = connection.execute(
                "SELECT data_type FROM information_schema.columns WHERE table_schema='mod' AND table_name='dim_year' AND column_name='year'"
            ).fetchone()[0]
        if not required.issubset(columns) or audit_null_rows or year_type != ("VARCHAR" if table_name == "dim_year" else year_type):
            migrations[table_name] = columns
    if not migrations:
        return

    if _table_exists(connection, "mod", "fact_indicator_observation"):
        connection.execute("""
            INSERT INTO sys.pipeline_runs (run_id, load_start_date, finished_at, status, error_message)
            SELECT CAST(source_run_id AS BIGINT), MIN(fetched_at), MIN(fetched_at), 'succeeded', NULL
            FROM mod.fact_indicator_observation
            GROUP BY CAST(source_run_id AS BIGINT)
            ON CONFLICT (run_id) DO NOTHING
        """)
    earliest_run = "(SELECT run_id FROM sys.pipeline_runs ORDER BY load_start_date, run_id LIMIT 1)"
    source_columns = {
        "dim_country": (
            "source_run_id", "source_path", "source_page", "source_record_index",
            "source_response_sha256", "source_url",
        ),
        "dim_indicator": (),
        "dim_year": (),
    }

    connection.execute("BEGIN TRANSACTION")
    try:
        for table_name, columns in migrations.items():
            table = f"mod.{table_name}"
            if "run_id" not in columns:
                connection.execute(f"ALTER TABLE {table} ADD COLUMN run_id BIGINT")
            if "load_start_date" not in columns:
                connection.execute(f"ALTER TABLE {table} ADD COLUMN load_start_date TIMESTAMP")

            if "source_run_id" in columns:
                preferred_run = "TRY_CAST(dimension.source_run_id AS BIGINT)"
            elif table_name == "dim_country":
                preferred_run = "(SELECT MIN(source_run_id) FROM mod.fact_indicator_observation WHERE country_code = dimension.country_code)"
            elif table_name == "dim_indicator":
                preferred_run = "(SELECT MIN(source_run_id) FROM mod.fact_indicator_observation WHERE indicator_code = dimension.indicator_code)"
            else:
                preferred_run = "(SELECT MIN(source_run_id) FROM mod.fact_indicator_observation WHERE year = CAST(dimension.year AS VARCHAR))"

            connection.execute(f"""
                UPDATE {table} AS dimension
                SET run_id = COALESCE(
                    (SELECT run_id FROM sys.pipeline_runs WHERE run_id = {preferred_run}),
                    {earliest_run}
                )
                WHERE run_id IS NULL
            """)
            connection.execute(f"""
                UPDATE {table} AS dimension
                SET load_start_date = (
                    SELECT load_start_date FROM sys.pipeline_runs WHERE run_id = dimension.run_id
                )
                WHERE load_start_date IS NULL
            """)
            missing_audit = connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id IS NULL OR load_start_date IS NULL"
            ).fetchone()[0]
            if missing_audit:
                raise RuntimeError(
                    f"Cannot assign dimension audit columns for {missing_audit} rows in {table}"
                )
            has_fact_foreign_key = connection.execute(
                """SELECT COUNT(*) FROM duckdb_constraints()
                   WHERE schema_name='mod' AND referenced_table=?
                     AND constraint_type='FOREIGN KEY'""",
                [table_name],
            ).fetchone()[0]
            if not has_fact_foreign_key:
                for column in source_columns[table_name]:
                    if column in columns:
                        connection.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
        connection.execute("COMMIT")
        for table_name in migrations:
            table = f"mod.{table_name}"
            has_fact_foreign_key = connection.execute(
                """SELECT COUNT(*) FROM duckdb_constraints()
                   WHERE schema_name='mod' AND referenced_table=?
                     AND constraint_type='FOREIGN KEY'""",
                [table_name],
            ).fetchone()[0]
            if not has_fact_foreign_key:
                connection.execute(f"ALTER TABLE {table} ALTER COLUMN run_id SET NOT NULL")
                connection.execute(f"ALTER TABLE {table} ALTER COLUMN load_start_date SET NOT NULL")
    except Exception:
        if connection.execute("SELECT current_transaction_id() IS NOT NULL").fetchone()[0]:
            connection.execute("ROLLBACK")
        raise


def _migrate_year_columns(connection: duckdb.DuckDBPyConnection) -> None:
    definitions = {
        ("ing", "run_observation_scope"): (
            "CREATE TABLE {target} (run_id BIGINT NOT NULL, country_code VARCHAR NOT NULL, "
            "indicator_code VARCHAR NOT NULL, year VARCHAR NOT NULL, "
            "PRIMARY KEY (run_id, country_code, indicator_code, year))",
            "SELECT run_id, country_code, indicator_code, CAST(year AS VARCHAR) AS year FROM {source}",
        ),
        ("mod", "dim_year"): (
            "CREATE TABLE {target} (year VARCHAR PRIMARY KEY, decade VARCHAR NOT NULL, year_start DATE NOT NULL, run_id BIGINT NOT NULL, load_start_date TIMESTAMP NOT NULL)",
            "SELECT CAST(year AS VARCHAR) AS year, CAST((TRY_CAST(year AS INTEGER) // 10) * 10 AS VARCHAR) || 's' AS decade, year_start, run_id, load_start_date FROM {source}",
        ),
        ("mod", "fact_indicator_observation"): (
            "CREATE TABLE {target} (country_code VARCHAR NOT NULL, indicator_code VARCHAR NOT NULL, "
            "year VARCHAR NOT NULL, value DOUBLE, observation_status VARCHAR NOT NULL "
            "CHECK (observation_status IN ('complete', 'incomplete', 'missing')), "
            "source_run_id BIGINT NOT NULL, source_path VARCHAR NOT NULL, source_page INTEGER NOT NULL, "
            "source_record_index INTEGER, source_response_sha256 VARCHAR NOT NULL, source_url VARCHAR NOT NULL, "
            "fetched_at TIMESTAMP NOT NULL, PRIMARY KEY (country_code, indicator_code, year))",
            "SELECT country_code, indicator_code, CAST(year AS VARCHAR) AS year, value, observation_status, "
            "source_run_id, source_path, source_page, source_record_index, source_response_sha256, source_url, fetched_at "
            "FROM {source}",
        ),
    }
    columns = connection.execute(
        """SELECT table_schema, table_name, column_name, data_type
           FROM information_schema.columns
           WHERE (table_schema, table_name) IN (
               ('ing', 'run_observation_scope'), ('mod', 'dim_year'),
               ('mod', 'fact_indicator_observation')
           )"""
    ).fetchall()
    needs_migration = any(
        (column_name == "year" or (table_name == "dim_year" and column_name == "decade"))
        and data_type != "VARCHAR"
        for _, table_name, column_name, data_type in columns
    )
    if not needs_migration:
        return

    for view_name in (
        "prl.gdp_consistency", "prl.income_group_comparison", "prl.latest_comparison",
        "prl.missing_or_incomplete", "prl.year_over_year",
        "stg.vw_observation", "stg.vw_indicator_observation", "stg.vw_country", "stg.vw_current_run",
    ):
        connection.execute(f"DROP VIEW IF EXISTS {view_name}")

    migration_order = (
        ("ing", "run_observation_scope"),
        ("mod", "fact_indicator_observation"),
        ("mod", "dim_year"),
    )
    for schema_name, table_name in migration_order:
        create_sql, select_sql = definitions[(schema_name, table_name)]
        if not _table_exists(connection, schema_name, table_name):
            continue
        year_type = connection.execute(
            """SELECT data_type FROM information_schema.columns
               WHERE table_schema = ? AND table_name = ? AND column_name = 'year'""",
            [schema_name, table_name],
        ).fetchone()[0]
        decade_type = None
        if table_name == "dim_year":
            decade_type = connection.execute(
                """SELECT data_type FROM information_schema.columns
                   WHERE table_schema = ? AND table_name = ? AND column_name = 'decade'""",
                [schema_name, table_name],
            ).fetchone()[0]
        if year_type == "VARCHAR" and (decade_type is None or decade_type == "VARCHAR"):
            continue
        source = f"{schema_name}.{table_name}"
        target = f"{schema_name}.{table_name}__year_migration"
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(create_sql.format(target=target))
            connection.execute(
                f"INSERT INTO {target} {select_sql.format(source=source)}"
            )
            connection.execute(f"DROP TABLE {source}")
            connection.execute(f"ALTER TABLE {target} RENAME TO {table_name}")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise


def _migrate_fact_foreign_keys(connection: duckdb.DuckDBPyConnection) -> None:
    fact_exists = _table_exists(connection, "mod", "fact_indicator_observation")
    if not fact_exists:
        return
    foreign_keys = connection.execute(
        """SELECT constraint_column_names, referenced_table, referenced_column_names
           FROM duckdb_constraints()
           WHERE schema_name = 'mod'
             AND table_name = 'fact_indicator_observation'
             AND constraint_type = 'FOREIGN KEY'"""
    ).fetchall()
    required = {
        (("country_code",), "dim_country", ("country_code",)),
        (("indicator_code",), "dim_indicator", ("indicator_code",)),
        (("year",), "dim_year", ("year",)),
    }
    present = {
        (tuple(source_columns), referenced_table, tuple(referenced_columns))
        for source_columns, referenced_table, referenced_columns in foreign_keys
    }
    if required.issubset(present):
        return

    dangling = connection.execute("""
        SELECT COUNT(*)
        FROM mod.fact_indicator_observation AS fact
        LEFT JOIN mod.dim_country AS country USING (country_code)
        LEFT JOIN mod.dim_indicator AS indicator USING (indicator_code)
        LEFT JOIN mod.dim_year AS calendar_year USING (year)
        WHERE country.country_code IS NULL
           OR indicator.indicator_code IS NULL
           OR calendar_year.year IS NULL
    """).fetchone()[0]
    if dangling:
        raise ValueError(f"Cannot add fact dimension foreign keys: {dangling} rows have missing dimensions")

    for view_name in (
        "prl.gdp_consistency", "prl.income_group_comparison", "prl.latest_comparison",
        "prl.missing_or_incomplete", "prl.year_over_year",
    ):
        connection.execute(f"DROP VIEW IF EXISTS {view_name}")

    shadow_table = "mod.fact_indicator_observation__fk_migration"
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(f"DROP TABLE IF EXISTS {shadow_table}")
        connection.execute(f"""
            CREATE TABLE {shadow_table} (
                country_code VARCHAR NOT NULL,
                indicator_code VARCHAR NOT NULL,
                year VARCHAR NOT NULL,
                value DOUBLE,
                observation_status VARCHAR NOT NULL CHECK (
                    observation_status IN ('complete', 'incomplete', 'missing')
                ),
                source_run_id BIGINT NOT NULL,
                source_path VARCHAR NOT NULL,
                source_page INTEGER NOT NULL,
                source_record_index INTEGER,
                source_response_sha256 VARCHAR NOT NULL,
                source_url VARCHAR NOT NULL,
                fetched_at TIMESTAMP NOT NULL,
                PRIMARY KEY (country_code, indicator_code, year),
                FOREIGN KEY (country_code) REFERENCES mod.dim_country(country_code),
                FOREIGN KEY (indicator_code) REFERENCES mod.dim_indicator(indicator_code),
                FOREIGN KEY (year) REFERENCES mod.dim_year(year)
            )
        """)
        connection.execute(f"INSERT INTO {shadow_table} SELECT * FROM mod.fact_indicator_observation")
        connection.execute("DROP TABLE mod.fact_indicator_observation")
        connection.execute(f"ALTER TABLE {shadow_table} RENAME TO fact_indicator_observation")
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def _restore_source_lineage(connection: duckdb.DuckDBPyConnection) -> None:
    if not _table_exists(connection, "mod", "dim_country"):
        return

    connection.execute("""
        UPDATE mod.fact_indicator_observation AS fact
        SET source_run_id = raw.run_id,
            source_path = raw.source_path,
            source_page = raw.source_page,
            source_record_index = raw.source_record_index,
            source_url = raw.source_url,
            fetched_at = raw.fetched_at
        FROM (
            SELECT run_id, country_code, indicator_code,
                   TRY_CAST(json_extract_string(payload_json, '$.date') AS VARCHAR) AS year,
                   TRY_CAST(json_extract_string(payload_json, '$.value') AS DOUBLE) AS value,
                   source_page, source_record_index, source_path, source_response_sha256,
                   source_url, fetched_at
            FROM ing.raw_indicator_observations
            WHERE source_record_index IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY country_code, indicator_code,
                             TRY_CAST(json_extract_string(payload_json, '$.date') AS VARCHAR),
                             source_response_sha256
                ORDER BY run_id, source_page, source_record_index
            ) = 1
        ) AS raw
        WHERE fact.country_code = raw.country_code
          AND fact.indicator_code = raw.indicator_code
          AND fact.year = raw.year
          AND fact.value IS NOT DISTINCT FROM raw.value
          AND fact.source_response_sha256 = raw.source_response_sha256
    """)

    connection.execute("""
        UPDATE mod.fact_indicator_observation AS fact
        SET source_run_id = raw.run_id,
            source_path = raw.raw_path,
            source_page = raw.page_number,
            source_record_index = NULL,
            source_url = raw.request_url,
            fetched_at = raw.fetched_at
        FROM (
            SELECT response.run_id, response.country_code, response.indicator_code,
                   response.page_number, response.raw_path, response.response_sha256,
                   response.request_url, response.fetched_at,
                   scope.year
            FROM ing.raw_responses AS response
            JOIN ing.run_observation_scope AS scope
              ON scope.run_id = response.run_id
             AND scope.country_code = response.country_code
             AND scope.indicator_code = response.indicator_code
            WHERE response.endpoint_type = 'indicators'
              AND response.page_number = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM ing.raw_indicator_observations AS observation
                  WHERE observation.run_id = response.run_id
                    AND observation.country_code = response.country_code
                    AND observation.indicator_code = response.indicator_code
                    AND TRY_CAST(json_extract_string(observation.payload_json, '$.date') AS VARCHAR) = scope.year
              )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY response.country_code, response.indicator_code, scope.year,
                             response.response_sha256
                ORDER BY response.run_id, response.page_number
            ) = 1
        ) AS raw
        WHERE fact.country_code = raw.country_code
          AND fact.indicator_code = raw.indicator_code
          AND fact.year = raw.year
          AND fact.observation_status = 'missing'
          AND fact.value IS NULL
          AND fact.source_response_sha256 = raw.response_sha256
    """)


def _migrate_legacy_model_tables(connection: duckdb.DuckDBPyConnection) -> None:
    legacy_objects_found = any(
        _table_exists(connection, schema_name, table_name)
        for schema_name in ("main", "mod")
        for table_name in ("countries", "indicators", "observations")
    )
    if legacy_objects_found:
        presentation_views = connection.execute(
            "SELECT table_name FROM information_schema.views WHERE table_schema = 'prl'"
        ).fetchall()
        for (view_name,) in presentation_views:
            connection.execute(f"DROP VIEW prl.{view_name}")

    for source_schema in ("main", "mod"):
        country_exists = _table_exists(connection, source_schema, "countries")
        indicator_exists = _table_exists(connection, source_schema, "indicators")
        observation_exists = _table_exists(connection, source_schema, "observations")
        if not any((country_exists, indicator_exists, observation_exists)):
            continue

        connection.execute("BEGIN TRANSACTION")
        try:
            if country_exists:
                connection.execute(f"""
                    INSERT INTO mod.dim_country (
                        country_code, country_name, region_id, region_name, income_group_id,
                        income_group_name, lending_type_id, lending_type_name, capital_city,
                        longitude, latitude, run_id, load_start_date
                    )
                    SELECT country_code, country_name, region_id, region_name, income_group_id,
                           income_group_name, lending_type_id, lending_type_name, capital_city,
                           longitude, latitude,
                           COALESCE((SELECT run_id FROM sys.pipeline_runs WHERE run_id=TRY_CAST(source_run_id AS BIGINT)),
                                    (SELECT run_id FROM sys.pipeline_runs ORDER BY load_start_date, run_id LIMIT 1)),
                           COALESCE((SELECT load_start_date FROM sys.pipeline_runs WHERE run_id=TRY_CAST(source_run_id AS BIGINT)),
                                    (SELECT load_start_date FROM sys.pipeline_runs ORDER BY load_start_date, run_id LIMIT 1))
                    FROM {source_schema}.countries
                    ON CONFLICT (country_code) DO UPDATE SET
                        country_name = excluded.country_name, region_id = excluded.region_id,
                        region_name = excluded.region_name, income_group_id = excluded.income_group_id,
                        income_group_name = excluded.income_group_name,
                        lending_type_id = excluded.lending_type_id, lending_type_name = excluded.lending_type_name,
                        capital_city = excluded.capital_city, longitude = excluded.longitude,
                        latitude = excluded.latitude, run_id = excluded.run_id,
                        load_start_date = excluded.load_start_date
                """)
            if indicator_exists:
                connection.execute(f"""
                    INSERT INTO mod.dim_indicator (indicator_code, indicator_name, run_id, load_start_date)
                    SELECT indicator_code, indicator_name,
                           (SELECT run_id FROM sys.pipeline_runs ORDER BY load_start_date, run_id LIMIT 1),
                           (SELECT load_start_date FROM sys.pipeline_runs ORDER BY load_start_date, run_id LIMIT 1)
                    FROM {source_schema}.indicators
                    ON CONFLICT (indicator_code) DO UPDATE SET
                        indicator_name = excluded.indicator_name, run_id = excluded.run_id,
                        load_start_date = excluded.load_start_date
                """)
            if observation_exists:
                connection.execute(f"""
                    INSERT INTO mod.dim_year (year, decade, year_start, run_id, load_start_date)
                          SELECT DISTINCT CAST(year AS VARCHAR),
                              CAST((CAST(year AS INTEGER) // 10) * 10 AS VARCHAR) || 's',
                              make_date(CAST(year AS INTEGER), 1, 1),
                              (SELECT run_id FROM sys.pipeline_runs ORDER BY load_start_date, run_id LIMIT 1),
                              (SELECT load_start_date FROM sys.pipeline_runs ORDER BY load_start_date, run_id LIMIT 1)
                    FROM {source_schema}.observations
                    ON CONFLICT (year) DO NOTHING
                """)
                connection.execute(f"""
                    INSERT INTO mod.fact_indicator_observation
                    SELECT country_code, indicator_code, year, value, observation_status,
                           source_run_id, source_path, source_page, source_record_index,
                           source_response_sha256, source_url, fetched_at
                    FROM {source_schema}.observations
                    ON CONFLICT (country_code, indicator_code, year) DO UPDATE SET
                        value = excluded.value, observation_status = excluded.observation_status,
                        source_run_id = excluded.source_run_id, source_path = excluded.source_path,
                        source_page = excluded.source_page, source_record_index = excluded.source_record_index,
                        source_response_sha256 = excluded.source_response_sha256,
                        source_url = excluded.source_url, fetched_at = excluded.fetched_at
                """)
            for table_name, exists in (
                ("observations", observation_exists),
                ("indicators", indicator_exists),
                ("countries", country_exists),
            ):
                if exists:
                    connection.execute(f"DROP TABLE {source_schema}.{table_name}")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise


def _table_exists(connection: duckdb.DuckDBPyConnection, schema_name: str, table_name: str) -> bool:
    return bool(connection.execute(
        """SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema = ? AND table_name = ?""",
        [schema_name, table_name],
    ).fetchone()[0])


def persist_raw_layer(
    connection: duckdb.DuckDBPyConnection,
    raw_responses: list[tuple],
    raw_countries: list[tuple],
    raw_observations: list[tuple],
    observation_scope: list[tuple],
) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.executemany(
            "INSERT INTO ing.raw_responses VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            raw_responses,
        )
        connection.executemany(
            "INSERT INTO ing.raw_country_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            raw_countries,
        )
        connection.executemany(
            "INSERT INTO ing.raw_indicator_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            raw_observations,
        )
        connection.executemany(
            "INSERT INTO ing.run_observation_scope VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
            observation_scope,
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def record_ingestion_quality_metrics(
    connection: duckdb.DuckDBPyConnection,
    run_id: int,
) -> None:
    tables = (
        "raw_responses",
        "raw_country_metadata",
        "raw_indicator_observations",
        "run_observation_scope",
    )
    connection.execute("BEGIN TRANSACTION")
    try:
        for table_name in tables:
            columns = [
                row[0] for row in connection.execute(
                    """SELECT column_name FROM information_schema.columns
                       WHERE table_schema='ing' AND table_name=? ORDER BY ordinal_position""",
                    [table_name],
                ).fetchall()
            ]
            null_expressions = ", ".join(
                f"COUNT(*) FILTER (WHERE {column_name} IS NULL) AS {column_name}"
                for column_name in columns
            )
            row = connection.execute(
                f"""SELECT COUNT(*) AS row_count, {null_expressions}
                    FROM ing.{table_name} WHERE run_id = ?""",
                [run_id],
            ).fetchone()
            row_count = int(row[0])
            null_counts = {
                column_name: int(null_count)
                for column_name, null_count in zip(columns, row[1:], strict=True)
                if null_count
            }
            sql_null_count = sum(null_counts.values())
            json_value_null_count = 0
            if table_name == "raw_indicator_observations":
                json_value_null_count = int(connection.execute(
                    """SELECT COUNT(*) FROM ing.raw_indicator_observations
                       WHERE run_id = ? AND json_type(payload_json, '$.value') = 'NULL'""",
                    [run_id],
                ).fetchone()[0])
            connection.execute(
                """INSERT INTO sys.ingestion_quality_metrics
                   (run_id, table_name, row_count, sql_null_count, sql_null_counts,
                    json_value_null_count, measured_at)
                   VALUES (?, ?, ?, ?, ?::JSON, ?, current_timestamp)
                   ON CONFLICT (run_id, table_name) DO UPDATE SET
                       row_count = excluded.row_count,
                       sql_null_count = excluded.sql_null_count,
                       sql_null_counts = excluded.sql_null_counts,
                       json_value_null_count = excluded.json_value_null_count,
                       measured_at = excluded.measured_at""",
                [run_id, table_name, row_count, sql_null_count, json.dumps(null_counts), json_value_null_count],
            )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def publish_staging(
    connection: duckdb.DuckDBPyConnection,
    run_id: int,
    indicators: dict[str, str],
    validator: Callable[[duckdb.DuckDBPyConnection], None] | None = None,
) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        load_start_date = connection.execute(
            "SELECT load_start_date FROM sys.pipeline_runs WHERE run_id = ?", [run_id]
        ).fetchone()[0]
        connection.execute(
            """DELETE FROM mod.fact_indicator_observation AS fact
            WHERE NOT EXISTS (
                SELECT 1
                FROM ing.run_observation_scope AS scope
                WHERE scope.run_id = ?
                  AND scope.country_code = fact.country_code
                  AND scope.indicator_code = fact.indicator_code
                  AND scope.year = fact.year
            )""",
            [run_id],
        )
        country_columns = {
            row[0] for row in connection.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema='mod' AND table_name='dim_country'"""
            ).fetchall()
        }
        legacy_source_columns = (
            "source_run_id", "source_path", "source_page", "source_record_index",
            "source_response_sha256", "source_url",
        )
        retained_source_columns = [
            column for column in legacy_source_columns if column in country_columns
        ]
        country_insert_columns = [
            "country_code", "country_name", "region_id", "region_name", "income_group_id",
            "income_group_name", "lending_type_id", "lending_type_name", "capital_city",
            "longitude", "latitude", "run_id", "load_start_date", *retained_source_columns,
        ]
        country_select_columns = [
            "country_code", "country_name", "region_id", "region_name", "income_group_id",
            "income_group_name", "lending_type_id", "lending_type_name", "capital_city",
            "longitude", "latitude", "run_id", "?", *retained_source_columns,
        ]
        legacy_updates = ",\n                ".join(
            f"{column} = excluded.{column}" for column in retained_source_columns
        )
        if legacy_updates:
            legacy_updates = ",\n                " + legacy_updates
        connection.execute(
            f"""INSERT INTO mod.dim_country ({', '.join(country_insert_columns)})
            SELECT {', '.join(country_select_columns)}
            FROM stg.vw_country
            WHERE run_id = ?
            ON CONFLICT (country_code) DO UPDATE SET
                country_name = excluded.country_name, region_id = excluded.region_id,
                region_name = excluded.region_name, income_group_id = excluded.income_group_id,
                income_group_name = excluded.income_group_name, lending_type_id = excluded.lending_type_id,
                lending_type_name = excluded.lending_type_name, capital_city = excluded.capital_city,
                longitude = excluded.longitude, latitude = excluded.latitude,
                run_id = excluded.run_id, load_start_date = excluded.load_start_date{legacy_updates}
            WHERE mod.dim_country.country_name IS DISTINCT FROM excluded.country_name
               OR mod.dim_country.region_id IS DISTINCT FROM excluded.region_id
               OR mod.dim_country.region_name IS DISTINCT FROM excluded.region_name
               OR mod.dim_country.income_group_id IS DISTINCT FROM excluded.income_group_id
               OR mod.dim_country.income_group_name IS DISTINCT FROM excluded.income_group_name
               OR mod.dim_country.lending_type_id IS DISTINCT FROM excluded.lending_type_id
               OR mod.dim_country.lending_type_name IS DISTINCT FROM excluded.lending_type_name
               OR mod.dim_country.capital_city IS DISTINCT FROM excluded.capital_city
               OR mod.dim_country.longitude IS DISTINCT FROM excluded.longitude
               OR mod.dim_country.latitude IS DISTINCT FROM excluded.latitude""",
            [load_start_date, run_id],
        )
        connection.executemany(
            """INSERT INTO mod.dim_indicator VALUES (?, ?, ?, ?)
            ON CONFLICT (indicator_code) DO UPDATE SET
                indicator_name = excluded.indicator_name, run_id = excluded.run_id,
                load_start_date = excluded.load_start_date
            WHERE mod.dim_indicator.indicator_name IS DISTINCT FROM excluded.indicator_name""",
            [(code, name, run_id, load_start_date) for code, name in indicators.items()],
        )
        connection.execute(
            """INSERT INTO mod.dim_year
                 SELECT DISTINCT CAST(year AS VARCHAR), CAST((CAST(year AS INTEGER) // 10) * 10 AS VARCHAR) || 's',
                     make_date(CAST(year AS INTEGER), 1, 1), run_id, ?
            FROM stg.vw_observation
            WHERE run_id = ?
            ON CONFLICT (year) DO NOTHING""",
            [load_start_date, run_id],
        )
        connection.execute(
            """INSERT INTO mod.fact_indicator_observation
            SELECT country_code, indicator_code, CAST(year AS VARCHAR), value, observation_status,
                   source_run_id, source_path, source_page, source_record_index,
                   source_response_sha256, source_url, fetched_at
            FROM stg.vw_observation
            WHERE run_id = ?
            ON CONFLICT (country_code, indicator_code, year) DO UPDATE SET
                value = excluded.value, observation_status = excluded.observation_status,
                source_run_id = excluded.source_run_id, source_path = excluded.source_path,
                source_page = excluded.source_page, source_record_index = excluded.source_record_index,
                source_response_sha256 = excluded.source_response_sha256,
                source_url = excluded.source_url, fetched_at = excluded.fetched_at
            WHERE mod.fact_indicator_observation.value IS DISTINCT FROM excluded.value
               OR mod.fact_indicator_observation.observation_status IS DISTINCT FROM excluded.observation_status""",
            [run_id],
        )
        if validator:
            validator(connection)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


