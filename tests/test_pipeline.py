from __future__ import annotations

import io
import json
import tempfile
import urllib.error
import unittest
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from world_bank_pipeline.client import ApiPage, WorldBankApiError, WorldBankClient
from world_bank_pipeline.config import PipelineConfig, load_config
from world_bank_pipeline.pipeline import _format_run_id, _indicator_names_from_raw, _run_folder_name
from world_bank_pipeline.quality import run_quality_checks
from world_bank_pipeline.store import initialize_database, persist_raw_layer, publish_staging


class WorldBankClientTests(unittest.TestCase):
    def test_run_id_format_and_windows_safe_folder(self):
        run_id = _format_run_id(datetime(2026, 10, 23, 14, 56, 31, tzinfo=timezone.utc))
        self.assertEqual(run_id, "2026_10_23_14:56:31")
        self.assertEqual(_run_folder_name(run_id), "2026_10_23_14-56-31")

    def test_get_pages_follows_api_pagination(self):
        responses = [
            [{"page": 1, "pages": 2}, [{"date": "2020"}]],
            [{"page": 2, "pages": 2}, [{"date": "2021"}]],
        ]
        requested_urls = []

        def opener(url, timeout):
            requested_urls.append(url)
            return io.BytesIO(json.dumps(responses[len(requested_urls) - 1]).encode())

        client = WorldBankClient("https://example.test", opener=opener, sleep=lambda _: None)
        pages = client.get_pages("country/BRA/indicator/TEST", {"date": "2020:2021"})
        self.assertEqual([page.page_number for page in pages], [1, 2])
        self.assertIn("page=1", requested_urls[0])
        self.assertIn("page=2", requested_urls[1])

    def test_retries_transient_http_failure(self):
        attempts = []
        delays = []

        def opener(url, timeout):
            attempts.append(url)
            if len(attempts) == 1:
                raise urllib.error.HTTPError(url, 503, "Unavailable", {}, None)
            return io.BytesIO(json.dumps([{"page": 1, "pages": 1}, []]).encode())

        client = WorldBankClient(
            "https://example.test", max_retries=2, retry_backoff_seconds=0.25,
            opener=opener, sleep=delays.append,
        )
        self.assertEqual(len(client.get_pages("country/BRA", {})), 1)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(delays, [0.25])

    def test_retries_http_408_timeout(self):
        attempts = []

        def opener(url, timeout):
            attempts.append(url)
            if len(attempts) == 1:
                raise urllib.error.HTTPError(url, 408, "Request Timeout", {}, None)
            return io.BytesIO(json.dumps([{"page": 1, "pages": 1}, []]).encode())

        client = WorldBankClient("https://example.test", max_retries=1, opener=opener, sleep=lambda _: None)
        self.assertEqual(len(client.get_pages("country/BRA", {})), 1)
        self.assertEqual(len(attempts), 2)

    def test_indicator_names_prefer_world_bank_response(self):
        rows = [
            ("run", "BRA", "SP.POP.TOTL", 1, 0, json.dumps({
                "indicator": {"id": "SP.POP.TOTL", "value": "Population, total"},
            })),
        ]
        names = _indicator_names_from_raw(rows, {"SP.POP.TOTL": "configured fallback"})
        self.assertEqual(names["SP.POP.TOTL"], "Population, total")

    def test_indicator_name_uses_config_fallback_if_payload_omits_it(self):
        rows = [("run", "BRA", "TEST", 1, 0, json.dumps({"indicator": {"id": "TEST"}}))]
        names = _indicator_names_from_raw(rows, {"TEST": "Configured label"})
        self.assertEqual(names["TEST"], "Configured label")

    def test_does_not_retry_permanent_http_failure(self):
        attempts = []

        def opener(url, timeout):
            attempts.append(url)
            raise urllib.error.HTTPError(url, 400, "Bad Request", {}, None)

        client = WorldBankClient("https://example.test", max_retries=3, opener=opener, sleep=lambda _: None)
        with self.assertRaises(WorldBankApiError):
            client.get_pages("country/BRA", {})
        self.assertEqual(len(attempts), 1)

class StoreTests(unittest.TestCase):
    def test_quality_check_rejects_complete_status_with_null_value(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_path = root / "raw.json"
            raw_path.write_text("{}", encoding="utf-8")
            config = PipelineConfig(
                root=root, base_url="https://example.test", timeout_seconds=1,
                max_retries=0, retry_backoff_seconds=0, start_year=2020, end_year=2020,
                raw_dir=root, database_path=root / "db.duckdb", reports_dir=root / "reports",
                countries=("BRA",), indicators={"TEST": "Test indicator"},
            )
            connection = initialize_database(":memory:")
            connection.execute("""INSERT INTO mod.dim_country VALUES
                ('BRA', 'Brazil', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 'run-1', 'raw.json', 1, 0, 'country-hash', 'country-url')""")
            connection.execute("INSERT INTO mod.dim_indicator VALUES ('TEST', 'Test indicator')")
            connection.execute("INSERT INTO mod.dim_year VALUES ('2020', '2020s', DATE '2020-01-01')")
            connection.execute("""INSERT INTO mod.fact_indicator_observation VALUES
                ('BRA', 'TEST', '2020', NULL, 'complete', 'run-1', 'raw.json', 1, 0,
                 'hash', 'url', current_timestamp)""")
            with self.assertRaisesRegex(ValueError, "status/value inconsistencies"):
                run_quality_checks(connection, config)
            connection.close()

    def test_quality_check_warns_on_related_indicator_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "raw.json").write_text("{}", encoding="utf-8")
            indicator_names = {
                "SP.POP.TOTL": "Population, total",
                "NY.GDP.MKTP.CD": "GDP, current US dollars",
                "NY.GDP.PCAP.CD": "GDP per capita, current US dollars",
            }
            config = PipelineConfig(
                root=root, base_url="https://example.test", timeout_seconds=1,
                max_retries=0, retry_backoff_seconds=0, start_year=2020, end_year=2020,
                raw_dir=root, database_path=root / "db.duckdb", reports_dir=root / "reports",
                countries=("BRA",), indicators=indicator_names,
            )
            connection = initialize_database(":memory:")
            connection.execute("""INSERT INTO mod.dim_country VALUES
                ('BRA', 'Brazil', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 'run-1', 'raw.json', 1, 0, 'country-hash', 'country-url')""")
            connection.executemany("INSERT INTO mod.dim_indicator VALUES (?, ?)", list(indicator_names.items()))
            connection.execute("INSERT INTO mod.dim_year VALUES ('2020', '2020s', DATE '2020-01-01')")
            observations = [
                ("BRA", "SP.POP.TOTL", "2020", 100.0),
                ("BRA", "NY.GDP.MKTP.CD", "2020", 1000.0),
                ("BRA", "NY.GDP.PCAP.CD", "2020", 5.0),
            ]
            connection.executemany("""INSERT INTO mod.fact_indicator_observation VALUES
                (?, ?, ?, ?, 'complete', 'run-1', 'raw.json', 1, 0, 'hash', 'url', current_timestamp)""", observations)
            with self.assertLogs("world_bank_pipeline.quality", level="WARNING") as captured:
                run_quality_checks(connection, config)
            self.assertIn("GDP consistency review: 1 rows", captured.output[0])
            connection.close()

    def test_missing_report_includes_null_value_with_complete_status(self):
        connection = initialize_database(":memory:")
        connection.execute("""INSERT INTO mod.dim_country VALUES
            ('BRA', 'Brazil', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
             'run-1', 'country.json', 1, 0, 'country-hash', 'country-url')""")
        connection.execute("INSERT INTO mod.dim_indicator VALUES ('TEST', 'Test indicator')")
        connection.execute("INSERT INTO mod.dim_year VALUES ('2020', '2020s', DATE '2020-01-01')")
        connection.execute("""INSERT INTO mod.fact_indicator_observation VALUES
            ('BRA', 'TEST', '2020', NULL, 'complete', 'run-1', 'raw.json', 1, 0,
             'observation-hash', 'observation-url', current_timestamp)""")
        sql = Path(__file__).resolve().parents[1] / "sql" / "missing_or_incomplete.sql"
        connection.execute(f"CREATE OR REPLACE VIEW prl.missing_or_incomplete AS {sql.read_text(encoding='utf-8')}")
        rows = connection.execute("SELECT * FROM prl.missing_or_incomplete").fetchall()
        columns = [column[0] for column in connection.description]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][columns.index("data_quality_issue")], "complete_status_with_null_value")
        self.assertIsNone(rows[0][columns.index("value")])
        connection.close()

    def test_staging_views_show_latest_valid_run_only(self):
        connection = initialize_database(":memory:")
        connection.executemany(
            "INSERT INTO sys.pipeline_runs VALUES (?, ?, ?, ?, NULL)",
            [
                ("run-old", "2026-10-23 10:00:00", "2026-10-23 10:01:00", "succeeded"),
                ("run-new", "2026-10-23 11:00:00", "2026-10-23 11:01:00", "succeeded"),
                ("run-failed", "2026-10-23 12:00:00", "2026-10-23 12:01:00", "failed"),
            ],
        )
        country_payloads = [
            ("run-old", "Old Brazil"),
            ("run-new", "Latest Brazil"),
        ]
        for run_id, country_name in country_payloads:
            connection.execute(
                """INSERT INTO ing.raw_country_metadata VALUES
                (?, 'BRA', 1, 0, ?, 'country.json', ?, 'country-url', ? )""",
                [
                    run_id,
                    json.dumps({"id": "BRA", "name": country_name}),
                    f"{run_id}-hash",
                    "2026-10-23T10:00:00+00:00" if run_id == "run-old" else "2026-10-23T11:00:00+00:00",
                ],
            )
            connection.execute(
                """INSERT INTO ing.raw_indicator_observations VALUES
                (?, 'BRA', 'TEST', 1, 0, ?, 'indicator.json', ?, 'indicator-url', ?)""",
                [
                    run_id,
                    json.dumps({"date": "2020", "value": 100 if run_id == "run-old" else 200}),
                    f"{run_id}-indicator-hash",
                    "2026-10-23T10:00:00+00:00" if run_id == "run-old" else "2026-10-23T11:00:00+00:00",
                ],
            )

        self.assertEqual(connection.execute("SELECT COUNT(*) FROM stg.vw_country").fetchone()[0], 1)
        self.assertEqual(connection.execute("SELECT country_name FROM stg.vw_country").fetchone()[0], "Latest Brazil")
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM stg.vw_indicator_observation").fetchone()[0], 1)
        self.assertEqual(connection.execute("SELECT value FROM stg.vw_indicator_observation").fetchone()[0], 200.0)
        self.assertEqual(connection.execute("SELECT run_id FROM stg.vw_current_run").fetchone()[0], "run-new")
        connection.close()

    def test_raw_to_staging_to_analytics_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_file = root / "data" / "raw" / "fixture.json"
            raw_file.parent.mkdir(parents=True)
            raw_file.write_text("{}", encoding="utf-8")
            config = PipelineConfig(
                root=root, base_url="https://example.test", timeout_seconds=1,
                max_retries=0, retry_backoff_seconds=0, start_year=2018, end_year=2020,
                raw_dir=root / "data" / "raw", database_path=root / "data" / "db.duckdb",
                reports_dir=root / "reports", countries=("BRA",), indicators={"TEST": "Test indicator"},
            )
            connection = initialize_database(str(config.database_path))
            connection.execute(
                "INSERT INTO sys.pipeline_runs VALUES ('run-1', current_timestamp, NULL, 'running', NULL)"
            )
            country_source_path = "data/raw/country.json"
            indicator_source_path = "data/raw/indicator.json"
            (root / country_source_path).write_text("{}", encoding="utf-8")
            (root / indicator_source_path).write_text("{}", encoding="utf-8")
            fetched_at = "2026-01-01T00:00:00+00:00"
            country_url = "https://example.test/country/BRA"
            indicator_url = "https://example.test/country/BRA/indicator/TEST"
            raw_responses = [
                ("run-1", "countries", "BRA", None, 1, country_url, fetched_at, "country-hash", country_source_path),
                ("run-1", "indicators", "BRA", "TEST", 1, indicator_url, fetched_at, "indicator-hash", indicator_source_path),
            ]
            country_record = json.dumps({
                "id": "BRA", "name": "Brazil", "region": {"id": "LAC", "value": "Latin America & Caribbean"},
                "incomeLevel": {"id": "UMC", "value": "Upper middle income"},
                "lendingType": {"id": "IBD", "value": "IBRD"}, "capitalCity": "Brasilia",
                "longitude": "-47.9", "latitude": "-15.8",
            })
            raw_countries = [(
                "run-1", "BRA", 1, 0, country_record, country_source_path,
                "country-hash", country_url, fetched_at,
            ), (
                "run-1", "BRA", 2, 0,
                country_record.replace('"Brazil"', '"Duplicate Brazil"'), country_source_path,
                "country-duplicate-hash", country_url, fetched_at,
            )]
            raw_observations = [
                ("run-1", "BRA", "TEST", 1, 0, json.dumps({"date": "2018", "value": 10}), indicator_source_path, "indicator-hash", indicator_url, fetched_at),
                ("run-1", "BRA", "TEST", 2, 0, json.dumps({"date": "2018", "value": 999}), indicator_source_path, "indicator-duplicate-hash", indicator_url, fetched_at),
                ("run-1", "BRA", "TEST", 1, 1, json.dumps({"date": "2020", "value": None}), indicator_source_path, "indicator-hash", indicator_url, fetched_at),
            ]
            observation_scope = [("run-1", "BRA", "TEST", str(year)) for year in range(2018, 2021)]
            for _ in range(2):
                persist_raw_layer(connection, raw_responses, raw_countries, raw_observations, observation_scope)
                publish_staging(
                    connection, "run-1", config.indicators,
                    validator=lambda database: run_quality_checks(database, config),
                )
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM ing.raw_country_metadata WHERE run_id = 'run-1'").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM ing.raw_indicator_observations WHERE run_id = 'run-1'").fetchone()[0], 3)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM stg.vw_country WHERE run_id = 'run-1'").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM stg.vw_indicator_observation WHERE run_id = 'run-1' AND year = '2018'").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT value FROM stg.vw_indicator_observation WHERE run_id = 'run-1' AND year = '2018'").fetchone()[0], 10.0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM stg.vw_observation WHERE run_id = 'run-1'").fetchone()[0], 3)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM mod.fact_indicator_observation").fetchone()[0], 3)
            self.assertEqual(connection.execute("SELECT observation_status FROM mod.fact_indicator_observation WHERE year = '2019'").fetchone()[0], "missing")
            self.assertEqual(connection.execute("SELECT value FROM stg.vw_observation WHERE year = '2018'").fetchone()[0], 10.0)
            self.assertEqual(connection.execute("SELECT typeof(year), typeof(year_start) FROM stg.vw_observation LIMIT 1").fetchone(), ("VARCHAR", "DATE"))
            self.assertEqual(connection.execute("SELECT year, year_start FROM stg.vw_observation WHERE year = '2018'").fetchone(), ("2018", datetime(2018, 1, 1).date()))
            self.assertEqual(connection.execute("SELECT country_name FROM stg.vw_country WHERE run_id = 'run-1' AND country_code = 'BRA'").fetchone()[0], "Brazil")
            self.assertEqual(
                connection.execute("SELECT table_type FROM information_schema.tables WHERE table_schema = 'stg' AND table_name = 'vw_observation'").fetchone()[0],
                "VIEW",
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'").fetchone()[0],
                0,
            )
            run_quality_checks(connection, config)
            connection.close()

    def test_all_reporting_queries_execute(self):
        with tempfile.TemporaryDirectory() as directory:
            config = load_config(Path(__file__).resolve().parents[1] / "config.toml")
            connection = duckdb.connect()
            connection = initialize_database(":memory:")
            connection.execute("INSERT INTO mod.dim_country VALUES ('BRA', 'Brazil', NULL, NULL, 'UMC', 'Upper middle income', NULL, NULL, NULL, NULL, NULL, 'run', 'country.json', 1, 0, 'country-hash', 'country-url')")
            for code, label in config.indicators.items():
                connection.execute("INSERT INTO mod.dim_indicator VALUES (?, ?)", [code, label])
                for year in range(2018, 2025):
                    connection.execute(
                        "INSERT INTO mod.dim_year VALUES (?, ?, make_date(?, 1, 1)) ON CONFLICT DO NOTHING",
                        [str(year), f"{(year // 10) * 10}s", year],
                    )
                    connection.execute(
                        "INSERT INTO mod.fact_indicator_observation VALUES ('BRA', ?, ?, ?, 'complete', 'run', 'raw.json', 1, 0, 'hash', 'url', current_timestamp)",
                        [code, year, float(year)],
                    )
            sql_dir = Path(__file__).resolve().parents[1] / "sql"
            for query_path in sql_dir.glob("*.sql"):
                with self.subTest(query=query_path.name):
                    view_name = query_path.stem
                    view_sql = query_path.read_text(encoding="utf-8").strip().rstrip(";")
                    connection.execute(f"CREATE OR REPLACE VIEW prl.{view_name} AS {view_sql}")
                    cursor = connection.execute(f"SELECT * FROM prl.{view_name}")
                    rows = cursor.fetchall()
                    columns = [column[0] for column in cursor.description]
                    if "year" in columns:
                        view_types = {
                            description[0]: description[1]
                            for description in connection.execute(f"DESCRIBE prl.{view_name}").fetchall()
                        }
                        self.assertEqual(view_types["year"], "VARCHAR")
                        self.assertEqual(view_types["year_start"], "DATE")
                        if rows:
                            expected_year = "2024" if view_name == "latest_comparison" else "2018"
                            self.assertEqual(rows[0][columns.index("year")], expected_year)
                            expected_date = "2024-01-01" if view_name == "latest_comparison" else "2018-01-01"
                            self.assertEqual(rows[0][columns.index("year_start")].isoformat(), expected_date)
                    if query_path.name == "latest_comparison.sql":
                        self.assertIn("source_response_sha256", columns)
                        self.assertIn("country_metadata_source_response_sha256", columns)
                        self.assertEqual(rows[0][columns.index("source_path")], "raw.json")
                        self.assertEqual(rows[0][columns.index("country_metadata_source_path")], "country.json")
                    elif query_path.name == "year_over_year.sql":
                        self.assertIn("prior_source_response_sha256", columns)
                        self.assertIn("country_metadata_source_response_sha256", columns)
                        self.assertEqual(columns.index("prior_year_value"), columns.index("value") + 1)
                        self.assertEqual(columns.index("absolute_change"), columns.index("prior_year_value") + 1)
                        self.assertEqual(rows[1][columns.index("prior_year")], "2018")
                        self.assertEqual(rows[1][columns.index("prior_year_start")].isoformat(), "2018-01-01")
                        self.assertEqual(rows[1][columns.index("source_path")], "raw.json")
                        self.assertEqual(rows[1][columns.index("prior_source_path")], "raw.json")
                    elif query_path.name == "income_group_comparison.sql":
                        self.assertIn("contributing_observation_lineage", columns)
                        self.assertIn("income_group_metadata_lineage", columns)
                        self.assertIn("raw.json", rows[0][columns.index("contributing_observation_lineage")])
                        self.assertIn("country.json", rows[0][columns.index("income_group_metadata_lineage")])
                    elif query_path.name == "gdp_consistency.sql":
                        self.assertIn("population_source_response_sha256", columns)
                        self.assertIn("country_metadata_source_response_sha256", columns)
            connection.close()

    def test_initialize_migrates_legacy_main_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "legacy.duckdb"
            legacy = duckdb.connect(str(database_path))
            legacy.execute("CREATE SCHEMA prl")
            legacy.execute("CREATE TABLE pipeline_runs (run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, status VARCHAR NOT NULL, error_message VARCHAR)")
            legacy.execute("INSERT INTO pipeline_runs VALUES ('legacy-run', current_timestamp, NULL, 'succeeded', NULL)")
            legacy.execute("CREATE TABLE countries (country_code VARCHAR PRIMARY KEY, country_name VARCHAR NOT NULL, region_id VARCHAR, region_name VARCHAR, income_group_id VARCHAR, income_group_name VARCHAR, lending_type_id VARCHAR, lending_type_name VARCHAR, capital_city VARCHAR, longitude DOUBLE, latitude DOUBLE, source_run_id VARCHAR NOT NULL, source_path VARCHAR NOT NULL, source_page INTEGER NOT NULL, source_record_index INTEGER NOT NULL, source_response_sha256 VARCHAR NOT NULL, source_url VARCHAR NOT NULL)")
            legacy.execute("INSERT INTO countries VALUES ('BRA', 'Brazil', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'legacy-run', 'raw.json', 1, 0, 'hash', 'url')")
            legacy.execute("CREATE TABLE indicators (indicator_code VARCHAR PRIMARY KEY, indicator_name VARCHAR NOT NULL)")
            legacy.execute("INSERT INTO indicators VALUES ('TEST', 'Test indicator')")
            legacy.execute("CREATE TABLE observations (country_code VARCHAR NOT NULL, indicator_code VARCHAR NOT NULL, year INTEGER NOT NULL, value DOUBLE, observation_status VARCHAR NOT NULL, source_run_id VARCHAR NOT NULL, source_path VARCHAR NOT NULL, source_page INTEGER NOT NULL, source_record_index INTEGER, source_response_sha256 VARCHAR NOT NULL, source_url VARCHAR NOT NULL, fetched_at TIMESTAMP NOT NULL, PRIMARY KEY (country_code, indicator_code, year))")
            legacy.execute("INSERT INTO observations VALUES ('BRA', 'TEST', 2020, 12.5, 'complete', 'legacy-run', 'raw.json', 1, 0, 'hash', 'url', current_timestamp)")
            legacy.execute("CREATE VIEW prl.legacy_report AS SELECT * FROM main.observations")
            legacy.close()

            migrated = initialize_database(str(database_path))
            self.assertEqual(migrated.execute("SELECT run_id FROM sys.pipeline_runs").fetchone()[0], "legacy-run")
            self.assertEqual(migrated.execute("SELECT country_name FROM mod.dim_country WHERE country_code = 'BRA'").fetchone()[0], "Brazil")
            self.assertEqual(migrated.execute("SELECT indicator_name FROM mod.dim_indicator WHERE indicator_code = 'TEST'").fetchone()[0], "Test indicator")
            fact = migrated.execute("SELECT value, year FROM mod.fact_indicator_observation WHERE country_code = 'BRA' AND indicator_code = 'TEST'").fetchone()
            self.assertEqual(fact, (12.5, "2020"))
            self.assertEqual(migrated.execute("SELECT decade FROM mod.dim_year WHERE year = '2020'").fetchone()[0], "2020s")
            self.assertEqual(migrated.execute("SELECT typeof(year) FROM mod.fact_indicator_observation LIMIT 1").fetchone()[0], "VARCHAR")
            self.assertEqual(migrated.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name IN ('pipeline_runs', 'countries')").fetchone()[0], 0)
            self.assertEqual(migrated.execute("SELECT COUNT(*) FROM sys.pipeline_runs").fetchone()[0], 1)
            self.assertEqual(migrated.execute("SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'prl' AND table_name = 'legacy_report'").fetchone()[0], 0)
            migrated.close()

    def test_initialize_migrates_numeric_years_in_all_model_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "numeric_years.duckdb"
            legacy = duckdb.connect(str(database_path))
            legacy.execute("CREATE SCHEMA ing")
            legacy.execute("CREATE SCHEMA mod")
            legacy.execute("CREATE SCHEMA stg")
            legacy.execute("CREATE SCHEMA prl")
            legacy.execute("""CREATE TABLE mod.dim_country (
                country_code VARCHAR PRIMARY KEY, country_name VARCHAR NOT NULL, region_id VARCHAR,
                region_name VARCHAR, income_group_id VARCHAR, income_group_name VARCHAR,
                lending_type_id VARCHAR, lending_type_name VARCHAR, capital_city VARCHAR,
                longitude DOUBLE, latitude DOUBLE, source_run_id VARCHAR NOT NULL,
                source_path VARCHAR NOT NULL, source_page INTEGER NOT NULL, source_record_index INTEGER NOT NULL,
                source_response_sha256 VARCHAR NOT NULL, source_url VARCHAR NOT NULL)""")
            legacy.execute("CREATE TABLE mod.dim_indicator (indicator_code VARCHAR PRIMARY KEY, indicator_name VARCHAR NOT NULL)")
            legacy.execute("INSERT INTO mod.dim_country VALUES ('BRA', 'Brazil', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'run-1', 'country.json', 1, 0, 'country-hash', 'url')")
            legacy.execute("INSERT INTO mod.dim_indicator VALUES ('TEST', 'Test indicator')")
            legacy.execute("CREATE TABLE ing.run_observation_scope (run_id VARCHAR, country_code VARCHAR, indicator_code VARCHAR, year INTEGER, PRIMARY KEY (run_id, country_code, indicator_code, year))")
            legacy.execute("INSERT INTO ing.run_observation_scope VALUES ('run-1', 'BRA', 'TEST', 2024)")
            legacy.execute("CREATE TABLE mod.dim_year (year INTEGER PRIMARY KEY, decade INTEGER NOT NULL, year_start DATE NOT NULL)")
            legacy.execute("INSERT INTO mod.dim_year VALUES (2024, 2020, DATE '2024-01-01')")
            legacy.execute("CREATE TABLE mod.fact_indicator_observation (country_code VARCHAR NOT NULL, indicator_code VARCHAR NOT NULL, year INTEGER NOT NULL, value DOUBLE, observation_status VARCHAR NOT NULL, source_run_id VARCHAR NOT NULL, source_path VARCHAR NOT NULL, source_page INTEGER NOT NULL, source_record_index INTEGER, source_response_sha256 VARCHAR NOT NULL, source_url VARCHAR NOT NULL, fetched_at TIMESTAMP NOT NULL, PRIMARY KEY (country_code, indicator_code, year))")
            legacy.execute("INSERT INTO mod.fact_indicator_observation VALUES ('BRA', 'TEST', 2024, 10.0, 'complete', 'run-1', 'raw.json', 1, 0, 'hash', 'url', current_timestamp)")
            legacy.execute("CREATE VIEW stg.vw_indicator_observation AS SELECT year FROM ing.run_observation_scope")
            legacy.execute("CREATE VIEW prl.year_over_year AS SELECT year FROM mod.fact_indicator_observation")
            legacy.close()

            migrated = initialize_database(str(database_path))
            year_types = migrated.execute("""
                SELECT table_schema, table_name, data_type
                FROM information_schema.columns
                WHERE (table_schema, table_name, column_name) IN (
                    ('ing', 'run_observation_scope', 'year'),
                    ('mod', 'dim_year', 'year'),
                    ('mod', 'fact_indicator_observation', 'year')
                )
                ORDER BY table_schema, table_name
            """).fetchall()
            self.assertEqual(year_types, [
                ("ing", "run_observation_scope", "VARCHAR"),
                ("mod", "dim_year", "VARCHAR"),
                ("mod", "fact_indicator_observation", "VARCHAR"),
            ])
            self.assertEqual(migrated.execute("SELECT year FROM ing.run_observation_scope").fetchone()[0], "2024")
            self.assertEqual(migrated.execute("SELECT year, decade, year_start FROM mod.dim_year").fetchone(), ("2024", "2020s", datetime(2024, 1, 1).date()))
            self.assertEqual(migrated.execute("SELECT year, value FROM mod.fact_indicator_observation").fetchone(), ("2024", 10.0))
            fact_fks = migrated.execute(
                """SELECT constraint_column_names, referenced_table, referenced_column_names
                   FROM duckdb_constraints()
                   WHERE schema_name = 'mod' AND table_name = 'fact_indicator_observation'
                     AND constraint_type = 'FOREIGN KEY'"""
            ).fetchall()
            self.assertEqual(len(fact_fks), 3)
            self.assertEqual(
                {(tuple(source), target, tuple(referenced)) for source, target, referenced in fact_fks},
                {
                    (("country_code",), "dim_country", ("country_code",)),
                    (("indicator_code",), "dim_indicator", ("indicator_code",)),
                    (("year",), "dim_year", ("year",)),
                },
            )
            with self.assertRaises(duckdb.ConstraintException):
                migrated.execute("INSERT INTO mod.fact_indicator_observation VALUES ('XXX', 'TEST', '2024', 1, 'complete', 'run-1', 'raw.json', 1, 0, 'hash', 'url', current_timestamp)")
            with self.assertRaises(duckdb.ConstraintException):
                migrated.execute("INSERT INTO mod.fact_indicator_observation VALUES ('BRA', 'UNKNOWN', '2024', 1, 'complete', 'run-1', 'raw.json', 1, 0, 'hash', 'url', current_timestamp)")
            with self.assertRaises(duckdb.ConstraintException):
                migrated.execute("INSERT INTO mod.fact_indicator_observation VALUES ('BRA', 'TEST', '1999', 1, 'complete', 'run-1', 'raw.json', 1, 0, 'hash', 'url', current_timestamp)")
            migrated.close()


if __name__ == "__main__":
    unittest.main()