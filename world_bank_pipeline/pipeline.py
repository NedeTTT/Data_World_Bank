from __future__ import annotations

import csv
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .client import ApiPage, WorldBankClient
from .config import PipelineConfig
from .quality import run_quality_checks
from .store import initialize_database, persist_raw_layer, publish_staging


LOGGER = logging.getLogger(__name__)


def _format_run_id(timestamp: datetime) -> str:
    return timestamp.strftime("%Y_%m_%d_%H:%M:%S")


def _run_folder_name(run_id: str) -> str:
    return run_id.replace(":", "-")


def _new_run_id(connection: duckdb.DuckDBPyConnection) -> str:
    while True:
        run_id = _format_run_id(datetime.now(timezone.utc))
        exists = connection.execute(
            "SELECT COUNT(*) FROM sys.pipeline_runs WHERE run_id = ?", [run_id]
        ).fetchone()[0]
        if not exists:
            return run_id
        time.sleep(0.1)


def _archive_page(
    config: PipelineConfig,
    run_id: str,
    endpoint_type: str,
    country_code: str | None,
    indicator_code: str | None,
    page: ApiPage,
) -> dict:
    digest = hashlib.sha256(page.body).hexdigest()
    request_key = country_code or "all"
    if indicator_code:
        request_key = f"{request_key}_{indicator_code.replace('.', '_')}"
    run_folder = _run_folder_name(run_id)
    relative_path = Path(run_folder) / endpoint_type / request_key / f"page-{page.page_number}-{digest[:12]}.json"
    destination = config.raw_dir / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(page.body)
    metadata = {
        "run_id": run_id,
        "endpoint_type": endpoint_type,
        "country_code": country_code,
        "indicator_code": indicator_code,
        "page_number": page.page_number,
        "request_url": page.request_url,
        "fetched_at": page.fetched_at,
        "response_sha256": digest,
        "raw_path": destination.relative_to(config.root).as_posix() if destination.is_relative_to(config.root) else destination.as_posix(),
    }
    manifest = config.raw_dir / run_folder / "manifest.jsonl"
    with manifest.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(metadata, sort_keys=True) + "\n")
    return metadata


def _fetch_and_archive(config: PipelineConfig, run_id: str, client: WorldBankClient):
    receipts: list[dict] = []
    raw_country_rows: list[tuple] = []
    raw_observation_rows: list[tuple] = []

    for country_code in config.countries:
        pages = client.get_pages(f"country/{country_code}", {"per_page": "100"})
        sources = [
            _archive_page(config, run_id, "countries", country_code, None, page)
            for page in pages
        ]
        receipts.extend(sources)
        for page, source in zip(pages, sources, strict=True):
            for record_index, record in enumerate(page.payload):
                raw_country_rows.append((
                    run_id, country_code, page.page_number, record_index,
                    json.dumps(record, separators=(",", ":"), ensure_ascii=False),
                    source["raw_path"], source["response_sha256"], source["request_url"], source["fetched_at"],
                ))

        for indicator_code in config.indicators:
            endpoint = f"country/{country_code}/indicator/{indicator_code}"
            params = {"date": f"{config.start_year}:{config.end_year}", "per_page": "1000"}
            indicator_pages = client.get_pages(endpoint, params)
            indicator_sources = [
                _archive_page(config, run_id, "indicators", country_code, indicator_code, page)
                for page in indicator_pages
            ]
            receipts.extend(indicator_sources)
            for page, source in zip(indicator_pages, indicator_sources, strict=True):
                for record_index, record in enumerate(page.payload):
                    raw_observation_rows.append((
                        run_id, country_code, indicator_code, page.page_number, record_index,
                        json.dumps(record, separators=(",", ":"), ensure_ascii=False),
                        source["raw_path"], source["response_sha256"], source["request_url"], source["fetched_at"],
                    ))
    return receipts, raw_country_rows, raw_observation_rows


def _export_reports(connection: duckdb.DuckDBPyConnection, reports_dir: Path, sql_dir: Path) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    for query_path in sorted(sql_dir.glob("*.sql")):
        view_name = query_path.stem
        connection.execute(
            f"CREATE OR REPLACE VIEW prl.{view_name} AS {query_path.read_text(encoding='utf-8')}"
        )
        cursor = connection.execute(f"SELECT * FROM prl.{view_name}")
        output_path = reports_dir / f"{query_path.stem}.csv"
        with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([column[0] for column in cursor.description])
            writer.writerows(cursor.fetchall())


def run_pipeline(config: PipelineConfig) -> str:
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    config.database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = initialize_database(str(config.database_path))
    run_id = _new_run_id(connection)
    connection.execute(
        "INSERT INTO sys.pipeline_runs (run_id, started_at, status) VALUES (?, ?, 'running')",
        [run_id, started_at],
    )
    try:
        client = WorldBankClient(
            config.base_url, config.timeout_seconds, config.max_retries,
            config.retry_backoff_seconds,
        )
        receipts, raw_country_rows, raw_observation_rows = _fetch_and_archive(config, run_id, client)
        indicator_names = _indicator_names_from_raw(raw_observation_rows, config.indicators)
        raw_rows = [(
            receipt["run_id"], receipt["endpoint_type"], receipt["country_code"],
            receipt["indicator_code"], receipt["page_number"], receipt["request_url"],
            receipt["fetched_at"], receipt["response_sha256"], receipt["raw_path"],
        ) for receipt in receipts]
        observation_scope = [
            (run_id, country_code, indicator_code, str(year))
            for country_code in config.countries
            for indicator_code in config.indicators
            for year in range(config.start_year, config.end_year + 1)
        ]
        persist_raw_layer(connection, raw_rows, raw_country_rows, raw_observation_rows, observation_scope)
        publish_staging(
            connection, run_id, indicator_names,
            validator=lambda database: run_quality_checks(database, config),
        )
        sql_dir = config.root / "sql"
        _export_reports(connection, config.reports_dir, sql_dir)
        connection.execute(
            "UPDATE sys.pipeline_runs SET status = 'succeeded', finished_at = current_timestamp WHERE run_id = ?",
            [run_id],
        )
        LOGGER.info("Run %s succeeded: %s raw pages, %s staged observations", run_id, len(receipts), len(observation_scope))
        return run_id
    except Exception as error:
        connection.execute(
            "UPDATE sys.pipeline_runs SET status = 'failed', finished_at = current_timestamp, error_message = ? WHERE run_id = ?",
            [str(error), run_id],
        )
        raise
    finally:
        connection.close()


def _indicator_names_from_raw(raw_observation_rows: list[tuple], configured: dict[str, str]) -> dict[str, str]:
    names: dict[str, str] = {}
    for raw_row in raw_observation_rows:
        indicator_code = raw_row[2]
        payload = json.loads(raw_row[5])
        indicator = payload.get("indicator") or {}
        api_name = indicator.get("value")
        if api_name:
            names[indicator_code] = str(api_name)
    return {code: names.get(code, label) for code, label in configured.items()}