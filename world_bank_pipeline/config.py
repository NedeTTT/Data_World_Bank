from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PipelineConfig:
    root: Path
    base_url: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    start_year: int
    end_year: int
    raw_dir: Path
    database_path: Path
    reports_dir: Path
    countries: tuple[str, ...]
    indicators: dict[str, str]


def load_config(path: str | Path) -> PipelineConfig:
    config_path = Path(path).resolve()
    raw: dict[str, Any] = tomllib.loads(config_path.read_text(encoding="utf-8"))
    root = config_path.parent
    source = raw["source"]
    pipeline = raw["pipeline"]
    start_year = int(pipeline["start_year"])
    end_year = int(pipeline["end_year"])
    countries = tuple(str(country).upper() for country in pipeline["countries"])
    indicators = {str(code): str(label) for code, label in raw["indicators"].items()}

    if start_year > end_year:
        raise ValueError("start_year must be less than or equal to end_year")
    if not countries or len(set(countries)) != len(countries):
        raise ValueError("countries must be a non-empty list without duplicates")
    if not indicators:
        raise ValueError("at least one indicator must be configured")

    def rooted(value: str) -> Path:
        candidate = Path(value)
        return candidate if candidate.is_absolute() else root / candidate

    return PipelineConfig(
        root=root,
        base_url=str(source["base_url"]).rstrip("/"),
        timeout_seconds=int(source.get("timeout_seconds", 30)),
        max_retries=int(source.get("max_retries", 4)),
        retry_backoff_seconds=float(source.get("retry_backoff_seconds", 1.0)),
        start_year=start_year,
        end_year=end_year,
        raw_dir=rooted(str(pipeline["raw_dir"])),
        database_path=rooted(str(pipeline["database_path"])),
        reports_dir=rooted(str(pipeline["reports_dir"])),
        countries=countries,
        indicators=indicators,
    )