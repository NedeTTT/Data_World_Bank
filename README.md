# World Bank Indicators Pipeline

Local pipeline: World Bank API → immutable raw files → DuckDB analytics and CSV reports.

## Run locally

Requirements: Windows PowerShell, Python 3.11+, and internet access for API retrieval. No API key or Docker is needed.

### 1. Download and run

Clone the repository (replace the URL), then run the pipeline:

```powershell
git clone <repository-url> Data_World_Bank
cd Data_World_Bank
```

If already downloaded, open PowerShell in the folder containing `run.ps1`. Review [config.toml](config.toml) to change countries, indicators, years, or output paths. Defaults:

| Output | Default location |
| --- | --- |
| DuckDB database | `data/world_bank.duckdb` |
| Archived API responses | `data/raw/<run_id>/` |
| CSV and HTML reports | `reports/` |

```powershell
.\run.ps1
```

The first run creates `.venv`, installs dependencies, fetches data, creates the database, exports reports, then runs offline tests. If the pipeline fails, tests are skipped. If PowerShell blocks the script, allow it for this terminal only:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\run.ps1
```

To use another TOML config:

```powershell
.\run.ps1 --config another-config.toml
```

### 2. Review results

Generated files:

- `reports/*.csv`: analytical reports.
- [reports/ingestion_quality.csv](reports/ingestion_quality.csv): per-run table counts and NULL counts.
- [reports/test-results.html](reports/test-results.html): latest test result; timestamped copies are retained.
- `data/raw/<run_id>/`: raw API pages and manifest.

### 3. Query with DBeaver

In DBeaver choose **Database > New Database Connection > DuckDB**, then select `<project-folder>\data\world_bank.duckdb`. Expand `sys`, `ing`, `stg`, `mod`, and `prl`, or run:

```sql
SELECT run_id, load_start_date, status, finished_at
FROM sys.pipeline_runs
ORDER BY run_id DESC;

SELECT country_code, indicator_code, year, value, observation_status
FROM mod.fact_indicator_observation
LIMIT 25;

SELECT run_id, table_name, row_count, sql_null_count, sql_null_counts, json_value_null_count
FROM sys.ingestion_quality_metrics
ORDER BY run_id DESC, table_name;
```

Close DBeaver's connection before rerunning the pipeline; an open connection can lock the database.

### 4. Query from PowerShell instead

No GUI is required. Start the project's Python interpreter:

```powershell
.\.venv\Scripts\python.exe
```

Then run SQL in the Python prompt:

```python
import duckdb
connection = duckdb.connect("data/world_bank.duckdb", read_only=True)
connection.sql("SELECT * FROM mod.fact_indicator_observation LIMIT 10").show()
connection.sql("SELECT * FROM sys.ingestion_quality_metrics ORDER BY run_id DESC, table_name").show()
connection.close()
```

Exit the prompt with `exit()`.

Run tests separately with `.\.venv\Scripts\python.exe run_tests.py`; they use local fixtures and do not call the API.

## Configuration and model

Edit [config.toml](config.toml) to change countries, indicators, year range, retry behavior, or output paths. API names are authoritative; configured indicator labels are fallbacks. Current indicators:

| Code | Indicator |
| --- | --- |
| `SP.POP.TOTL` | Population, total |
| `NY.GDP.MKTP.CD` | GDP, current US dollars |
| `NY.GDP.PCAP.CD` | GDP per capita, current US dollars |
| `SP.DYN.LE00.IN` | Life expectancy at birth, total years |

Add countries using World Bank ISO3 codes and indicators using World Bank indicator codes. The configured country × indicator × year combinations define expected coverage.

Proj structure and model
├── sys/
│   ├── pipeline_runs
│   └── ingestion_quality_metrics
├── ing/
│   ├── raw_responses
│   ├── raw_country_metadata
│   ├── raw_indicator_observations
│   └── run_observation_scope
├── stg/
│   ├── vw_current_run
│   ├── vw_country
│   ├── vw_country_source
│   ├── vw_indicator_observation
│   └── vw_observation
├── mod/
│   ├── fact_indicator_observation
│   ├── dim_country
│   ├── dim_indicator
│   └── dim_year
└── prl/
    ├── missing_or_incomplete
    ├── year_over_year
    ├── latest_comparison
    ├── income_group_comparison
    └── gdp_consistency
## Implementation summary

- `ing`: immutable API pages, hashes, and expected observation scope.
- `sys`: sequential run IDs and run history; ingestion counts/NULL profiles are exported to `reports/ingestion_quality.csv`.
- `stg`: typed, deduplicated views and explicit missing/incomplete status.
- `mod`: star schema / 3 dimensions and 1 fact.
- `prl`: five SQL reporting views exported as CSV.
- Quality/tests: transactional model checks and 18 offline tests; `run.ps1` runs tests only after a successful pipeline.

### Grain, keys, and relationships

The final analytical grain is exactly one observation for a country, indicator, and year. The fact business key and primary key is `(country_code, indicator_code, year)`; these are natural keys taken from the World Bank ISO3 code, indicator code, and year. `mod.dim_country.country_code`, `mod.dim_indicator.indicator_code`, and `mod.dim_year.year` are the respective dimension primary keys. The fact has three enforced foreign keys:

```text
mod.dim_country.country_code       1 ─── * mod.fact_indicator_observation.country_code
mod.dim_indicator.indicator_code  1 ─── * mod.fact_indicator_observation.indicator_code
mod.dim_year.year                  1 ─── * mod.fact_indicator_observation.year
```

This is a **star schema**: the central fact table stores measurable observations, while descriptive dimension tables connect directly to it. Here, `mod.fact_indicator_observation` is the center, holding one row per country, indicator, and year with the numeric value and observation status. `dim_country`, `dim_indicator`, and `dim_year` are the surrounding dimensions, providing country details, indicator names, and calendar attributes. Each fact row joins to one member in each dimension through its country, indicator, and year keys; those direct relationships form the star shape and make reporting queries straightforward. Year keys are `VARCHAR` (avoiding thousands separators); `year_start` is a `DATE`. Income group reflects current World Bank metadata, not historical classifications.


Raw API pages are archived unchanged and indexed in `ing`; staging views parse, type, and deduplicate records, then join them to configured scope so omitted years are explicit. The model keeps observation lineage on facts and audit columns on dimensions.

Staging selects the latest valid run and resolves duplicates deterministically; failed runs do not replace valid data. Loads reconcile facts to configured scope in a transaction and update rows only when values or status change. Dimension audit IDs advance only when attributes change. Raw data remains available for replay; staging is computed from raw on query.

Observation status is `complete` (value present), `incomplete` (API record with null value), or `missing` (no API record). Missing values remain SQL `NULL`, never zero.

## Lineage and outputs

Raw files and `manifest.jsonl` retain request URLs, timestamps, page numbers, and hashes. Facts and reports expose observation lineage; country metadata lineage is joined from raw data using the dimension audit run.

DuckDB is stored at `data/world_bank.duckdb`. SQL definitions in `sql/` create persistent views in `prl` and produce:

| Report | Contents |
| --- | --- |
| `missing_or_incomplete.csv` | Missing/incomplete values and quality flags |
| `year_over_year.csv` | Prior value and annual changes |
| `latest_comparison.csv` | Latest available values by indicator and country |
| `income_group_comparison.csv` | Annual statistics by current income group |
| `gdp_consistency.csv` | GDP cross-check and review flag |

YoY changes are null when values are unavailable; percent change is also null when the prior value is zero. GDP differences above 10% are review warnings, not load failures.

All reports are also queryable as views in `prl`.

## Data quality and tests

Automated checks run inside the staging-to-model transaction before it commits:

- The fact row count matches the configured country × indicator × year scope.
- The fact business key `(country_code, indicator_code, year)` is unique; the database primary key enforces this as well.
- `complete` observations have a non-null value, and `missing`/`incomplete` observations do not have a value.
- Every fact resolves to country, indicator, and year dimensions; database foreign keys independently enforce these relationships.
- Each fact's raw source file exists on disk.
- GDP is checked against GDP per capita × population for each country/year. A relative difference above 10% emits a review warning; it does not fail the load because source definitions, revisions, and rounding can differ.

The offline `unittest` suite does not call the live API. It covers API pagination with fixtures, retries for HTTP 408/503 and no retry for permanent 400, indicator-name extraction/fallback, raw-to-staging-to-model idempotency, deduplication, latest-valid-run selection, missing/incomplete semantics, rejection of `complete + NULL`, the GDP consistency warning, ingestion row/NULL metrics, year-type migration, primary/foreign-key enforcement, lineage in reports, and execution/layout of all presentation views.

For a plain-language explanation of each runtime check and each offline test, see [DATA_QUALITY_AND_TESTS.md](DATA_QUALITY_AND_TESTS.md).


## Decisions and trade-offs

- Python orchestrates ingestion and exports; DuckDB provides local analytical storage without a server.
- DBT Core was considered at the user's request, but not selected because the project is small and time is limited. Python orchestration plus SQL views meets the current scope with less setup.
- Each run refreshes the configured historical range to capture source corrections. Raw responses are retained; failed runs do not publish partial analytical data.

## Timebox and remaining work

Potential production follow-ups:

- Historical income-group classifications.
- Live-source drift monitoring and alerts.
- Concurrent-run coordination and atomic report publication.
- Raw-data retention and history for corrected facts.

## Production design note

For production, schedule refreshes with Airflow, Dagster, or a cloud scheduler; retain immutable raw snapshots and corrected-value history; monitor run status, counts, missingness, and schema drift; and coordinate concurrent runs. Object storage plus a cloud warehouse can replace local files and DuckDB as scale grows.

## AI usage

GitHub Copilot coding agent assisted with implementation, debugging, tests, and documentation. The Explore subagent was used for read-only repository research. Changes and assumptions were reviewed and validated with offline tests; live API data may still contain revisions or metadata changes.
