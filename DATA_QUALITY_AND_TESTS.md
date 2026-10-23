# Data Quality Checks and Tests

This note explains, in non-technical terms, what the pipeline checks during a normal run and what the separate offline test suite verifies.

## Checks performed on every pipeline run

These checks run after the data has been prepared for the analytics model and before the model update is committed. A failed blocking check stops publication of the new analytical batch. The raw API responses are retained for diagnosis.

1. **Expected coverage**  
   The pipeline calculates how many country × indicator × year combinations should exist from `config.toml`, then compares that with the fact table row count. This catches a partially built analytical set. Missing API values still count as rows because they are represented explicitly as `missing` or `incomplete`.

2. **No duplicate analytical keys**  
   Each country, indicator, and year must identify only one fact row. The database primary key enforces this, and the quality check also looks for repeated combinations.

3. **Value agrees with observation status**  
   A row marked `complete` must have a value. Rows marked `missing` or `incomplete` must not have one. A contradiction blocks the model update.

4. **Every fact has its dimensions**  
   Every fact must refer to a country, indicator, and year that exist in the matching dimension tables. The check looks for unresolved references; database foreign keys enforce the same relationships when rows are written.

   All three dimensions also carry the same audit fields, `run_id` and `load_start_date`. These identify the run that created or last changed a member (or its first insertion for `dim_year`). The run ID matches `sys.pipeline_runs`; DuckDB cannot enforce this relationship as a foreign key across schemas.

5. **Source file still exists**  
   For every fact row, the raw JSON file recorded as its source must be present on disk. This helps ensure the value can be traced back to the API response.

6. **GDP-related indicator consistency**  
   Where GDP, GDP per capita, and population are all available for a country and year, the pipeline compares GDP with GDP per capita × population. A relative difference above 10% is logged as a review warning. It does not block the load because World Bank series can differ due to definitions, revisions, and rounding.

## Offline tests

Run the offline suite and generate/update the HTML report with:

```powershell
python run_tests.py
```

The tests use simulated API responses and temporary or in-memory DuckDB databases. They do not call the live World Bank API. `run.ps1` runs them after each pipeline attempt and writes the latest results to `reports/test-results.html`, plus a timestamped archive for each execution. Each report row explains the test in plain Romanian and shows its key assertion or representative SQL condition; technical failure details are expandable.

1. **Sequential run IDs**  
   Confirms a new local database assigns run IDs in order (`1`, `2`, `3`, ...), which are also used to group each run's raw files and lineage.

2. **API pagination**  
   Simulates a response split over two pages and confirms both pages are requested.

3. **Retry temporary server failure**  
   Simulates HTTP 503 and confirms the client waits and retries.

4. **Retry request timeout**  
   Simulates HTTP 408 and confirms it is treated as temporary.

5. **Indicator name from API**  
   Confirms the API-provided indicator name takes precedence over the configured fallback label.

6. **Indicator name fallback**  
   Confirms a configured label is used if the API payload has no indicator name.

7. **Do not retry permanent client error**  
   Simulates HTTP 400 and confirms the request fails immediately instead of repeating an invalid request.

8. **Reject complete status with no value**  
   Inserts a deliberately contradictory row and confirms the data-quality check blocks it.

9. **Warn on inconsistent related indicators**  
   Supplies GDP, population, and GDP per capita values that disagree by more than the threshold and confirms a review warning is emitted.

10. **Report NULL values even when status is wrong**  
    Confirms the missing/incomplete report still shows a row whose status says `complete` but whose value is NULL, and flags the contradiction.

11. **Show only the latest valid snapshot**  
    Provides multiple successful runs plus a later failed run and confirms staging exposes the latest successful data, not duplicates or failed-run data.

12. **Safe reprocessing from raw through analytics**  
    Loads the same raw fixture twice and confirms the fact table does not gain duplicate analytical rows. It also checks duplicate raw records are resolved in staging and missing years are represented explicitly.

13. **Presentation SQL and lineage**  
    Executes each report query against a local sample model and checks year/date types, adjacent current/prior values in the YoY report, and source lineage fields.

14. **Migrate legacy tables**  
    Starts from an older table layout and confirms existing country, indicator, observation, and run data are carried into the current schemas.

15. **Migrate numeric year columns and enforce relationships**  
    Starts with integer year keys, confirms values survive conversion to text while calendar dates stay dates, checks fact foreign keys, and verifies invalid country/indicator/year references are rejected.

16. **Standard dimension audit columns**
   Confirms country, indicator, and year dimensions have the same `run_id` and `load_start_date` types, and that the audit identity advances only when a dimension attribute changes.

## What a warning means

A GDP consistency warning is a signal for analyst review, not proof that a source value is wrong. The source values and their lineage remain available. Blocking checks are reserved for structural or integrity failures that would make the analytical dataset unsafe to publish.
