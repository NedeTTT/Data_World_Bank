from __future__ import annotations

import html
import sys
import time
import unittest
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REPORTS_DIR = ROOT / "reports"

TEST_GUIDANCE = {
    "test_run_ids_are_sequential_integers": (
        "Confirmă că DuckDB generează ID-uri de rulare consecutive.",
        "SELECT nextval('sys.pipeline_run_id_seq') -> 1, 2, 3",
    ),
    "test_get_pages_follows_api_pagination": (
        "Verifică dacă clientul cere toate paginile indicate de API.",
        "page_number rezultat = [1, 2]; URL-urile conțin page=1 și page=2",
    ),
    "test_retries_transient_http_failure": (
        "Un răspuns HTTP 503 temporar trebuie reîncercat după pauza configurată.",
        "Apeluri HTTP = 2; întârzieri = [0.25 secunde]",
    ),
    "test_retries_http_408_timeout": (
        "Un răspuns HTTP 408 trebuie reîncercat, nu tratat ca eroare permanentă.",
        "Apeluri HTTP = 2 după primul 408",
    ),
    "test_indicator_names_prefer_world_bank_response": (
        "Numele indicatorului din răspunsul World Bank are prioritate față de eticheta locală.",
        "_indicator_names_from_raw(...) -> 'Population, total'",
    ),
    "test_indicator_name_uses_config_fallback_if_payload_omits_it": (
        "Folosește eticheta din config când răspunsul API nu conține numele indicatorului.",
        "_indicator_names_from_raw(...) -> 'Configured label'",
    ),
    "test_does_not_retry_permanent_http_failure": (
        "Un răspuns HTTP 400 permanent oprește cererea fără retry.",
        "WorldBankApiError; număr apeluri HTTP = 1",
    ),
    "test_startup_restores_fact_lineage_without_rewriting_dimension_audit": (
        "La redeschiderea bazei, reface sursa originală a factului fără să schimbe auditul dimensiunii.",
        "SELECT source_run_id, source_path FROM mod.fact_indicator_observation -> prima sursă cu hash identic",
    ),
    "test_dimensions_and_facts_change_only_when_source_values_change": (
        "Verifică auditul comun al dimensiunilor și actualizările doar când se schimbă atributele sau valorile.",
        "dim_*: run_id BIGINT + load_start_date TIMESTAMP; membrii neschimbați păstrează run_id",
    ),
    "test_quality_check_rejects_complete_status_with_null_value": (
        "Blochează o observație declarată complete dacă valoarea lipsește.",
        "observation_status = 'complete' AND value IS NULL -> ValueError",
    ),
    "test_quality_check_warns_on_related_indicator_mismatch": (
        "Semnalează pentru analiză diferența mare dintre PIB și PIB/locuitor × populație.",
        "ABS(GDP - GDP_per_capita * population) / ABS(GDP) > 0.10 -> warning",
    ),
    "test_missing_report_includes_null_value_with_complete_status": (
        "Raportul de lipsuri include valorile NULL chiar dacă statusul spune complete și marchează contradicția.",
        "prl.missing_or_incomplete: complete + NULL -> complete_status_with_null_value",
    ),
    "test_staging_views_show_latest_valid_run_only": (
        "Staging alege ultima rulare validă și ignoră rulările eșuate ulterioare.",
        "stg.vw_current_run -> MAX(load_start_date, run_id) WHERE status IN ('running', 'succeeded')",
    ),
    "test_raw_to_staging_to_analytics_is_idempotent": (
        "Reprocesează aceleași date fără duplicate și păstrează anii lipsă/incompleți explicit.",
        "COUNT(*) facts = 3; anii fără valoare primesc status missing sau incomplete",
    ),
    "test_all_reporting_queries_execute": (
        "Rulează toate cele cinci rapoarte SQL și verifică tipurile, lineage-ul și calculele YoY.",
        "sql/*.sql -> SELECT * FROM prl.<raport>; year VARCHAR, year_start DATE",
    ),
    "test_initialize_migrates_legacy_main_tables": (
        "Migrează tabelele vechi din main în schemele curente și păstrează datele.",
        "SELECT country_name, indicator_name, value, year FROM mod.* după initialize_database(...) ",
    ),
    "test_initialize_migrates_numeric_years_in_all_model_tables": (
        "Convertește anii numerici în text și verifică relațiile FK ale factului.",
        "typeof(year) = VARCHAR; fact -> dim_country, dim_indicator, dim_year",
    ),
}


class ReportingTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records: list[dict[str, str | float]] = []
        self._active_records: dict[int, dict[str, str | float]] = {}

    def startTest(self, test):
        super().startTest(test)
        record = {
            "name": test.id(),
            "status": "RUNNING",
            "duration": 0.0,
            "details": "",
        }
        method_name = test.id().rsplit(".", 1)[-1]
        description, key_check = TEST_GUIDANCE.get(
            method_name,
            (test.shortDescription() or "Descriere indisponibilă.", ""),
        )
        record["description"] = description
        record["key_check"] = key_check
        self.records.append(record)
        self._active_records[id(test)] = record
        record["_started"] = time.perf_counter()

    def stopTest(self, test):
        record = self._active_records.pop(id(test), None)
        if record:
            record["duration"] = time.perf_counter() - float(record.pop("_started", time.perf_counter()))
            if record["status"] == "RUNNING":
                record["status"] = "PASS"
        super().stopTest(test)

    def addSuccess(self, test):
        self._active_records[id(test)]["status"] = "PASS"
        super().addSuccess(test)

    def addFailure(self, test, err):
        record = self._active_records[id(test)]
        record["status"] = "FAIL"
        record["details"] = str(err[1])
        super().addFailure(test, err)

    def addError(self, test, err):
        record = self._active_records[id(test)]
        record["status"] = "ERROR"
        record["details"] = str(err[1])
        super().addError(test, err)

    def addSkip(self, test, reason):
        record = self._active_records[id(test)]
        record["status"] = "SKIP"
        record["details"] = reason
        super().addSkip(test, reason)

    def addExpectedFailure(self, test, err):
        self._active_records[id(test)]["status"] = "EXPECTED FAILURE"
        super().addExpectedFailure(test, err)

    def addUnexpectedSuccess(self, test):
        record = self._active_records[id(test)]
        record["status"] = "UNEXPECTED PASS"
        record["details"] = "Test was expected to fail but passed."
        super().addUnexpectedSuccess(test)

    def addSubTest(self, test, subtest, err):
        if err is not None:
            record = self._active_records[id(test)]
            record["status"] = "FAIL"
            record["details"] = str(err[1])
        super().addSubTest(test, subtest, err)


class ReportingTestRunner(unittest.TextTestRunner):
    resultclass = ReportingTestResult


def render_report(result: ReportingTestResult, started_at: datetime, duration: float) -> str:
    counts = {
        "passed": result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped) - len(result.unexpectedSuccesses),
        "failed": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
    }
    success = result.wasSuccessful()
    status = "PASSED" if success else "ATTENTION REQUIRED"
    rows = []
    for record in result.records:
        details = str(record["details"])
        detail_markup = (
            f'<details><summary>Details</summary><pre>{html.escape(details)}</pre></details>'
            if details
            else '<span class="quiet">No details</span>'
        )
        rows.append(
            "<tr data-status=\"{status}\">"
            "<td><span class=\"badge {status_class}\">{status}</span></td>"
            "<td class=\"test-name\">{name}</td>"
            "<td class=\"explanation\">{description}</td>"
            "<td class=\"key-check\"><code>{key_check}</code></td>"
            "<td class=\"duration\">{duration:.3f}s</td>"
            "<td class=\"diagnostics\">{details}</td>"
            "</tr>".format(
                status=html.escape(str(record["status"])),
                status_class=html.escape(str(record["status"]).lower().replace(" ", "-")),
                name=html.escape(str(record["name"])),
                description=html.escape(str(record["description"])),
                key_check=html.escape(str(record["key_check"])),
                duration=float(record["duration"]),
                details=detail_markup,
            )
        )
    table_rows = "\n".join(rows) or '<tr><td colspan="6" class="empty">Nu au fost găsite teste.</td></tr>'
    generated = started_at.astimezone().strftime("%A, %B %d, %Y at %H:%M:%S %Z")
    verdict_class = "success" if success else "failure"
    return f'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Raport teste pipeline | {started_at:%Y-%m-%d %H:%M}</title>
<style>
:root {{ color-scheme: light; --ink: #172321; --muted: #687874; --paper: #f4f6f1; --line: #dce4dc; --green: #087e64; --red: #b33c35; --amber: #a36b13; --shadow: 0 18px 55px #18352c12; }}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--paper); color: var(--ink); font: 15px/1.5 "Segoe UI", "Aptos", sans-serif; }}
header {{ padding: 38px max(22px, calc((100vw - 1160px)/2)); color: #f5fff8; background: #173f35; position: relative; overflow: hidden; }}
header::after {{ content: ""; position: absolute; width: 320px; height: 320px; border: 1px solid #ffffff25; border-radius: 50%; right: max(8vw, calc((100vw - 1160px)/2)); top: -190px; box-shadow: 0 0 0 32px #ffffff08, 0 0 0 72px #ffffff06; pointer-events: none; }}
.kicker {{ margin: 0 0 8px; color: #a7d8c2; font-size: 12px; font-weight: 700; text-transform: uppercase; }}
h1 {{ max-width: 720px; margin: 0; font: 600 clamp(30px, 5vw, 48px)/1.05 Georgia, "Times New Roman", serif; }}
.subtitle {{ margin: 12px 0 0; color: #d3e6dd; }}
main {{ width: min(1160px, calc(100% - 36px)); margin: -20px auto 60px; position: relative; }}
.verdict {{ display: flex; justify-content: space-between; align-items: center; gap: 18px; margin-bottom: 18px; padding: 20px 24px; background: white; border-left: 5px solid var(--green); border-radius: 5px; box-shadow: var(--shadow); }}
.verdict.failure {{ border-left-color: var(--red); }}
.verdict strong {{ display: block; font-size: 21px; }}
.verdict span {{ color: var(--muted); }}
.stamp {{ color: var(--muted); font-size: 13px; text-align: right; }}
.metrics {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 22px; }}
.metric {{ padding: 18px 20px; background: white; border: 1px solid var(--line); border-radius: 5px; box-shadow: var(--shadow); }}
.metric b {{ display: block; font: 600 30px/1 Georgia, serif; }}
.metric span {{ display: block; margin-top: 9px; color: var(--muted); font-size: 13px; }}
.results {{ overflow: hidden; background: white; border: 1px solid var(--line); border-radius: 5px; box-shadow: var(--shadow); }}
.toolbar {{ display: flex; justify-content: space-between; gap: 14px; align-items: center; padding: 18px 20px; border-bottom: 1px solid var(--line); }}
h2 {{ margin: 0; font: 600 23px Georgia, "Times New Roman", serif; }}
.controls {{ display: flex; gap: 8px; }}
input, select {{ min-height: 40px; padding: 0 12px; color: var(--ink); background: #fbfcfa; border: 1px solid var(--line); border-radius: 4px; font: inherit; }}
input {{ width: min(260px, 44vw); }}
.table-wrap {{ overflow-x: auto; }}
table {{ width: 100%; border-collapse: collapse; text-align: left; }}
th {{ padding: 12px 16px; color: var(--muted); background: #f8faf7; font-size: 11px; text-transform: uppercase; }}
td {{ padding: 14px 16px; border-top: 1px solid #edf1ec; vertical-align: top; }}
tr:hover td {{ background: #fbfcfa; }}
.test-name {{ min-width: 260px; overflow-wrap: anywhere; font-family: ui-monospace, Consolas, monospace; font-size: 13px; }}
.explanation {{ min-width: 250px; }}
.key-check {{ min-width: 300px; }}
.key-check code {{ display: block; padding: 8px 10px; background: #f2f6f2; border-radius: 3px; color: #315a4b; white-space: normal; overflow-wrap: anywhere; font-size: 12px; }}
.diagnostics {{ min-width: 120px; }}
.duration {{ white-space: nowrap; color: var(--muted); font-variant-numeric: tabular-nums; }}
.badge {{ display: inline-block; min-width: 78px; padding: 4px 8px; border-radius: 3px; background: #e3f3ed; color: #09694f; font-size: 11px; font-weight: 800; text-align: center; }}
.badge.fail, .badge.error, .badge.unexpected-pass {{ background: #fde9e6; color: #9e2e27; }}
.badge.skip, .badge.expected-failure {{ background: #fff2d7; color: #865809; }}
details summary {{ color: var(--green); cursor: pointer; font-size: 13px; }}
pre {{ max-width: 480px; max-height: 260px; overflow: auto; padding: 12px; background: #f5f7f4; border-radius: 3px; white-space: pre-wrap; font-size: 12px; }}
.quiet, .empty {{ color: var(--muted); }}
.empty {{ padding: 30px; text-align: center; }}
footer {{ padding: 18px 20px; color: var(--muted); border-top: 1px solid var(--line); font-size: 12px; }}
@media (max-width: 680px) {{
 header {{ padding: 30px 22px 42px; }} main {{ width: calc(100% - 24px); }}
 .verdict, .toolbar {{ align-items: flex-start; flex-direction: column; }} .stamp {{ text-align: left; }}
 .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }} .metric {{ padding: 15px; }}
 .controls {{ width: 100%; }} input {{ flex: 1; min-width: 0; }} select {{ max-width: 130px; }}
 th, td {{ padding: 11px 10px; }} .test-name {{ min-width: 220px; }}
}}
</style>
</head>
<body>
<header>
  <p class="kicker">World Bank Indicators Pipeline</p>
    <h1>Raport de teste</h1>
    <p class="subtitle">Rezultatele verificărilor automate pentru această rulare.</p>
</header>
<main>
  <section class="verdict {verdict_class}">
        <div><strong>{'TOATE TESTELE AU TRECUT' if success else 'SUNT TESTE DE VERIFICAT'}</strong><span>{result.testsRun} teste rulate în {duration:.2f} secunde</span></div>
        <div class="stamp">Generat la<br>{html.escape(generated)}</div>
  </section>
    <section class="metrics" aria-label="Sumar teste">
        <div class="metric"><b>{counts['passed']}</b><span>Trecute</span></div>
        <div class="metric"><b>{counts['failed']}</b><span>Eșuate</span></div>
        <div class="metric"><b>{counts['errors']}</b><span>Erori</span></div>
        <div class="metric"><b>{counts['skipped']}</b><span>Sărite</span></div>
  </section>
  <section class="results">
        <div class="toolbar"><h2>Ce verifică fiecare test</h2><div class="controls">
            <input id="search" type="search" placeholder="Caută în teste" aria-label="Caută teste">
            <select id="status-filter" aria-label="Filtrează după rezultat">
                <option value="all">Toate rezultatele</option><option value="PASS">Trecute</option>
                <option value="FAIL">Eșuate</option><option value="ERROR">Erori</option><option value="SKIP">Sărite</option>
      </select>
    </div></div>
        <div class="table-wrap"><table><thead><tr><th>Rezultat</th><th>Nume tehnic</th><th>Ce verifică</th><th>Condiție / query-cheie</th><th>Durată</th><th>Detalii eroare</th></tr></thead>
    <tbody id="test-rows">{table_rows}</tbody></table></div>
        <footer>Rapoartele sunt salvate în <code>reports/</code>. Fișierul cu timestamp păstrează această rulare; <code>test-results.html</code> conține întotdeauna ultimul rezultat.</footer>
  </section>
</main>
<script>
const search = document.querySelector('#search');
const statusFilter = document.querySelector('#status-filter');
function filterRows() {{
  const needle = search.value.toLowerCase();
  for (const row of document.querySelectorAll('#test-rows tr[data-status]')) {{
    const matchesName = row.querySelector('.test-name').textContent.toLowerCase().includes(needle);
    const matchesStatus = statusFilter.value === 'all' || row.dataset.status === statusFilter.value;
    row.hidden = !(matchesName && matchesStatus);
  }}
}}
search.addEventListener('input', filterRows);
statusFilter.addEventListener('change', filterRows);
</script>
</body>
</html>
'''


def main() -> int:
    started_at = datetime.now().astimezone()
    started_clock = time.perf_counter()
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    runner = ReportingTestRunner(stream=sys.stderr, verbosity=2)
    result = runner.run(suite)
    duration = time.perf_counter() - started_clock
    report = render_report(result, started_at, duration)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    run_path = REPORTS_DIR / f"test-results-{started_at:%Y%m%d-%H%M%S-%f}.html"
    latest_path = REPORTS_DIR / "test-results.html"
    run_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(f"HTML test report: {latest_path.relative_to(ROOT)}")
    print(f"Archived test report: {run_path.relative_to(ROOT)}")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
