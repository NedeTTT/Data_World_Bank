$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot
$venvPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
$requirements = Join-Path $projectRoot 'requirements.txt'
$config = Join-Path $projectRoot 'config.toml'

Push-Location $projectRoot
try {
    if (-not (Test-Path $venvPython)) {
        if (Get-Command py -ErrorAction SilentlyContinue) {
            & py -3 -m venv .venv
        } elseif (Get-Command python -ErrorAction SilentlyContinue) {
            & python -m venv .venv
        } else {
            throw 'Python 3.11+ is required. Install Python and retry.'
        }
        if ($LASTEXITCODE -ne 0) {
            throw 'Could not create the project virtual environment.'
        }
    }

    & $venvPython -c "import sys; raise SystemExit(sys.version_info < (3, 11))"
    if ($LASTEXITCODE -ne 0) {
        throw 'Python 3.11 or newer is required.'
    }

    & $venvPython -m pip install -r $requirements
    if ($LASTEXITCODE -ne 0) {
        throw 'Dependency installation failed.'
    }

    & $venvPython -m world_bank_pipeline --config $config @args
    $pipelineExitCode = $LASTEXITCODE
    if ($pipelineExitCode -ne 0) {
        throw "World Bank pipeline failed with exit code $pipelineExitCode. Offline tests were not run."
    }

    & $venvPython (Join-Path $projectRoot 'run_tests.py')
    $testExitCode = $LASTEXITCODE

    if ($testExitCode -ne 0) {
        throw "Offline tests failed with exit code $testExitCode. See reports/test-results.html."
    }
} finally {
    Pop-Location
}