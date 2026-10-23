from __future__ import annotations

import argparse
import logging

from .config import load_config
from .pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Load World Bank indicators into local DuckDB.")
    parser.add_argument("--config", default="config.toml", help="TOML configuration path")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_id = run_pipeline(load_config(args.config))
    print(f"Pipeline completed successfully. run_id={run_id}")


if __name__ == "__main__":
    main()