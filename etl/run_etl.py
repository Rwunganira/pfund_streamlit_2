"""
etl/run_etl.py
==============
ETL pipeline orchestrator.

Usage:
    python -m etl.run_etl              # full pipeline
    python -m etl.run_etl --marts-only # rebuild marts only (fast refresh)

Schedule via cron or Windows Task Scheduler to keep mart_ tables fresh.
"""

from __future__ import annotations
import argparse
import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run_full_pipeline() -> None:
    from etl.extract               import run_extract
    from etl.transform.dimensions  import run_dimensions
    from etl.transform.facts       import run_facts
    from etl.transform.marts       import run_marts

    steps = [
        ("Extract (operational → stg)",  run_extract),
        ("Dimensions (stg → dwh)",       run_dimensions),
        ("Facts (stg → dwh)",            run_facts),
        ("Marts (dwh → mart)",           run_marts),
    ]
    t0 = time.time()
    for name, fn in steps:
        log.info(f"=== {name} ===")
        fn()
    log.info(f"Pipeline complete in {time.time() - t0:.1f}s")


def run_marts_only() -> None:
    from etl.transform.marts import run_marts
    t0 = time.time()
    log.info("=== Marts refresh only ===")
    run_marts()
    log.info(f"Marts refresh complete in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pandemic Fund ETL pipeline")
    parser.add_argument(
        "--marts-only",
        action="store_true",
        help="Rebuild mart tables from existing fact data without re-extracting",
    )
    args = parser.parse_args()

    try:
        if args.marts_only:
            run_marts_only()
        else:
            run_full_pipeline()
    except Exception as exc:
        log.exception(f"Pipeline failed: {exc}")
        sys.exit(1)
