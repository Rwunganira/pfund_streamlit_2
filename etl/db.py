"""
etl/db.py
=========
Database connection helpers for the ETL pipeline.

  get_source_engine()    → Heroku Postgres (operational source, DATABASE_URL)
  get_warehouse_engine() → local warehouse (WAREHOUSE_URL)
"""

from __future__ import annotations
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from sqlalchemy import create_engine, Engine


def _make_engine(url: str, label: str) -> Engine:
    if not url:
        raise EnvironmentError(
            f"{label} environment variable is not set. "
            "Add it to your .env file."
        )
    # Heroku uses postgres:// — SQLAlchemy 1.4+ requires postgresql://
    url = url.replace("postgres://", "postgresql://", 1)
    return create_engine(url, pool_pre_ping=True)


def get_source_engine() -> Engine:
    """Operational source database (Heroku Postgres)."""
    return _make_engine(os.getenv("DATABASE_URL", ""), "DATABASE_URL")


def get_warehouse_engine() -> Engine:
    """Local data warehouse database."""
    return _make_engine(os.getenv("WAREHOUSE_URL", ""), "WAREHOUSE_URL")


# Keep backward-compat alias used by older code
get_engine = get_warehouse_engine
