"""
etl/transform/dimensions.py
============================
Upserts dimension tables from staging data.
"""

from __future__ import annotations
import logging
import pandas as pd
from sqlalchemy import text
from etl.db     import get_engine
from etl.config import map_strategic_area, ALL_STRATEGIC_AREAS

log = logging.getLogger(__name__)


def _create_dim_tables(engine) -> None:
    """Create dimension tables if they don't exist (idempotent)."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dwh.dim_implementing_entity (
                id          SERIAL PRIMARY KEY,
                entity_name VARCHAR(255) UNIQUE NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dwh.dim_results_area (
                id        SERIAL PRIMARY KEY,
                area_name VARCHAR(255) UNIQUE NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dwh.dim_category (
                id            SERIAL PRIMARY KEY,
                category_name VARCHAR(255) UNIQUE NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dwh.dim_delivery_partner (
                id           SERIAL PRIMARY KEY,
                partner_name VARCHAR(255) UNIQUE NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dwh.dim_strategic_area (
                id        SERIAL PRIMARY KEY,
                area_name VARCHAR(255) UNIQUE NOT NULL
            )
        """))


def run_dimensions() -> None:
    log.info("Dimensions: start")
    engine = get_engine()

    _create_dim_tables(engine)

    with engine.connect() as conn:
        acts = pd.read_sql("SELECT * FROM stg.stg_activities", conn)
        inds = pd.read_sql("SELECT * FROM stg.stg_indicators", conn)

    _upsert_implementing_entities(engine, acts, inds)
    _upsert_results_areas(engine, acts)
    _upsert_categories(engine, acts)
    _upsert_delivery_partners(engine, acts)
    _upsert_strategic_areas(engine)
    _upsert_dim_indicator(engine, inds)

    engine.dispose()
    log.info("Dimensions: complete")


def _upsert_implementing_entities(engine, acts, inds) -> None:
    entities = set(acts["implementing_entity"].dropna().unique()) | \
               set(inds["implementing_entity"].dropna().unique())
    entities.discard("")
    with engine.begin() as conn:
        for name in sorted(entities):
            conn.execute(text("""
                INSERT INTO dwh.dim_implementing_entity (entity_name)
                VALUES (:n)
                ON CONFLICT (entity_name) DO NOTHING
            """), {"n": name})
    log.info(f"  dim_implementing_entity: {len(entities)} entities")


def _upsert_results_areas(engine, acts) -> None:
    areas = set(acts["results_area"].dropna().unique())
    areas.discard("")
    with engine.begin() as conn:
        for name in sorted(areas):
            conn.execute(text("""
                INSERT INTO dwh.dim_results_area (area_name)
                VALUES (:n)
                ON CONFLICT (area_name) DO NOTHING
            """), {"n": name})
    log.info(f"  dim_results_area: {len(areas)} areas")


def _upsert_categories(engine, acts) -> None:
    cats = set(acts["category"].dropna().unique())
    cats.discard("")
    with engine.begin() as conn:
        for name in sorted(cats):
            conn.execute(text("""
                INSERT INTO dwh.dim_category (category_name)
                VALUES (:n)
                ON CONFLICT (category_name) DO NOTHING
            """), {"n": name})
    log.info(f"  dim_category: {len(cats)} categories")


def _upsert_delivery_partners(engine, acts) -> None:
    partners = set(acts["delivery_partner"].dropna().unique())
    partners.discard("")
    with engine.begin() as conn:
        for name in sorted(partners):
            conn.execute(text("""
                INSERT INTO dwh.dim_delivery_partner (partner_name)
                VALUES (:n)
                ON CONFLICT (partner_name) DO NOTHING
            """), {"n": name})
    log.info(f"  dim_delivery_partner: {len(partners)} partners")


def _upsert_strategic_areas(engine) -> None:
    with engine.begin() as conn:
        for name in ALL_STRATEGIC_AREAS:
            conn.execute(text("""
                INSERT INTO dwh.dim_strategic_area (area_name)
                VALUES (:n)
                ON CONFLICT (area_name) DO NOTHING
            """), {"n": name})
    log.info(f"  dim_strategic_area: {len(ALL_STRATEGIC_AREAS)} areas")


def _upsert_dim_indicator(engine, inds: pd.DataFrame) -> None:
    df = inds.copy()
    df["quantitative_flag"] = df["indicator_type"].str.lower().str.contains(
        "quant", na=False
    )
    df["qualitative_flag"] = df["indicator_type"].str.lower().str.contains(
        "qual", na=False
    )
    df["strategic_area"] = df.apply(
        lambda r: map_strategic_area(
            r.get("new_proposed_indicator", ""),
            r.get("key_project_activity", ""),
            r.get("indicator_definition", ""),
        ),
        axis=1,
    )

    rows = df[[
        "id", "activity_code", "new_proposed_indicator",
        "indicator_definition", "indicator_type", "naphs",
        "quantitative_flag", "qualitative_flag", "strategic_area",
        "implementing_entity", "data_source",
    ]].rename(columns={
        "id":                     "indicator_id",
        "new_proposed_indicator": "indicator_text",
        "naphs":                  "naphs_flag",
        "implementing_entity":    "entity_name",
    })

    with engine.begin() as conn:
        rows.to_sql("dim_indicator", conn, schema="dwh",
                    if_exists="replace", index=False)
    log.info(f"  dim_indicator: {len(rows)} indicators")
