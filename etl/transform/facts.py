"""
etl/transform/facts.py
======================
Builds fact_indicator_progress and fact_budget_execution from staging.
All business logic (scoring, classification, execution rate) lives here,
imported from config.py.
"""

from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from etl.db     import get_engine
from etl.config import (
    map_strategic_area,
    score_qualitative_stage,
    compute_achievement_category,
    classify_indicator_subtype,
)

log = logging.getLogger(__name__)


def run_facts() -> None:
    log.info("Facts: start")
    engine = get_engine()

    with engine.connect() as conn:
        inds = pd.read_sql("SELECT * FROM stg.stg_indicators", conn)
        acts = pd.read_sql("SELECT * FROM stg.stg_activities", conn)

    _build_fact_indicator_progress(engine, inds)
    _build_fact_budget_execution(engine, acts)

    engine.dispose()
    log.info("Facts: complete")


def _build_fact_indicator_progress(engine, df: pd.DataFrame) -> None:
    log.info("  Building fact_indicator_progress")

    df = df.copy()
    df["quantitative_flag"] = df["indicator_type"].str.lower().str.contains(
        "quant", na=False
    )
    df["qualitative_flag"] = df["indicator_type"].str.lower().str.contains(
        "qual", na=False
    )

    # Sub-type: Number vs Percentage vs Qualitative
    # Percentage indicators must use actual value directly as progress (never sum)
    df["indicator_subtype"] = df.apply(
        lambda r: (
            "Qualitative" if r["qualitative_flag"]
            else classify_indicator_subtype(
                r.get("new_proposed_indicator", ""),
                r.get("indicator_definition", ""),
            )
        ),
        axis=1,
    )

    df["strategic_area"] = df.apply(
        lambda r: map_strategic_area(
            r.get("new_proposed_indicator", ""),
            r.get("key_project_activity", ""),
            r.get("indicator_definition", ""),
        ),
        axis=1,
    )

    year_rows = []
    for year in (1, 2, 3):
        sub = df[[
            "id", "activity_code", "implementing_entity",
            "strategic_area", "indicator_type",
            "quantitative_flag", "qualitative_flag", "naphs",
            "indicator_subtype",
            f"target_year{year}", f"actual_year{year}",
            f"progress_year{year}",
            f"status_year{year}",
            f"qualitative_stage_year{year}",
            "last_progress_update",
        ]].copy()

        sub = sub.rename(columns={
            "id":                           "indicator_id",
            "implementing_entity":          "entity_name",
            "naphs":                        "naphs_flag",
            f"target_year{year}":           "target",
            f"actual_year{year}":           "actual",
            f"progress_year{year}":         "progress_pct",
            f"status_year{year}":           "status",
            f"qualitative_stage_year{year}":"qualitative_stage",
        })
        sub["year_number"] = year

        # Qualitative score
        sub["qualitative_score"] = sub["qualitative_stage"].apply(
            lambda s: score_qualitative_stage(s) if pd.notna(s) else 0
        )

        # Completion rate (avoid division by zero)
        # For Percentage sub-type: actual IS already a % value, so
        # completion_rate = actual/100 (normalise to 0-1 for achievement logic)
        # For Number sub-type: completion_rate = actual/target (standard)
        has_data = sub["target"].notna() & (sub["target"] > 0) & sub["actual"].notna()
        is_pct   = sub["indicator_subtype"] == "Percentage"

        sub["completion_rate"] = np.where(
            has_data & ~is_pct,
            sub["actual"] / sub["target"],
            np.where(
                sub["actual"].notna() & is_pct,
                sub["actual"] / 100.0,   # normalise % to 0-1 for achievement thresholds
                np.nan,
            ),
        )

        # Gap (meaningful only for Number indicators with explicit targets)
        sub["gap"] = np.where(
            has_data & ~is_pct,
            sub["target"] - sub["actual"],
            np.nan,
        )

        # Achievement category
        sub["achievement_category"] = sub.apply(
            lambda r: compute_achievement_category(
                completion_rate  = r["completion_rate"]  if pd.notna(r["completion_rate"])  else None,
                is_qualitative   = bool(r["qualitative_flag"]),
                qualitative_score= r["qualitative_score"] if pd.notna(r["qualitative_score"]) else None,
                status_text      = str(r["status"] or ""),
            ),
            axis=1,
        )

        year_rows.append(sub)

    fact_df = pd.concat(year_rows, ignore_index=True)[[
        "indicator_id", "activity_code", "entity_name", "strategic_area",
        "indicator_type", "indicator_subtype",
        "quantitative_flag", "qualitative_flag", "naphs_flag",
        "year_number", "target", "actual", "progress_pct",
        "completion_rate", "gap",
        "qualitative_stage", "qualitative_score", "achievement_category",
        "status", "last_progress_update",
    ]]

    with engine.begin() as conn:
        fact_df.to_sql("fact_indicator_progress", conn, schema="dwh",
                       if_exists="replace", index=False)

    log.info(f"  → {len(fact_df)} fact_indicator_progress rows")


def _build_fact_budget_execution(engine, df: pd.DataFrame) -> None:
    log.info("  Building fact_budget_execution")

    year_rows = []
    for year in (1, 2, 3):
        budget_col = f"budget_year{year}"
        used_col   = f"budget_used_year{year}"
        sub = df[[
            "code", "implementing_entity", "results_area",
            "delivery_partner", "category",
            budget_col, used_col,
            "proposed_activity",
            "progress", "status", "start_date", "end_date",
        ]].copy()

        sub = sub.rename(columns={
            "code":               "activity_code",
            "implementing_entity":"entity_name",
            budget_col:           "budget_allocated",
            used_col:             "budget_used",
        })
        sub["year_number"] = year

        # Execution rate — safe division
        sub["execution_rate"] = np.where(
            sub["budget_allocated"].notna() & (sub["budget_allocated"] > 0),
            sub["budget_used"].fillna(0) / sub["budget_allocated"],
            0.0,
        )

        year_rows.append(sub)

    fact_df = pd.concat(year_rows, ignore_index=True)

    with engine.begin() as conn:
        fact_df.to_sql("fact_budget_execution", conn, schema="dwh",
                       if_exists="replace", index=False)

    log.info(f"  → {len(fact_df)} fact_budget_execution rows")
