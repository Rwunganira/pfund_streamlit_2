"""
etl/transform/marts.py
======================
Builds all mart_ tables from the warehouse fact/dimension tables.
These are the tables the Streamlit dashboard reads directly.
"""

from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from etl.db import get_engine

log = logging.getLogger(__name__)

STALE_DAYS = 90   # indicators not updated within this many days are flagged


def run_marts() -> None:
    log.info("Marts: start")
    engine = get_engine()

    with engine.connect() as conn:
        facts_ind = pd.read_sql("SELECT * FROM dwh.fact_indicator_progress", conn)
        facts_bud = pd.read_sql("SELECT * FROM dwh.fact_budget_execution",   conn)

    _build_mart_indicator_kpis(engine, facts_ind)
    _build_mart_entity_performance(engine, facts_ind)
    _build_mart_strategic_summary(engine, facts_ind)
    _build_mart_indicator_tracker(engine, facts_ind)
    _build_mart_budget_performance(engine, facts_bud)
    _build_mart_activity_status(engine, facts_bud)

    engine.dispose()
    log.info("Marts: complete")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_mean(s: pd.Series) -> float:
    s = s.dropna()
    return float(s.mean()) if not s.empty else 0.0


def _eff_progress(row) -> float:
    """
    Effective progress respecting indicator sub-type:
    - Percentage: actual value IS the progress (e.g. 75 means 75% achieved).
                  Never multiply completion_rate×100 as that compounds the %.
    - Number:     reported progress_pct → completion_rate×100 fallback.
    - Qualitative: qualitative_score.
    """
    subtype = row.get("indicator_subtype", "Number")

    if subtype == "Percentage":
        actual = row.get("actual")
        if pd.notna(actual):
            return float(actual)
        # fallback: reported progress_pct
        if pd.notna(row.get("progress_pct")):
            return float(row["progress_pct"])
        return np.nan

    if pd.notna(row.get("progress_pct")):
        return float(row["progress_pct"])
    if row.get("qualitative_flag") and pd.notna(row.get("qualitative_score")):
        return float(row["qualitative_score"])
    if pd.notna(row.get("completion_rate")):
        return float(row["completion_rate"]) * 100
    return np.nan


def _add_eff_progress(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["eff_progress"] = df.apply(_eff_progress, axis=1)
    return df


# ── KPI mart ─────────────────────────────────────────────────────────────────

def _build_mart_indicator_kpis(engine, facts: pd.DataFrame) -> None:
    facts = _add_eff_progress(facts)
    rows = []
    for year in (1, 2, 3):
        df = facts[facts["year_number"] == year]
        total = len(df)
        if total == 0:
            continue
        cats = df["achievement_category"].value_counts()
        def pct(lbl): return round(cats.get(lbl, 0) / total * 100, 1)

        # Sub-type splits
        num_df  = df[df.get("indicator_subtype", pd.Series("Number", index=df.index)) == "Number"]
        pct_df  = df[df.get("indicator_subtype", pd.Series("Number", index=df.index)) == "Percentage"]
        # Prefer explicit column if present
        if "indicator_subtype" in df.columns:
            num_df = df[df["indicator_subtype"] == "Number"]
            pct_df = df[df["indicator_subtype"] == "Percentage"]

        # Number indicator progress: sum_actual / sum_target × 100
        n_total_actual = num_df["actual"].sum(skipna=True)
        n_total_target = num_df["target"].sum(skipna=True)
        avg_progress_number = (
            round(n_total_actual / n_total_target * 100, 1)
            if n_total_target > 0 else 0.0
        )

        # Percentage indicator progress: mean of actuals (never sum)
        avg_progress_pct = (
            round(pct_df["actual"].dropna().mean(), 1)
            if not pct_df.empty and pct_df["actual"].notna().any()
            else 0.0
        )

        # Reporting rate: % of indicators that have at least one actual
        has_actual = df["actual"].notna() | df["progress_pct"].notna()
        reporting_rate = round(has_actual.sum() / total * 100, 1)

        rows.append({
            "year_number":            year,
            "total_indicators":       total,
            "quantitative":           int(df["quantitative_flag"].sum()),
            "qualitative":            int(df["qualitative_flag"].sum()),
            "num_count_number":       len(num_df),
            "num_count_percentage":   len(pct_df),
            "pct_completed":          pct("Completed"),
            "pct_on_track":           pct("On Track"),
            "pct_at_risk":            pct("At Risk"),
            "pct_not_started":        pct("Not Started"),
            "avg_progress":           round(_safe_mean(df["eff_progress"]), 1),
            "avg_progress_number":    avg_progress_number,
            "avg_progress_percentage":avg_progress_pct,
            "reporting_rate":         reporting_rate,
        })
    kpi_df = pd.DataFrame(rows)
    with engine.begin() as conn:
        kpi_df.to_sql("mart_indicator_kpis", conn, schema="mart",
                      if_exists="replace", index=False)
    log.info(f"  mart_indicator_kpis: {len(kpi_df)} rows")


# ── Entity performance mart ───────────────────────────────────────────────────

def _build_mart_entity_performance(engine, facts: pd.DataFrame) -> None:
    facts = _add_eff_progress(facts)
    rows = []
    for year in (1, 2, 3):
        df = facts[facts["year_number"] == year]
        for entity, grp in df.groupby("entity_name", dropna=False):
            cats = grp["achievement_category"].value_counts()
            rows.append({
                "year_number":      year,
                "entity_name":      entity,
                "total_indicators": len(grp),
                "avg_progress":     round(_safe_mean(grp["eff_progress"]), 1),
                "completed":        int(cats.get("Completed",   0)),
                "on_track":         int(cats.get("On Track",    0)),
                "at_risk":          int(cats.get("At Risk",     0)),
                "not_started":      int(cats.get("Not Started", 0)),
            })
    ep_df = pd.DataFrame(rows)
    with engine.begin() as conn:
        ep_df.to_sql("mart_entity_performance", conn, schema="mart",
                     if_exists="replace", index=False)
    log.info(f"  mart_entity_performance: {len(ep_df)} rows")


# ── Strategic summary mart ────────────────────────────────────────────────────

def _build_mart_strategic_summary(engine, facts: pd.DataFrame) -> None:
    facts = _add_eff_progress(facts)
    rows = []
    for year in (1, 2, 3):
        df = facts[facts["year_number"] == year]
        for area, grp in df.groupby("strategic_area", dropna=False):
            cats = grp["achievement_category"].value_counts()
            rows.append({
                "year_number":    year,
                "strategic_area": area,
                "num_indicators": len(grp),
                "avg_progress":   round(_safe_mean(grp["eff_progress"]), 1),
                "completed":      int(cats.get("Completed",   0)),
                "on_track":       int(cats.get("On Track",    0)),
                "at_risk":        int(cats.get("At Risk",     0)),
                "not_started":    int(cats.get("Not Started", 0)),
            })
    ss_df = pd.DataFrame(rows)
    with engine.begin() as conn:
        ss_df.to_sql("mart_strategic_summary", conn, schema="mart",
                     if_exists="replace", index=False)
    log.info(f"  mart_strategic_summary: {len(ss_df)} rows")


# ── Indicator tracker mart (full detail + bottleneck flags) ───────────────────

def _build_mart_indicator_tracker(engine, facts: pd.DataFrame) -> None:
    facts = _add_eff_progress(facts)
    stale_cutoff = pd.Timestamp.now() - pd.Timedelta(days=STALE_DAYS)

    # Load indicator text from dim
    engine2 = get_engine()
    with engine2.connect() as conn:
        dim = pd.read_sql(
            "SELECT indicator_id, indicator_text FROM dwh.dim_indicator",
            conn,
        )
    engine2.dispose()

    df = facts.merge(dim, on="indicator_id", how="left")

    # ── Existing bottleneck flags ─────────────────────────────────────────────
    df["no_actuals_flag"] = (
        df["quantitative_flag"] &
        df["target"].notna() & (df["target"] > 0) &
        (df["actual"].isna() | (df["actual"] == 0))
    )
    df["at_risk_flag"] = df["achievement_category"] == "At Risk"

    lu = pd.to_datetime(df["last_progress_update"], errors="coerce")
    df["stale_flag"] = lu.isna() | (lu < stale_cutoff)

    df["gap_rank"] = (
        df.groupby("year_number")["gap"]
        .rank(method="dense", ascending=False, na_option="bottom")
        .astype("Int64")
    )

    # ── New quality flags from analysis ──────────────────────────────────────
    # Over-achievement: Number indicators where actual > target (potential data error)
    df["over_target_flag"] = (
        df.get("indicator_subtype", "Number").eq("Number") &
        df["actual"].notna() & df["target"].notna() & (df["target"] > 0) &
        (df["actual"] > df["target"])
    ) if "indicator_subtype" in df.columns else False

    # Status mismatch: labelled On Track but calculated progress < 50%
    df["status_mismatch_flag"] = (
        df["status"].str.lower().str.contains("on track|on-track", na=False) &
        df["eff_progress"].notna() &
        (df["eff_progress"] < 50)
    )

    # Has actual data reported (for compliance reporting)
    df["has_actual"] = df["actual"].notna() | df["progress_pct"].notna()

    tracker_cols = [
        "indicator_id", "year_number", "activity_code", "entity_name",
        "indicator_text", "indicator_type",
        *([c] for c in ["indicator_subtype"] if c in df.columns),
        "strategic_area",
        "naphs_flag", "quantitative_flag", "qualitative_flag",
        "target", "actual", "eff_progress", "gap", "completion_rate",
        "qualitative_stage", "qualitative_score", "achievement_category",
        "status", "last_progress_update",
        "no_actuals_flag", "at_risk_flag", "stale_flag", "gap_rank",
        "over_target_flag", "status_mismatch_flag", "has_actual",
    ]
    # Flatten any nested lists from the conditional include
    flat_cols = []
    for c in tracker_cols:
        if isinstance(c, list):
            flat_cols.extend(c)
        else:
            flat_cols.append(c)
    # Only keep columns that exist in df
    flat_cols = [c for c in flat_cols if c in df.columns]

    tracker_df = df[flat_cols].rename(columns={"eff_progress": "progress_pct"})

    with engine.begin() as conn:
        tracker_df.to_sql("mart_indicator_tracker", conn, schema="mart",
                          if_exists="replace", index=False)
    log.info(f"  mart_indicator_tracker: {len(tracker_df)} rows")


# ── Budget performance mart ───────────────────────────────────────────────────

def _build_mart_budget_performance(engine, facts: pd.DataFrame) -> None:
    df = facts.drop(columns=["id"], errors="ignore")
    with engine.begin() as conn:
        df.to_sql("mart_budget_performance", conn, schema="mart",
                  if_exists="replace", index=False)
    log.info(f"  mart_budget_performance: {len(facts)} rows")


# ── Activity status mart ──────────────────────────────────────────────────────

def _build_mart_activity_status(engine, facts: pd.DataFrame) -> None:
    rows = []
    for year in (1, 2, 3):
        df = facts[facts["year_number"] == year]
        grp = (
            df.groupby(["entity_name", "results_area", "status"], dropna=False)
            .size()
            .reset_index(name="activity_count")
        )
        grp["year_number"] = year
        rows.append(grp)

    status_df = pd.concat(rows, ignore_index=True)
    with engine.begin() as conn:
        status_df.to_sql("mart_activity_status", conn, schema="mart",
                         if_exists="replace", index=False)
    log.info(f"  mart_activity_status: {len(status_df)} rows")
