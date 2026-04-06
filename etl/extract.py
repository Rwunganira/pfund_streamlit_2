"""
etl/extract.py
==============
Copies operational tables into the staging layer.
All JOIN resolution happens here so nothing downstream touches
the operational schema.
"""

from __future__ import annotations
import logging
import pandas as pd
from sqlalchemy import text
from etl.db import get_source_engine, get_warehouse_engine

log = logging.getLogger(__name__)


def run_extract() -> None:
    log.info("Extract: start")
    src = get_source_engine()
    wh  = get_warehouse_engine()

    _extract_activities(src, wh)
    _extract_indicators(src, wh)

    src.dispose()
    wh.dispose()
    log.info("Extract: complete")


def _extract_activities(src, wh) -> None:
    log.info("  Extracting activities → stg.stg_activities")
    with src.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                    code, initial_activity, proposed_activity,
                    implementing_entity, delivery_partner,
                    results_area, category,
                    CAST(budget_year1 AS NUMERIC)        AS budget_year1,
                    CAST(budget_year2 AS NUMERIC)        AS budget_year2,
                    CAST(budget_year3 AS NUMERIC)        AS budget_year3,
                    CAST(budget_total AS NUMERIC)        AS budget_total,
                    CAST(budget_used  AS NUMERIC)        AS budget_used,
                    CAST(budget_used_year1 AS NUMERIC)   AS budget_used_year1,
                    CAST(budget_used_year2 AS NUMERIC)   AS budget_used_year2,
                    CAST(budget_used_year3 AS NUMERIC)   AS budget_used_year3,
                    status,
                    CAST(progress AS NUMERIC)            AS progress,
                    notes,
                    start_date::DATE                     AS start_date,
                    end_date::DATE                       AS end_date
                FROM public.activities
                ORDER BY code
            """),
            conn,
        )

    # Truncate and reload staging
    with wh.begin() as conn:
        conn.execute(text("TRUNCATE stg.stg_activities"))
        df.to_sql("stg_activities", conn, schema="stg",
                  if_exists="append", index=False)

    log.info(f"  → {len(df)} activities staged")


def _extract_indicators(src, wh) -> None:
    log.info("  Extracting indicators → stg.stg_indicators (with activity JOIN)")
    with src.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT
                    i.id,
                    i.activity_id,
                    i.activity_code,
                    COALESCE(a.implementing_entity, '')  AS implementing_entity,
                    i.key_project_activity,
                    i.new_proposed_indicator,
                    TRIM(i.indicator_type)               AS indicator_type,
                    i.naphs,
                    i.indicator_definition,
                    i.data_source,
                    CASE WHEN TRIM(i.baseline_proposal_year::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.baseline_proposal_year::TEXT)::NUMERIC ELSE NULL END AS baseline_proposal_year,
                    CASE WHEN TRIM(i.target_year1::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.target_year1::TEXT)::NUMERIC ELSE NULL END AS target_year1,
                    CASE WHEN TRIM(i.target_year2::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.target_year2::TEXT)::NUMERIC ELSE NULL END AS target_year2,
                    CASE WHEN TRIM(i.target_year3::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.target_year3::TEXT)::NUMERIC ELSE NULL END AS target_year3,
                    i.submitted,
                    i.comments,
                    i.portal_edited,
                    i.comment_addressed,
                    CASE WHEN TRIM(i.actual_baseline::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.actual_baseline::TEXT)::NUMERIC ELSE NULL END AS actual_baseline,
                    CASE WHEN TRIM(i.actual_year1::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.actual_year1::TEXT)::NUMERIC ELSE NULL END AS actual_year1,
                    CASE WHEN TRIM(i.actual_year2::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.actual_year2::TEXT)::NUMERIC ELSE NULL END AS actual_year2,
                    CASE WHEN TRIM(i.actual_year3::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.actual_year3::TEXT)::NUMERIC ELSE NULL END AS actual_year3,
                    CASE WHEN TRIM(i.progress_year1::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.progress_year1::TEXT)::NUMERIC ELSE NULL END AS progress_year1,
                    CASE WHEN TRIM(i.progress_year2::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.progress_year2::TEXT)::NUMERIC ELSE NULL END AS progress_year2,
                    CASE WHEN TRIM(i.progress_year3::TEXT) ~ '^-?[0-9]*[.]?[0-9]+$' THEN TRIM(i.progress_year3::TEXT)::NUMERIC ELSE NULL END AS progress_year3,
                    TRIM(i.status_year1)                       AS status_year1,
                    TRIM(i.status_year2)                       AS status_year2,
                    TRIM(i.status_year3)                       AS status_year3,
                    i.last_progress_update::DATE               AS last_progress_update,
                    LOWER(TRIM(i.qualitative_stage_year1))     AS qualitative_stage_year1,
                    LOWER(TRIM(i.qualitative_stage_year2))     AS qualitative_stage_year2,
                    LOWER(TRIM(i.qualitative_stage_year3))     AS qualitative_stage_year3
                FROM public.indicators i
                LEFT JOIN public.activities a ON a.code = i.activity_code
                ORDER BY i.activity_code, i.id
            """),
            conn,
        )

    with wh.begin() as conn:
        conn.execute(text("TRUNCATE stg.stg_indicators"))
        df.to_sql("stg_indicators", conn, schema="stg",
                  if_exists="append", index=False)

    log.info(f"  → {len(df)} indicators staged")
