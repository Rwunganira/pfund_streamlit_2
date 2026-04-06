"""
tests/test_config.py
====================
Unit tests for all classification functions in etl/config.py.
Run with: pytest tests/
"""

import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.config import (
    map_strategic_area,
    score_qualitative_stage,
    compute_achievement_category,
)


# ── Strategic area mapping ────────────────────────────────────────────────────

class TestMapStrategicArea:
    def test_surveillance_keyword(self):
        assert map_strategic_area("outbreak detection system") == "Surveillance"

    def test_workforce_keyword(self):
        assert map_strategic_area("trained epidemiologists") == "Workforce Development"

    def test_laboratory_keyword(self):
        assert map_strategic_area("PCR equipment procured") == "Laboratory Systems"

    def test_policy_keyword(self):
        assert map_strategic_area("national strategy validated") == "Policy / Strategy / Governance"

    def test_one_health_keyword(self):
        assert map_strategic_area("wildlife surveillance") == "Surveillance"  # first match wins

    def test_one_health_specific(self):
        assert map_strategic_area("veterinary officers recruited") == "Workforce Development"

    def test_fallback_other(self):
        assert map_strategic_area("unrelated activity") == "Other / Cross-Cutting"

    def test_empty_string(self):
        assert map_strategic_area("") == "Other / Cross-Cutting"

    def test_uses_activity_text(self):
        assert map_strategic_area("", "laboratory equipment procured", "") == "Laboratory Systems"

    def test_uses_definition_text(self):
        assert map_strategic_area("", "", "number of epidemiologists trained") == "Workforce Development"


# ── Qualitative score mapping ─────────────────────────────────────────────────

class TestScoreQualitativeStage:
    def test_completed(self):
        assert score_qualitative_stage("completed") == 100

    def test_validated(self):
        assert score_qualitative_stage("validated") == 100

    def test_finalized(self):
        assert score_qualitative_stage("finalized") == 100

    def test_in_progress(self):
        assert score_qualitative_stage("in progress") == 50

    def test_not_started(self):
        assert score_qualitative_stage("not started") == 0

    def test_unknown_stage(self):
        assert score_qualitative_stage("some unknown stage") == 0

    def test_case_insensitive(self):
        assert score_qualitative_stage("COMPLETED") == 100
        assert score_qualitative_stage("In Progress") == 50

    def test_whitespace(self):
        assert score_qualitative_stage("  validated  ") == 100


# ── Achievement category ──────────────────────────────────────────────────────

class TestComputeAchievementCategory:

    # Quantitative cases
    def test_quant_completed(self):
        assert compute_achievement_category(1.0, False, None) == "Completed"

    def test_quant_over_100(self):
        assert compute_achievement_category(1.5, False, None) == "Completed"

    def test_quant_on_track(self):
        assert compute_achievement_category(0.75, False, None) == "On Track"

    def test_quant_at_risk(self):
        assert compute_achievement_category(0.46, False, None) == "At Risk"

    def test_quant_not_started_zero(self):
        assert compute_achievement_category(0.0, False, None) == "Not Started"

    def test_quant_not_started_none(self):
        assert compute_achievement_category(None, False, None) == "Not Started"

    # Qualitative cases
    def test_qual_completed(self):
        assert compute_achievement_category(None, True, 100) == "Completed"

    def test_qual_on_track(self):
        assert compute_achievement_category(None, True, 70) == "On Track"

    def test_qual_at_risk(self):
        assert compute_achievement_category(None, True, 50) == "At Risk"

    def test_qual_not_started(self):
        assert compute_achievement_category(None, True, 0) == "Not Started"

    def test_qual_status_fallback_completed(self):
        result = compute_achievement_category(None, True, 0, "strategy validated")
        assert result == "Completed"

    def test_qual_status_fallback_on_track(self):
        result = compute_achievement_category(None, True, 0, "in progress")
        assert result == "On Track"
