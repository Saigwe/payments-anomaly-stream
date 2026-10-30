"""Checks on a full replay (run `make run` first; skipped when its output isn't there)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

SUMMARY = Path(__file__).resolve().parents[1] / "output" / "run_summary.json"
pytestmark = pytest.mark.skipif(not SUMMARY.exists(), reason="run `make run` first")


@pytest.fixture(scope="module")
def s():
    return json.loads(SUMMARY.read_text())


def test_every_transaction_is_processed_exactly_once(s):
    d = s["delivery"]
    assert d["processed"] == s["transactions"] == 284_807
    assert d["messages"] == d["processed"] + d["redeliveries_ignored"]


def test_day_two_is_out_of_time_and_catches_most_fraud(s):
    assert s["day1"]["transactions"] + s["day2"]["transactions"] == s["transactions"]
    assert s["day2_model_only"]["recall"] >= 0.8 and s["day2_rules_only"]["recall"] >= 0.8


def test_late_events_are_scored_not_dropped(s):
    assert s["late_messages_over_5_min"] > 0 and s["alerts_on_late_events"] > 0
