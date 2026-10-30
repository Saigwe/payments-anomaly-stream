"""Processor and delivery behaviour on small, hand-built streams (no data download, no broker)."""
from __future__ import annotations

import numpy as np
import pandas as pd

from stream.events import FEATURES, delivery
from stream.processor import Processor
from stream.scoring import Scorer


class FixedModel:
    """Scores each transaction by its V1 value, so a test controls which ones alert."""
    def predict_proba(self, x):
        p = x[:, 0]
        return np.column_stack([1 - p, p])


def transactions(n: int, fraud_ids=()) -> pd.DataFrame:
    df = pd.DataFrame(0.0, index=np.arange(n), columns=FEATURES)
    df["V1"] = [0.9 if i in fraud_ids else 0.01 for i in range(n)]
    df["event_seconds"] = np.arange(n) * 60.0          # one transaction a minute
    df["Amount"] = 10.0
    df.index.name = "txn_id"
    return df


def messages(rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["txn_id", "event_seconds", "arrival_seconds"])


def scorer() -> Scorer:
    return Scorer(FixedModel(), threshold=0.5, rules={})


def test_redeliveries_are_processed_once():
    tx = transactions(5, fraud_ids={2})
    p = Processor(scorer())
    p.process(messages([(i, i * 60.0, i * 60.0 + 1) for i in range(5)] + [(2, 120.0, 200.0)]), tx)
    p.process(messages([(2, 120.0, 400.0)]), tx)
    assert p.stats["processed"] == 5 and p.stats["redeliveries_ignored"] == 2
    assert [a["txn_id"] for a in p.alerts] == [2]


def test_a_late_fraud_is_still_alerted_but_does_not_change_a_closed_window():
    tx = transactions(30, fraud_ids={1})
    p = Processor(scorer(), window=300, lateness=120)
    on_time = [i for i in range(30) if i != 1]
    p.process(messages([(i, i * 60.0, i * 60.0 + 1) for i in on_time]), tx)
    closed_first = next(w for w in p.final_windows if w["window_start"] == 0)
    p.process(messages([(1, 60.0, 1800.0)]), tx)          # 29 minutes late, behind the watermark
    assert [a["txn_id"] for a in p.alerts] == [1]
    assert p.stats["late_events"] == 1
    assert closed_first["transactions"] == 4 and closed_first["alerts"] == 0


def test_windows_count_every_on_time_transaction_once():
    tx = transactions(20)
    p = Processor(scorer(), window=300, lateness=0)
    p.process(messages([(i, i * 60.0, i * 60.0 + 1) for i in range(20)]), tx)
    p.close()
    assert sum(w["transactions"] for w in p.final_windows) == 20


def test_delivery_redelivers_some_messages_and_delivers_every_transaction():
    df = pd.DataFrame({"txn_id": np.arange(20_000), "event_seconds": np.arange(20_000, dtype=float)})
    msgs = delivery(df)
    assert set(msgs["txn_id"]) == set(df["txn_id"])
    dup_share = msgs["is_redelivery"].mean()
    assert 0.01 < dup_share < 0.03
    assert msgs["arrival_seconds"].is_monotonic_increasing
