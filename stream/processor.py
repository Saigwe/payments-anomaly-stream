"""The stream processor: broker-agnostic, so the Kafka consumer and the offline replay feed the same code.

For each micro-batch of delivered messages (as a Kafka poll returns them):
  1. drop redeliveries already processed (idempotent: at-least-once in, effectively-once out)
  2. score every new transaction and raise an alert on a model score over the threshold or a rule hit.
     A fraud alert is never dropped, however late the event arrives.
  3. update 5-minute event-time monitoring windows. A watermark (latest event time minus the allowed lateness)
     finalises windows; an event older than the watermark still gets scored and alerted, but is counted as late
     instead of silently changing a window that has already been published.
"""
from __future__ import annotations

import time
from collections import defaultdict

import numpy as np
import pandas as pd

from .scoring import Scorer

WINDOW_SECONDS = 300
ALLOWED_LATENESS = 120


class Processor:
    def __init__(self, scorer: Scorer, window: int = WINDOW_SECONDS, lateness: int = ALLOWED_LATENESS):
        self.scorer, self.window, self.lateness = scorer, window, lateness
        self.seen: set[int] = set()
        self.watermark = -np.inf
        self.open_windows: dict[int, dict] = defaultdict(lambda: {"transactions": 0, "amount": 0.0, "alerts": 0, "score_sum": 0.0})
        self.final_windows: list[dict] = []
        self.alerts: list[dict] = []
        self.stats = {"messages": 0, "redeliveries_ignored": 0, "processed": 0, "late_events": 0}
        self.batch_ms: list[float] = []

    def process(self, messages: pd.DataFrame, transactions: pd.DataFrame) -> None:
        """messages: delivered rows (txn_id, event_seconds, arrival_seconds); transactions: features by txn_id."""
        t0 = time.perf_counter()
        self.stats["messages"] += len(messages)
        new = messages[~messages["txn_id"].isin(self.seen)].drop_duplicates("txn_id")
        self.stats["redeliveries_ignored"] += len(messages) - len(new)
        if len(new):
            self.seen.update(new["txn_id"].tolist())
            batch = transactions.loc[new["txn_id"].to_numpy()]
            scores = self.scorer.score(batch)
            rules = self.scorer.rule_hits(batch)
            alert = (scores >= self.scorer.threshold) | rules
            arrival = new["arrival_seconds"].to_numpy()
            for i in np.flatnonzero(alert):
                self.alerts.append({"txn_id": int(batch.index[i]), "event_seconds": float(batch["event_seconds"].iat[i]),
                                    "arrival_seconds": float(arrival[i]), "score": float(scores[i]),
                                    "model_alert": bool(scores[i] >= self.scorer.threshold), "rule_alert": bool(rules[i])})
            for ev, amt, sc, al in zip(batch["event_seconds"].to_numpy(), batch["Amount"].to_numpy(), scores, alert):
                if ev < self.watermark:
                    self.stats["late_events"] += 1
                    continue
                w = self.open_windows[int(ev // self.window) * self.window]
                w["transactions"] += 1
                w["amount"] += float(amt)
                w["alerts"] += int(al)
                w["score_sum"] += float(sc)
            self.stats["processed"] += len(new)
            self.watermark = max(self.watermark, float(batch["event_seconds"].max()) - self.lateness)
            self._finalise()
        self.batch_ms.append((time.perf_counter() - t0) * 1000)

    def _finalise(self, flush: bool = False) -> None:
        for start in sorted(self.open_windows):
            if flush or start + self.window <= self.watermark:
                w = self.open_windows.pop(start)
                self.final_windows.append({"window_start": start, **w,
                                           "mean_score": w["score_sum"] / w["transactions"] if w["transactions"] else 0.0})

    def close(self) -> None:
        self._finalise(flush=True)
