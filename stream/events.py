"""Card transactions as events, and the way a real stream delivers them.

Data: the ULB credit card fraud dataset (Dal Pozzolo et al., 2015) via OpenML 1597: 284,807 real card
transactions by European cardholders over two days in September 2013, 492 of them fraud. Features V1-V28 are a PCA of
the original fields (the bank couldn't release them), plus Time (seconds from the first transaction) and Amount.

delivery() turns them into what a consumer actually sees: events arrive after a network delay, a few arrive very late
(beyond the watermark), and some are redelivered (at-least-once delivery). Fixed seed.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

FEATURES = [f"V{i}" for i in range(1, 29)] + ["log_amount"]
SECONDS_PER_DAY = 86_400
SEED = 2013


def load(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df = df.reset_index(drop=True)
    df.insert(0, "txn_id", np.arange(len(df)))
    df["event_seconds"] = df["Time"].astype(float)
    df["day"] = np.where(df["event_seconds"] < SECONDS_PER_DAY, 1, 2)
    df["log_amount"] = np.log1p(df["Amount"].astype(float))
    df["is_fraud"] = df["Class"].astype(int) == 1
    return df


def delivery(df: pd.DataFrame, seed: int = SEED, dup_rate: float = 0.02, late_rate: float = 0.005,
             late_minutes: tuple[int, int] = (6, 30)) -> pd.DataFrame:
    """One row per delivered message, in arrival order: (txn_id, event_seconds, arrival_seconds, is_redelivery)."""
    rng = np.random.default_rng(seed)
    n = len(df)
    delay = rng.lognormal(mean=0.5, sigma=0.8, size=n)                        # median ~1.6 s
    late = rng.random(n) < late_rate
    delay[late] = rng.uniform(late_minutes[0] * 60, late_minutes[1] * 60, late.sum())
    first = pd.DataFrame({"txn_id": df["txn_id"], "event_seconds": df["event_seconds"],
                          "arrival_seconds": df["event_seconds"] + delay, "is_redelivery": False})
    dup = rng.random(n) < dup_rate
    again = first[dup].copy()
    again["arrival_seconds"] += rng.uniform(1, 120, dup.sum())
    again["is_redelivery"] = True
    out = pd.concat([first, again], ignore_index=True)
    return out.sort_values(["arrival_seconds", "txn_id"], kind="stable").reset_index(drop=True)
