"""Fraud scoring trained on day 1 only, so day 2 is a fair, out-of-time test.

rules   three interpretable rules on the features that separate fraud most on day 1. Each threshold is the 0.2nd
        percentile of genuine transactions on day 1, so each rule alone would flag about 0.2% of genuine traffic.
model   gradient-boosted trees on V1-V28 and log amount, trained on day 1 to 18:00, with the alert threshold set on
        day 1 from 18:00 to midnight to meet an alert budget: the share of transactions analysts can review.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from .events import FEATURES

ALERT_BUDGET = 0.002          # analysts can review 0.2% of transactions (about 280 a day)
RULE_PERCENTILE = 0.2


@dataclass
class Scorer:
    model: HistGradientBoostingClassifier
    threshold: float
    rules: dict[str, float]           # feature -> flag when the value is below this

    def score(self, batch: pd.DataFrame) -> np.ndarray:
        return self.model.predict_proba(batch[FEATURES].to_numpy())[:, 1]

    def rule_hits(self, batch: pd.DataFrame) -> np.ndarray:
        hit = np.zeros(len(batch), dtype=bool)
        for f, t in self.rules.items():
            hit |= batch[f].to_numpy() < t
        return hit


def train(day1: pd.DataFrame, seed: int = 7) -> Scorer:
    fit_part = day1[day1["event_seconds"] < 18 * 3600]
    tune_part = day1[day1["event_seconds"] >= 18 * 3600]
    model = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
                                           l2_regularization=1.0, class_weight={0: 1, 1: 20}, random_state=seed)
    model.fit(fit_part[FEATURES].to_numpy(), fit_part["is_fraud"].to_numpy())
    tune_scores = model.predict_proba(tune_part[FEATURES].to_numpy())[:, 1]
    threshold = float(np.quantile(tune_scores, 1 - ALERT_BUDGET))

    genuine, fraud = day1[~day1["is_fraud"]], day1[day1["is_fraud"]]
    separation = {f: (genuine[f].median() - fraud[f].median()) / genuine[f].std() for f in FEATURES if f != "log_amount"}
    top = sorted(separation, key=lambda f: -separation[f])[:3]          # features where fraud sits far below genuine
    rules = {f: float(np.percentile(genuine[f], RULE_PERCENTILE)) for f in top}
    return Scorer(model, threshold, rules)
