"""README charts from output/run_summary.json. Usage: python charts/make_charts.py [output_dir]"""
import json
import sys
from pathlib import Path

import style as st

ROOT = Path(__file__).resolve().parents[1]
RES = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "output"
OUT = ROOT / "docs" / "charts"
d = json.loads((RES / "run_summary.json").read_text(encoding="utf-8"))
ways = [("day2_rules_only", "Rules only"), ("day2_model_only", "Model only"), ("day2_rules_or_model", "Rules or model")]
budget = d["alert_budget"]

f, (a1, a2) = st.plt.subplots(1, 2, figsize=(8, 3.4), dpi=150)
rec = [d[k]["recall"] for k, _ in ways]
val = [d[k]["fraud_value_caught"] / d[k]["fraud_value"] for k, _ in ways]
xs = range(len(ways))
b1 = a1.bar([i - 0.2 for i in xs], rec, 0.38, color=st.S[0], label="Frauds")
b2 = a1.bar([i + 0.2 for i in xs], val, 0.38, color=st.S[1], label="Fraud amount")
st.vbar_labels(a1, b1, lambda v: f"{v:.0%}")
st.vbar_labels(a1, b2, lambda v: f"{v:.0%}")
a1.set_xticks(list(xs), [n for _, n in ways])
a1.set_ylim(0, 1.05)
st.fmt(a1, "y", "pct")
st.head(a1, "About 86% of day-2 frauds caught", f"Share caught of {d['day2']['frauds']} frauds unseen in training", legend=True, ncol=2)

rate = [d[k]["alert_rate"] for k, _ in ways]
b3 = a2.bar(list(xs), rate, 0.55, color=[st.S[0], st.S[0], st.S[0]])
st.vbar_labels(a2, b3, lambda v: f"{v:.2%}")
a2.axhline(budget, color=st.CRITICAL, lw=1.2, label=f"Alert budget {budget:.1%}")
a2.set_xticks(list(xs), [n for _, n in ways])
st.fmt(a2, "y", "pct", 1)
st.head(a2, "But alerts run at 3.5x the budget", f"Share of day-2 traffic alerted; score drift PSI {d['score_psi_day1_vs_day2']:.2f}", legend=True)
st.save(f, OUT / "day2_results.png")
print(f"wrote charts to {OUT}")
