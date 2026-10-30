# 3-minute walkthrough: Real-Time Payments Anomaly Stream

A timed script for a Loom recording of about 3 minutes (screen plus camera bubble, 1080p). Say it in your own words; the lines are a guide, not a teleprompter.

**Open these tabs before you press record:** README, `stream/events.py`, `stream/processor.py`, `stream/scoring.py`, `output/run_summary.md`, `tests/test_stream.py`, GitHub Actions.

| Time | On screen | What to say |
|---|---|---|
| 0:00–0:20 | Portfolio card, then the README title | Fraud has to be caught as a payment arrives, from a feed that delivers events late, out of order and sometimes twice, and analysts can only review a few hundred alerts a day. This handles all of that on 284,807 real card transactions. |
| 0:20–0:40 | README Data section | At PwC I built near-real-time transaction and risk datasets for banks. This is the streaming side, on a public dataset of real, anonymised European card transactions over two days. The transactions are real; the delivery problems are simulated. |
| 0:40–1:05 | `events.py`: `delivery` | Each transaction arrives after a network delay. Half a percent arrive up to 30 minutes late, and 2% are delivered twice, the way at-least-once delivery behaves. |
| 1:05–1:35 | `processor.py` | Every micro-batch: drop redeliveries by transaction id, score every new transaction, raise alerts, and update five-minute windows with a watermark. The key rule: a late event is still scored and can still alert. It's only kept out of windows that are already published. |
| 1:35–2:00 | `scoring.py` | Train on day 1, test on day 2. Three interpretable rules on the features where fraud sits furthest from normal, and a gradient-boosted model whose threshold is set to an alert budget of 0.2% of traffic. |
| 2:00–2:25 | `run_summary.md` | On day 2 the model catches 181 of 211 frauds, 85.8%, at 18.6% precision. The three rules catch 176. Every transaction is processed exactly once, and batches of 500 take about 13 milliseconds. |
| 2:25–2:45 | README: the alert-budget paragraph | The lesson: the threshold tuned on day 1 alerts on 0.70% of day 2, three and a half times the budget, because scores drifted. In production I'd use a rolling-quantile threshold and alert on drift. |
| 2:45–3:00 | `test_stream.py`, then the README top | No fraud happened to arrive late in the replay, so a test builds one and proves it still alerts. For a bank: alerts that are never lost, and numbers that don't change after they're published. Links are under the video. |

## Recording checklist
- Close email, chat and notifications. Zoom the browser to 125% so code is readable.
- Run `make all` once before recording so the summary is fresh. Don't demo the Kafka wrappers; they haven't run against a live broker.
- Keep to one take per section; Loom lets you trim. Aim for 2:45 to 3:10.
- Show real numbers on screen as you say them; never estimate on camera.
- Say "real transactions, simulated delivery" when you introduce the data.

## After recording
1. Copy the Loom share link.
2. In `README.md`, replace the roadmap line with `- [x] [3-minute Loom walkthrough](<your Loom link>)` and add the link under the title.
3. Add the link to this project's card on saigwe.github.io.
