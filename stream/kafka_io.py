"""Thin Kafka wrappers around the same Processor the offline replay uses.

Not yet run against a live broker: the processing logic is what CI tests. To try it, start Redpanda with
`docker compose up -d`, then run the consumer and producer in two terminals:

    python -m stream.kafka_io consume
    python -m stream.kafka_io produce

The producer keys messages by transaction id and sends them in the delivery order from events.delivery(). The
consumer commits offsets only after a batch is processed (at-least-once); the Processor drops redeliveries.
"""
from __future__ import annotations

import json
import sys

import pandas as pd

from .events import delivery, load
from .processor import Processor
from .scoring import train

TOPIC, BROKER, DATA = "card_transactions", "localhost:9092", "data/raw/creditcard.pq"


def produce() -> None:
    from confluent_kafka import Producer

    tx = load(DATA)
    p = Producer({"bootstrap.servers": BROKER, "enable.idempotence": False})
    for row in delivery(tx).itertuples():
        p.produce(TOPIC, key=str(row.txn_id), value=json.dumps({"txn_id": int(row.txn_id), "event_seconds": row.event_seconds}))
        p.poll(0)
    p.flush()


def consume(batch: int = 500) -> None:
    from confluent_kafka import Consumer

    tx = load(DATA)
    proc = Processor(train(tx[tx["day"] == 1]))
    indexed = tx.set_index("txn_id")
    c = Consumer({"bootstrap.servers": BROKER, "group.id": "fraud-scoring", "enable.auto.commit": False,
                  "auto.offset.reset": "earliest"})
    c.subscribe([TOPIC])
    try:
        while True:
            msgs = [m for m in c.consume(batch, timeout=1.0) if m.error() is None]
            if not msgs:
                continue
            rows = [json.loads(m.value()) for m in msgs]
            frame = pd.DataFrame(rows).assign(arrival_seconds=lambda d: d["event_seconds"])
            proc.process(frame, indexed)
            c.commit(asynchronous=False)
    finally:
        c.close()


if __name__ == "__main__":
    {"produce": produce, "consume": consume}[sys.argv[1]]()
