"""Feed snapshots into Kafka.

Each snapshot holds the whole network, most of it unchanged since the last one: a
vehicle that hasn't reported again still has its old timestamp. Only positions newer
than the last one sent for that vehicle are produced, so a vehicle's stream has no
duplicates and Kafka carries changes, not copies.

The message timestamp is the vehicle's own report time (event time), not the time the
feed was read, so windows downstream group by when things happened.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field

from confluent_kafka import Producer

from . import topics
from .feed import FeedError, decode

log = logging.getLogger("tramvaj")


@dataclass
class Stats:
    snapshots: int = 0
    positions: int = 0
    skipped: int = 0  # unchanged since the previous snapshot
    delays: int = 0
    unreadable: int = 0


@dataclass
class FeedProducer:
    producer: Producer
    last_seen: dict[str, int] = field(default_factory=dict)  # vehicle -> newest ts sent
    last_delay: dict[str, tuple[int, int]] = field(default_factory=dict)  # trip -> (ts, delay)
    stats: Stats = field(default_factory=Stats)

    @classmethod
    def connect(cls, broker: str) -> FeedProducer:
        return cls(
            Producer(
                {
                    "bootstrap.servers": broker,
                    "enable.idempotence": True,  # retries never write a message twice
                    "acks": "all",
                    "linger.ms": 50,
                    "compression.type": "zstd",
                }
            )
        )

    def send(self, data: bytes) -> None:
        self.stats.snapshots += 1
        try:
            snapshot = decode(data)
        except FeedError as error:
            # keep the raw bytes: a changed feed format is a debugging job for later
            self.stats.unreadable += 1
            self.producer.produce(topics.DLQ.name, value=json.dumps({"error": str(error), "raw": base64.b64encode(data[:4096]).decode()}))
            log.warning("unreadable snapshot sent to %s: %s", topics.DLQ.name, error)
            return

        for p in snapshot.positions:
            if p.ts <= self.last_seen.get(p.vehicle_id, 0):
                self.stats.skipped += 1
                continue
            self.last_seen[p.vehicle_id] = p.ts
            self.producer.produce(topics.POSITIONS.name, key=p.vehicle_id, value=json.dumps(p.to_dict()), timestamp=p.ts * 1000)
            self.stats.positions += 1

        for d in snapshot.delays:
            if self.last_delay.get(d.trip_id) == (d.ts, d.delay_s):
                continue
            self.last_delay[d.trip_id] = (d.ts, d.delay_s)
            self.producer.produce(topics.DELAYS.name, key=d.trip_id, value=json.dumps(d.to_dict()), timestamp=d.ts * 1000)
            self.stats.delays += 1
        self.producer.poll(0)

    def run(self, snapshots: Iterable[bytes], log_every: int = 20) -> Stats:
        for i, data in enumerate(snapshots, start=1):
            self.send(data)
            if i % log_every == 0:
                log.info("%d snapshots: %d positions sent, %d unchanged skipped", i, self.stats.positions, self.stats.skipped)
        self.producer.flush(30)
        return self.stats
