"""The Kafka topics and why each is set up the way it is.

    zet.positions     raw vehicle positions          key: vehicle   delete after 3 days
    zet.delays        delay at the next stop         key: trip      delete after 3 days
    zet.movements     positions with speed, heading  key: vehicle   delete after 3 days
    zet.vehicles      latest state of each vehicle   key: vehicle   compacted
    zet.line-speed    5-minute averages per line     key: line      delete after 30 days
    zet.hotspots      10-minute averages per area    key: cell      delete after 30 days
    zet.alerts        bunching                       key: line      delete after 30 days
    zet.dlq           feeds that could not be read   no key        delete after 7 days

Keying by vehicle puts all of a vehicle's positions in one partition, in order, which
the speed calculation needs. zet.vehicles is compacted: Kafka keeps only the newest
message per key, so a new consumer reading it from the start gets the current state of
every vehicle without replaying the whole history.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from confluent_kafka.admin import AdminClient, NewTopic

DAY_MS = 24 * 3600 * 1000
# appended to every name, to run several copies on one broker (tests, staging)
SUFFIX = os.environ.get("TRAMVAJ_TOPIC_SUFFIX", "")


@dataclass(frozen=True)
class TopicSpec:
    base: str
    partitions: int = 6
    config: dict = field(default_factory=dict)

    @property
    def name(self) -> str:
        return self.base + SUFFIX


POSITIONS = TopicSpec("zet.positions", config={"retention.ms": str(3 * DAY_MS)})
DELAYS = TopicSpec("zet.delays", config={"retention.ms": str(3 * DAY_MS)})
MOVEMENTS = TopicSpec("zet.movements", config={"retention.ms": str(3 * DAY_MS)})
VEHICLES = TopicSpec("zet.vehicles", config={"cleanup.policy": "compact", "min.cleanable.dirty.ratio": "0.1", "segment.ms": "600000"})
LINE_SPEED = TopicSpec("zet.line-speed", partitions=3, config={"retention.ms": str(30 * DAY_MS)})
HOTSPOTS = TopicSpec("zet.hotspots", partitions=3, config={"retention.ms": str(30 * DAY_MS)})
ALERTS = TopicSpec("zet.alerts", partitions=3, config={"retention.ms": str(30 * DAY_MS)})
DLQ = TopicSpec("zet.dlq", partitions=1, config={"retention.ms": str(7 * DAY_MS)})

ALL = (POSITIONS, DELAYS, MOVEMENTS, VEHICLES, LINE_SPEED, HOTSPOTS, ALERTS, DLQ)


def ensure(broker: str, specs=ALL, replication: int = 1) -> list[str]:
    """Create missing topics. Returns the names created; existing topics are left alone."""
    admin = AdminClient({"bootstrap.servers": broker})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [
        NewTopic(s.name, num_partitions=s.partitions, replication_factor=replication, config=s.config)
        for s in specs
        if s.name not in existing
    ]
    if not new:
        return []
    for future in admin.create_topics(new).values():
        try:
            future.result()
        except Exception as error:  # created meanwhile by another service
            if "TOPIC_ALREADY_EXISTS" not in str(error):
                raise
    return [t.topic for t in new]
