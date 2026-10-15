"""Stream processing: positions in, movement, line speeds, hotspots and bunching out.

    zet.positions ─ keyed by vehicle ─▶ speed and heading (state: the vehicle's last fix)
                                          ├─▶ zet.movements
                                          ├─▶ zet.vehicles (compacted: latest per vehicle)
                                          ├─ regroup by line ─┬─▶ bunching (state: the line's vehicles) ─▶ zet.alerts
                                          │                   └─▶ 5-minute windows ─▶ zet.line-speed
                                          └─ regroup by grid cell ─▶ 10-minute windows ─▶ zet.hotspots

Windows use event time (when a vehicle reported), with a grace period for positions
that arrive late. State lives in RocksDB and is backed by changelog topics, so a
restarted processor picks up where it left off; with exactly-once processing, its
output and its consumer offsets are committed in one Kafka transaction.
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta

from confluent_kafka import ConsumerGroupState
from confluent_kafka.admin import AdminClient
from quixstreams import Application
from quixstreams.dataframe.windows import Collect, Count, Max, Mean
from quixstreams.models import TopicConfig

from . import bunching, motion, topics
from .geo import cell_center

log = logging.getLogger("tramvaj")

LINE_WINDOW = timedelta(minutes=5)
CELL_WINDOW = timedelta(minutes=10)
# How long a window waits for late events after its end. After regrouping by line,
# events arrive in the order their source partitions are read: when one is ahead (a
# backlog after a restart, a fast replay), the others' events look late. Live, a minute
# is plenty and results come out a minute after each window; a fast replay needs more.
GRACE = timedelta(seconds=60)
MIN_HOTSPOT_SAMPLES = 6
GROUP = "tramvaj"
# Windows close when time on their partition passes end + grace, not only when the same key
# (line, grid cell) shows up again. With Quix's default, per key, a cell's window stayed open
# until a tram next drove through that cell, at night until morning.
CLOSING = "partition"


def event_time(value, headers, timestamp, timestamp_type) -> int:
    return int(value["ts"]) * 1000


def track(position: dict, state) -> dict | None:
    movement, new_state = motion.step(state.get("last"), position)
    state.set("last", new_state)
    return movement


def check_bunching(movement: dict, state) -> list[dict]:
    alerts, new_state = bunching.check(state.get("line"), movement)
    state.set("line", new_state)
    return alerts


def line_window(value: dict, key, timestamp, headers) -> dict:
    return {
        "route_id": key,
        "start": value["start"],
        "end": value["end"],
        "mean_kmh": round(value["mean_kmh"], 1) if value["mean_kmh"] is not None else None,
        "max_kmh": value["max_kmh"],
        "stopped_share": round(value["stopped"] / value["samples"], 3) if value["samples"] else None,
        "samples": value["samples"],
        "vehicles": len(set(value["vehicles"])),
    }


def cell_window(value: dict, key, timestamp, headers) -> dict:
    lat, lon = cell_center(key)
    return {
        "cell": key,
        "lat": lat,
        "lon": lon,
        "start": value["start"],
        "end": value["end"],
        "mean_kmh": round(value["mean_kmh"], 1),
        "samples": value["samples"],
        "vehicles": len(set(value["vehicles"])),
        "routes": sorted(set(value["routes"])),
    }


def late(value, key, timestamp_ms, late_by_ms, start, end, name, topic, partition, offset) -> bool:
    """Count late events and log every hundredth, instead of a warning for each one."""
    late.count += 1
    if late.count % 100 == 1:
        log.warning("%d events too late for their window so far (latest: %s, %d s late)", late.count, name, late_by_ms // 1000)
    return False


late.count = 0


def build(app: Application, grace: timedelta = GRACE) -> None:
    def out(spec: topics.TopicSpec):
        config = TopicConfig(num_partitions=spec.partitions, replication_factor=1, extra_config=spec.config)
        return app.topic(spec.name, key_serializer="str", value_serializer="json", config=config)

    positions = app.topic(topics.POSITIONS.name, key_deserializer="str", value_deserializer="json", timestamp_extractor=event_time)
    sdf = app.dataframe(positions)

    moves = sdf.apply(track, stateful=True).filter(lambda m: m is not None)
    moves.to_topic(out(topics.MOVEMENTS))
    moves.to_topic(out(topics.VEHICLES))

    by_line = moves.group_by("route_id", name="by-line")
    by_line.apply(check_bunching, stateful=True, expand=True).to_topic(out(topics.ALERTS))
    (
        by_line.tumbling_window(LINE_WINDOW, grace_ms=grace, on_late=late)
        .agg(
            mean_kmh=Mean("speed_kmh"),
            max_kmh=Max("speed_kmh"),
            stopped=_count_stopped(),
            samples=Count(),
            vehicles=Collect("vehicle_id"),
        )
        .final(closing_strategy=CLOSING)
        .apply(line_window, metadata=True)
        .to_topic(out(topics.LINE_SPEED))
    )

    by_cell = moves.filter(lambda m: m["tram"]).group_by("cell", name="by-cell")
    (
        by_cell.tumbling_window(CELL_WINDOW, grace_ms=grace, on_late=late)
        .agg(mean_kmh=Mean("speed_kmh"), samples=Count(), vehicles=Collect("vehicle_id"), routes=Collect("route_id"))
        .final(closing_strategy=CLOSING)
        .filter(lambda w: w["samples"] >= MIN_HOTSPOT_SAMPLES)
        .apply(cell_window, metadata=True)
        .to_topic(out(topics.HOTSPOTS))
    )


def _count_stopped():
    from quixstreams.dataframe.windows import Sum

    class StoppedCount(Sum):
        """How many samples in the window were standing still."""

        def agg(self, old, new, timestamp):
            return old + (1 if new.get("stopped") else 0)

    return StoppedCount()


def make_app(broker: str, group: str = GROUP, state_dir: str = "state", exactly_once: bool = True) -> Application:
    return Application(
        broker_address=broker,
        consumer_group=group,
        auto_offset_reset="earliest",
        state_dir=state_dir,
        processing_guarantee="exactly-once" if exactly_once else "at-least-once",
        loglevel="WARNING",
    )


def run(broker: str, grace: timedelta = GRACE, **kwargs) -> None:
    app = make_app(broker, **kwargs)
    build(app, grace)
    app.run()


def wait_until_running(broker: str, group: str = GROUP, timeout_s: float = 120) -> None:
    """Block until the processor's consumer group is stable and has the positions topic assigned.

    Positions produced before that pile up, and the processor then reads the backlog one
    partition at a time. After regrouping by line, one partition's events arrive far ahead
    of another's, the windows' grace runs out and the late ones are dropped. In CI the
    processor took about 15 seconds to join; a fixed sleep before producing was not enough.
    """
    admin = AdminClient({"bootstrap.servers": broker})
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        description = admin.describe_consumer_groups([group])[group].result()
        assigned = {tp.topic for member in description.members for tp in member.assignment.topic_partitions}
        if description.state == ConsumerGroupState.STABLE and topics.POSITIONS.name in assigned:
            return
        time.sleep(1)
    raise TimeoutError(f"processor group {group!r} did not start consuming {topics.POSITIONS.name} in {timeout_s} s")
