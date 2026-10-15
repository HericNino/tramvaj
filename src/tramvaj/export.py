"""The processor's output, saved as one JSON file the map can play back without a server.

The live map needs Kafka and `tramvaj serve` running. For a static copy (GitHub Pages), a
recorded hour goes through the same pipeline in CI and this reads everything it produced:

    {"start": ..., "end": ...,                      unix seconds, the span of the movements
     "tracks": [{"v": "2201", "r": "6", "tram": 1,  one per vehicle and line
                 "p": [[dt, lat, lon, kmh, heading], ...]}],   dt = seconds after start
     "lines": [...], "hotspots": [...], "alerts": [...]}       as the processor wrote them

A vehicle that changes line starts a new track. Heading is -1 when unknown.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition

from . import topics

log = logging.getLogger("tramvaj")


def read_topic(broker: str, topic: str, idle_s: float = 5.0) -> list[dict]:
    """Every committed message in a topic, oldest first within each partition."""
    consumer = Consumer({"bootstrap.servers": broker, "group.id": f"tramvaj-export-{uuid.uuid4()}", "isolation.level": "read_committed"})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
        values, idle = [], 0.0
        while idle < idle_s:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1
                continue
            if message.error():
                continue
            idle = 0.0
            values.append(json.loads(message.value()))
        return values
    finally:
        consumer.close()


def tracks(movements: list[dict]) -> tuple[int, int, list[dict]]:
    """(start, end, tracks) from movement events."""
    if not movements:
        return 0, 0, []
    movements = sorted(movements, key=lambda m: (m["vehicle_id"], m["ts"]))
    start = min(m["ts"] for m in movements)
    end = max(m["ts"] for m in movements)
    out: list[dict] = []
    current = None
    for m in movements:
        if current is None or current["v"] != m["vehicle_id"] or current["r"] != m["route_id"]:
            current = {"v": m["vehicle_id"], "r": m["route_id"], "tram": int(bool(m["tram"])), "p": []}
            out.append(current)
        heading = -1 if m["heading"] is None else round(m["heading"])
        current["p"].append([m["ts"] - start, round(m["lat"], 5), round(m["lon"], 5), round(m["speed_kmh"]), heading])
    return start, end, out


def settle(broker: str, quiet_s: float = 20, timeout_s: float = 900) -> list[dict]:
    """Wait until the processor has caught up: the movements topic stops growing for quiet_s."""
    deadline = time.monotonic() + timeout_s
    movements = read_topic(broker, topics.MOVEMENTS.name)
    while time.monotonic() < deadline:
        time.sleep(quiet_s)
        again = read_topic(broker, topics.MOVEMENTS.name)
        if len(again) == len(movements):
            return again
        log.info("processor still going: %d movements", len(again))
        movements = again
    raise TimeoutError("the movements topic kept growing")


def export(broker: str, out: Path, wait: bool = False) -> dict:
    movements = settle(broker) if wait else read_topic(broker, topics.MOVEMENTS.name)
    start, end, vehicle_tracks = tracks(movements)
    data = {
        "start": start,
        "end": end,
        "tracks": vehicle_tracks,
        "lines": sorted(read_topic(broker, topics.LINE_SPEED.name), key=lambda w: (w["route_id"], w["start"])),
        "hotspots": sorted(read_topic(broker, topics.HOTSPOTS.name), key=lambda h: (h["start"], h["cell"])),
        "alerts": sorted(read_topic(broker, topics.ALERTS.name), key=lambda a: a["ts"]),
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, separators=(",", ":")))
    return {
        "movements": len(movements),
        "tracks": len(vehicle_tracks),
        "lines": len(data["lines"]),
        "hotspots": len(data["hotspots"]),
        "alerts": len(data["alerts"]),
        "bytes": out.stat().st_size,
    }
