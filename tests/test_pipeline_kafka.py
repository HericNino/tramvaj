"""The whole pipeline against a real broker: synthetic feeds in, every output topic checked.

Runs when KAFKA_BROKER is set (CI starts Kafka as a service container):
    docker run -d -p 9092:9092 apache/kafka:3.9.1
    KAFKA_BROKER=localhost:9092 uv run pytest -m kafka
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import uuid

import pytest
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from feeds import feed, ilica

from tramvaj import topics
from tramvaj.processor import wait_until_running
from tramvaj.producer import FeedProducer

BROKER = os.environ.get("KAFKA_BROKER")
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not BROKER, reason="KAFKA_BROKER not set")]

# Recent, on an hour (and so a window) boundary. Kafka deletes by message timestamp, which
# is event time here: data from a fixed day in the past would be past retention and could be
# deleted while the test runs.
START = int(time.time()) // 3600 * 3600 - 3600
LATER = START + 2700


def snapshots():
    """15 minutes of line 6 on Ilica, a snapshot every 15 s, then two late ones.

    Trams 2201 and 2202 run 100 m apart at 18 km/h (bunched); 2203 is 1 km ahead;
    bus 280 is on its own line. One unreadable snapshot is mixed in. The last two
    snapshots, 45 minutes on, move event time past every window and its grace, the way
    the next morning's data would.
    """
    for i in range(61):
        ts = START + i * 15
        moved = i * 75
        vehicles = [
            ("2201", "6", *ilica(ts, moved)),
            ("2202", "6", *ilica(ts, moved + 100)),
            ("2203", "6", *ilica(ts, moved + 1000)),
            ("280", "280", 45.80, 16.00 + i * 0.0001),
        ]
        yield feed(ts + 1, vehicles, [("trip-6", "6", 90)] if i % 4 == 0 else [])
        if i == 30:
            yield b"<html>502 Bad Gateway</html>"
    for j in range(2):
        ts = LATER + j * 15
        # forty trams along Ilica, so every partition of the regrouped streams sees time move on
        yield feed(ts + 1, [(f"3{n:03d}", "6", *ilica(ts, n * 125 + j * 75)) for n in range(40)])


def read_all(topic: str, timeout: float = 5) -> list[tuple[str | None, dict]]:
    """Every committed message in the topic. Partitions are assigned directly, with no
    consumer group to join, so the idle timeout only counts time spent waiting for data."""
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "unused", "isolation.level": "read_committed"})
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in partitions])
    out, idle = [], 0.0
    while idle < timeout:
        message = consumer.poll(1.0)
        if message is None:
            idle += 1
            continue
        if message.error():
            continue
        idle = 0.0
        key = message.key().decode() if message.key() else None
        out.append((key, json.loads(message.value())))
    consumer.close()
    return out


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory):
    """Producer and processor running side by side, the feed played about 75 times faster
    than real time (a snapshot every 0.2 s for every 15 s of feed time)."""
    suffix = f"-{uuid.uuid4().hex[:6]}"  # fresh topics: reruns don't see old messages
    env = os.environ | {"TRAMVAJ_TOPIC_SUFFIX": suffix, "KAFKA_BROKER": BROKER}
    topics.SUFFIX = suffix
    topics.ensure(BROKER)

    state = tmp_path_factory.mktemp("state")
    group = f"test{suffix}"
    command = [sys.executable, "-m", "tramvaj.cli", "process", "--grace", "120", "--state-dir", str(state), "--group", group]
    processor = subprocess.Popen(command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        wait_until_running(BROKER, group)  # see wait_until_running: producing earlier makes a backlog
        producer = FeedProducer.connect(BROKER)
        for data in snapshots():
            producer.send(data)
            producer.producer.flush(5)
            time.sleep(0.2)
        # wait until the processor has turned every position into a movement
        expected = producer.stats.positions - len(producer.last_seen)
        deadline = time.monotonic() + 240
        while len(read_all(topics.MOVEMENTS.name, 2)) < expected and time.monotonic() < deadline:
            time.sleep(3)
        time.sleep(10)  # and written the windows that closed with the last events
    finally:
        processor.send_signal(signal.SIGTERM)
        processor.wait(30)
    return producer.stats


def test_producer_sends_changes_only(pipeline):
    assert pipeline.snapshots == 64
    assert pipeline.unreadable == 1
    assert pipeline.positions == 61 * 4 + 2 * 40
    assert len(read_all(topics.DLQ.name, 3)) == 1


def test_movements_have_speed(pipeline):
    moves = read_all(topics.MOVEMENTS.name)
    assert len(moves) == 60 * 4 + 40  # every position but each vehicle's first
    tram = [v for k, v in moves if k == "2201"]
    assert {round(v["speed_kmh"]) for v in tram} == {18}
    assert all(v["heading"] == 90 for v in tram)


def test_latest_vehicle_state_is_keyed_by_vehicle(pipeline):
    latest = {k: v for k, v in read_all(topics.VEHICLES.name)}
    assert {"2201", "2202", "2203", "280"} <= set(latest)
    assert latest["2201"]["ts"] == START + 60 * 15


def test_bunching_alert_for_the_close_pair_only(pipeline):
    alerts = [(k, v) for k, v in read_all(topics.ALERTS.name) if v["ts"] < LATER]  # the clock's trams are close by design
    assert {tuple(v["vehicles"]) for k, v in alerts} == {("2201", "2202")}
    assert all(k == "6" for k, _ in alerts)
    assert all(95 <= v["distance_m"] <= 105 for _, v in alerts)  # 100 m, measured at the same moment
    assert len(alerts) == 2  # once, and again after the 10 minute cooldown


def test_line_speed_windows(pipeline):
    windows = sorted((v for k, v in read_all(topics.LINE_SPEED.name) if k == "6" and v["start"] < LATER * 1000), key=lambda w: w["start"])
    # four 5-minute windows: 0-5, 5-10, 10-15, and 15-20 holding the last positions
    assert [w["start"] for w in windows] == [(START + n * 300) * 1000 for n in range(4)]
    assert all(round(w["mean_kmh"]) == 18 and w["vehicles"] == 3 and w["stopped_share"] == 0 for w in windows)


def test_hotspots_are_trams_only(pipeline):
    cells = [v for _, v in read_all(topics.HOTSPOTS.name) if v["start"] < LATER * 1000]
    assert len(cells) >= 10  # Ilica, cell by cell
    assert all(v["routes"] == ["6"] and round(v["mean_kmh"]) == 18 for v in cells)
    assert all(v["vehicles"] <= 3 for v in cells)
