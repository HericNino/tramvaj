"""The live map's backend: reads the processor's output topics and serves them.

A background thread consumes the output topics into memory. zet.vehicles is compacted,
so reading it from the start gives the current state of every vehicle at once; the
windowed topics are read from the start too and only their newest windows are kept.
The browser gets a snapshot over HTTP and then changes over Server-Sent Events.

    uvicorn tramvaj.api:create_app --factory
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import topics

log = logging.getLogger("tramvaj")
SITE = Path(os.environ.get("TRAMVAJ_SITE", Path(__file__).resolve().parents[2] / "site"))
STALE_S = 180  # vehicles not heard from for this long (in feed time) are off the map
HISTORY = 24  # windows of line speed kept per line: 2 hours


class LiveState:
    """Everything the map shows, updated by the consumer thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.vehicles: dict[str, dict] = {}
        self.changed_at: dict[str, int] = {}  # vehicle -> version of its last change
        self.lines: dict[str, deque] = defaultdict(lambda: deque(maxlen=HISTORY))
        self.hotspots: dict[str, dict] = {}
        self.alerts: deque = deque(maxlen=50)
        self.clock = 0  # newest event time seen, unix seconds: "now" for the feed
        self.version = 0  # bumped on every vehicle change; the event stream sends what changed after a version

    def apply(self, topic: str, key: str | None, value: dict) -> None:
        with self.lock:
            if topic == topics.VEHICLES.name:
                if key not in self.vehicles or self.vehicles[key]["ts"] < value["ts"]:
                    self.version += 1
                    self.vehicles[key] = value
                    self.changed_at[key] = self.version
                self.clock = max(self.clock, value["ts"])
            elif topic == topics.LINE_SPEED.name:
                history = self.lines[value["route_id"]]
                if not history or history[-1]["start"] < value["start"]:
                    history.append(value)
            elif topic == topics.HOTSPOTS.name:
                current = self.hotspots.get(value["cell"])
                if current is None or current["start"] <= value["start"]:
                    self.hotspots[value["cell"]] = value
            elif topic == topics.ALERTS.name:
                self.alerts.appendleft(value)

    def snapshot(self, trams_only: bool = False) -> dict:
        with self.lock:
            fresh = [v for v in self.vehicles.values() if self.clock - v["ts"] <= STALE_S and (v["tram"] or not trams_only)]
            newest_hotspot = max((h["end"] for h in self.hotspots.values()), default=0)
            return {
                "clock": self.clock,
                "version": self.version,
                "vehicles": fresh,
                "lines": {route: list(history) for route, history in self.lines.items()},
                "hotspots": [h for h in self.hotspots.values() if h["end"] == newest_hotspot],
                "alerts": list(self.alerts)[:20],
            }

    def changed_since(self, version: int) -> tuple[int, int, list[dict]]:
        """(version, clock, vehicles changed after the given version)."""
        with self.lock:
            changed = [self.vehicles[k] for k, v in self.changed_at.items() if v > version]
            return self.version, self.clock, changed


def consume(broker: str, state: LiveState, stop: threading.Event) -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"tramvaj-map-{uuid.uuid4()}",  # every instance reads everything
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([topics.VEHICLES.name, topics.LINE_SPEED.name, topics.HOTSPOTS.name, topics.ALERTS.name])
    try:
        while not stop.is_set():
            for message in consumer.consume(500, timeout=0.5):
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer: %s", message.error())
                    continue
                try:
                    value = json.loads(message.value())
                except (TypeError, ValueError):
                    continue
                key = message.key().decode() if message.key() else None
                state.apply(message.topic(), key, value)
    finally:
        consumer.close()


def create_app(broker: str | None = None, start_consumer: bool = True) -> FastAPI:
    broker = broker or os.environ.get("KAFKA_BROKER", "localhost:9092")
    state = LiveState()
    stop = threading.Event()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        thread = None
        if start_consumer:
            thread = threading.Thread(target=consume, args=(broker, state, stop), daemon=True)
            thread.start()
        yield
        stop.set()
        if thread:
            thread.join(5)

    app = FastAPI(title="tramvaj", lifespan=lifespan)
    app.state.live = state

    @app.get("/api/live")
    def live(trams_only: bool = False):
        return state.snapshot(trams_only)

    @app.get("/api/stream")
    async def stream(since: int | None = None, every: float = 2.0):
        """Server-Sent Events: vehicles that changed after version `since` (from /api/live), every couple of seconds."""

        async def events():
            version = state.version if since is None else since
            while True:
                new_version, clock, changed = state.changed_since(version)
                if changed:
                    version = new_version
                    yield f"data: {json.dumps({'version': version, 'clock': clock, 'vehicles': changed})}\n\n"
                else:
                    yield ": keep-alive\n\n"
                await asyncio.sleep(max(every, 0.2))

        return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    @app.get("/api/health")
    def health():
        return {"status": "ok", "vehicles": len(state.vehicles), "feed_time": state.clock, "lag_s": int(time.time()) - state.clock}

    if SITE.exists():
        app.mount("/", StaticFiles(directory=SITE, html=True), name="site")
    return app
