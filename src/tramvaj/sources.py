"""Where feed snapshots come from: ZET live, or files recorded earlier.

Recorded snapshots make the whole pipeline runnable without the network, and the same
morning can be replayed as many times as needed while working on the processing.
"""

from __future__ import annotations

import gzip
import logging
import time
from collections.abc import Iterator
from pathlib import Path

import httpx

from .feed import FEED_URL

log = logging.getLogger("tramvaj")


def live(url: str = FEED_URL, interval_s: float = 10.0, client: httpx.Client | None = None) -> Iterator[bytes]:
    """Poll the feed forever. Network errors are logged and retried on the next tick."""
    client = client or httpx.Client(timeout=10, headers={"User-Agent": "tramvaj (github.com/HericNino/tramvaj)"})
    while True:
        started = time.monotonic()
        try:
            response = client.get(url)
            response.raise_for_status()
            yield response.content
        except httpx.HTTPError as error:
            log.warning("feed unavailable: %s", error)
        time.sleep(max(0.0, interval_s - (time.monotonic() - started)))


def recorded(directory: Path, speed: float = 1.0, loop: bool = False, start: float | None = None) -> Iterator[bytes]:
    """Snapshots saved by `tramvaj record`, in order, spaced as they were recorded.

    speed=10 plays ten times faster; speed=0 plays as fast as possible. With loop, every
    pass is moved forward in time by the recording's length, so to the pipeline it looks
    like the feed simply carried on (positions that go back in time would be dropped).

    start moves the whole recording so that it begins at that unix time. Kafka's retention
    counts from each message's timestamp, which here is the time of the position: a
    recording from last month, replayed as it is, is past the topics' retention and the
    broker deletes it within minutes of it being written.
    """
    files = sorted(directory.glob("*.pb.gz"), key=lambda path: int(path.name.split(".")[0]))
    if not files:
        raise FileNotFoundError(f"no recorded snapshots (*.pb.gz) in {directory}")
    stamps = [int(path.name.split(".")[0]) for path in files]
    length = stamps[-1] - stamps[0] + (stamps[1] - stamps[0] if len(stamps) > 1 else 15)
    offset = 0 if start is None else int(start) - stamps[0]
    while True:
        previous = None
        for stamp, path in zip(stamps, files, strict=True):
            if previous is not None and speed > 0:
                time.sleep(max(0, stamp - previous) / speed)
            previous = stamp
            data = gzip.decompress(path.read_bytes())
            yield shift(data, offset) if offset else data
        if not loop:
            return
        offset += length


def shift(data: bytes, seconds: int) -> bytes:
    """The same snapshot, every timestamp in it moved by `seconds`."""
    from google.protobuf.message import DecodeError
    from google.transit import gtfs_realtime_pb2

    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(data)
    except DecodeError:
        return data
    message.header.timestamp += seconds
    for entity in message.entity:
        if entity.HasField("vehicle") and entity.vehicle.timestamp:
            entity.vehicle.timestamp += seconds
        if entity.HasField("trip_update") and entity.trip_update.timestamp:
            entity.trip_update.timestamp += seconds
    return message.SerializeToString()


def record(out: Path, minutes: float, interval_s: float = 15.0, url: str = FEED_URL) -> int:
    """Save live snapshots as <feed timestamp>.pb.gz. Returns how many were new."""
    from .feed import FeedError, decode

    out.mkdir(parents=True, exist_ok=True)
    saved = 0
    deadline = time.monotonic() + minutes * 60
    for data in live(url, interval_s):
        try:
            stamp = decode(data).ts
        except FeedError as error:
            log.warning("skipping unreadable snapshot: %s", error)
        else:
            path = out / f"{stamp}.pb.gz"
            if not path.exists():
                path.write_bytes(gzip.compress(data, mtime=0))
                saved += 1
        if time.monotonic() >= deadline:
            return saved
    return saved
