"""Decoding ZET's GTFS-Realtime feed.

ZET publishes one protobuf message (https://www.zet.hr/gtfs-rt-protobuf) with the
whole network: about 200 vehicle positions, refreshed every 15 seconds or so, and a
few trip updates with the delay at the next stop. Speed and bearing are not filled in,
so they are worked out downstream from consecutive positions.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

FEED_URL = "https://www.zet.hr/gtfs-rt-protobuf"


@dataclass(frozen=True)
class Position:
    vehicle_id: str
    route_id: str
    trip_id: str
    lat: float
    lon: float
    ts: int  # when the vehicle reported it, unix seconds (event time)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Delay:
    trip_id: str
    route_id: str
    stop_id: str
    stop_sequence: int
    delay_s: int  # positive: late
    ts: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Snapshot:
    ts: int  # when ZET generated the feed
    positions: list[Position]
    delays: list[Delay]


class FeedError(ValueError):
    pass


def decode(data: bytes) -> Snapshot:
    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(data)
    except DecodeError as error:
        raise FeedError(f"not a GTFS-Realtime message: {error}") from error
    if not message.HasField("header"):
        raise FeedError("feed has no header")

    positions: list[Position] = []
    delays: list[Delay] = []
    for entity in message.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            if not v.HasField("position") or not v.vehicle.id:
                continue
            lat, lon = v.position.latitude, v.position.longitude
            if not (45.0 < lat < 46.5 and 15.0 < lon < 17.0):  # outside the Zagreb area: a bad fix
                continue
            positions.append(
                Position(
                    vehicle_id=v.vehicle.id,
                    route_id=v.trip.route_id,
                    trip_id=v.trip.trip_id,
                    lat=round(lat, 6),
                    lon=round(lon, 6),
                    ts=int(v.timestamp or message.header.timestamp),
                )
            )
        if entity.HasField("trip_update"):
            t = entity.trip_update
            for update in t.stop_time_update[:1]:  # the next stop is the one that matters
                event = update.arrival if update.HasField("arrival") else update.departure
                delays.append(
                    Delay(
                        trip_id=t.trip.trip_id,
                        route_id=t.trip.route_id,
                        stop_id=update.stop_id,
                        stop_sequence=update.stop_sequence,
                        delay_s=int(event.delay),
                        ts=int(t.timestamp or message.header.timestamp),
                    )
                )
    return Snapshot(ts=int(message.header.timestamp), positions=positions, delays=delays)


def is_tram(route_id: str) -> bool:
    """ZET trams run lines 1 to 17 by day and 31 to 34 at night; everything else is a bus."""
    return route_id.isdigit() and (1 <= int(route_id) <= 17 or 31 <= int(route_id) <= 34)
