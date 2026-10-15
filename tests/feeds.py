"""Building GTFS-Realtime messages for tests, shaped like ZET's."""

from __future__ import annotations

from google.transit import gtfs_realtime_pb2

# Ilica, heading east towards the main square: about 75 m per 0.001 degrees of longitude
ILICA = (45.8128, 15.9600)


def feed(ts: int, vehicles: list[tuple[str, str, float, float]], delays: list[tuple[str, str, int]] = ()) -> bytes:
    """vehicles: (vehicle_id, route_id, lat, lon); delays: (trip_id, route_id, delay_s)."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "1.0"
    message.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    message.header.timestamp = ts
    for vehicle_id, route_id, lat, lon in vehicles:
        entity = message.entity.add()
        entity.id = f"X_{vehicle_id}"
        v = entity.vehicle
        v.trip.trip_id = f"0_24_{route_id}_{vehicle_id}"
        v.trip.route_id = route_id
        v.position.latitude = lat
        v.position.longitude = lon
        v.timestamp = ts - 1
        v.vehicle.id = vehicle_id
    for trip_id, route_id, delay in delays:
        entity = message.entity.add()
        entity.id = f"T_{trip_id}"
        t = entity.trip_update
        t.trip.trip_id = trip_id
        t.trip.route_id = route_id
        update = t.stop_time_update.add()
        update.stop_sequence = 7
        update.stop_id = "221_1"
        update.arrival.delay = delay
        t.timestamp = ts
    return message.SerializeToString()


def ilica(ts: int, offset_m: float) -> tuple[float, float]:
    """A point on Ilica, offset_m metres east of the start."""
    return ILICA[0], ILICA[1] + offset_m / 77_600
