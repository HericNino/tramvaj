"""Turning a vehicle's consecutive positions into movement: speed and heading.

This is the stateful step: it needs the vehicle's previous position, so the stream is
keyed by vehicle and the last position is kept in state, per vehicle.
"""

from __future__ import annotations

from .feed import is_tram
from .geo import cell, distance_m, heading_deg

MIN_GAP_S = 5  # closer reports say nothing reliable about speed
MAX_GAP_S = 300  # after a longer silence the vehicle could have been anywhere
MAX_KMH = 90  # trams top out around 70 km/h; faster means a bad GPS fix
STOPPED_KMH = 3
MIN_MOVE_M = 8  # below this, GPS jitter decides the heading


def step(previous: dict | None, position: dict) -> tuple[dict | None, dict]:
    """(movement event or None, state to keep).

    A position older than or equal to the one in state is a duplicate or out of order
    and changes nothing. A position after a long gap starts over without a speed.
    """
    if previous is not None and position["ts"] <= previous["ts"]:
        return None, previous

    state = {
        "ts": position["ts"],
        "lat": position["lat"],
        "lon": position["lon"],
        "heading": previous.get("heading") if previous else None,
    }
    if previous is None or position["route_id"] != previous.get("route_id"):
        state["route_id"] = position["route_id"]
        return None, state | {"heading": None}
    state["route_id"] = position["route_id"]

    gap = position["ts"] - previous["ts"]
    if gap < MIN_GAP_S:
        return None, previous  # keep the older point as the base for the next speed
    if gap > MAX_GAP_S:
        return None, state

    metres = distance_m(previous["lat"], previous["lon"], position["lat"], position["lon"])
    kmh = metres / gap * 3.6
    if kmh > MAX_KMH:
        return None, previous  # a jump: ignore this fix, keep the last good one
    if metres >= MIN_MOVE_M:
        state["heading"] = round(heading_deg(previous["lat"], previous["lon"], position["lat"], position["lon"]))

    movement = {
        "vehicle_id": position["vehicle_id"],
        "route_id": position["route_id"],
        "trip_id": position["trip_id"],
        "tram": is_tram(position["route_id"]),
        "lat": position["lat"],
        "lon": position["lon"],
        "ts": position["ts"],
        "speed_kmh": round(kmh, 1),
        "stopped": kmh < STOPPED_KMH,
        "heading": state["heading"],
        "cell": cell(position["lat"], position["lon"]),
    }
    return movement, state
