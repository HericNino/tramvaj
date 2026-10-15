"""Bunching: two vehicles of the same line, going the same way, right behind each other.

The classic public transport failure: one tram runs late, gathers the passengers of the
one behind it, gets later still, and the one behind catches up. Passengers see a 15
minute gap and then two trams at once. Detecting it needs every vehicle of a line in
one place, so the stream is regrouped by line and the line's vehicles kept in state.
"""

from __future__ import annotations

from .geo import angle_between, distance_m, move

CLOSE_M = 250
SAME_WAY_DEG = 45
SAME_TIME_S = 30  # only positions this close in time are compared, after projecting
KEEP_S = 300  # a vehicle not heard from for longer is dropped from the line's state
COOLDOWN_S = 600  # report a pair at most every 10 minutes

# Events of different vehicles reach this step in partition order, not time order: after
# a restart or a replay, one tram's whole morning can arrive before another's. So every
# comparison checks event times, in both directions, instead of trusting arrival order.


def check(line_state: dict | None, movement: dict) -> tuple[list[dict], dict]:
    """(alerts, new line state). State: {"vehicles": {id: {...}}, "reported": {pair: ts}}."""
    state = line_state or {"vehicles": {}, "reported": {}}
    now = movement["ts"]
    vehicles = {vid: v for vid, v in state["vehicles"].items() if now - v["ts"] <= KEEP_S}
    reported = {pair: ts for pair, ts in state["reported"].items() if abs(now - ts) < COOLDOWN_S}

    alerts = []
    me = movement
    if me["heading"] is not None and not me["stopped"]:
        for vid, other in vehicles.items():
            if vid == me["vehicle_id"] or abs(now - other["ts"]) > SAME_TIME_S:
                continue
            if other["heading"] is None or angle_between(me["heading"], other["heading"]) > SAME_WAY_DEG:
                continue
            # where the other vehicle was (or will be) at this event's time, at its last speed
            shift_m = other["speed_kmh"] / 3.6 * (now - other["ts"])
            lat, lon = move(other["lat"], other["lon"], other["heading"], shift_m)
            gap = distance_m(me["lat"], me["lon"], lat, lon)
            if gap > CLOSE_M:
                continue
            pair = "|".join(sorted((me["vehicle_id"], vid)))
            if pair in reported:
                continue
            reported[pair] = now
            alerts.append(
                {
                    "type": "bunching",
                    "route_id": me["route_id"],
                    "vehicles": sorted((me["vehicle_id"], vid)),
                    "distance_m": round(gap),
                    "lat": round((me["lat"] + lat) / 2, 6),
                    "lon": round((me["lon"] + lon) / 2, 6),
                    "ts": now,
                }
            )

    if me["vehicle_id"] not in vehicles or vehicles[me["vehicle_id"]]["ts"] < now:
        vehicles[me["vehicle_id"]] = {k: me[k] for k in ("lat", "lon", "heading", "ts", "stopped", "speed_kmh")}
    return alerts, {"vehicles": vehicles, "reported": reported}
