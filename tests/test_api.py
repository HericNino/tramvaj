from fastapi.testclient import TestClient

from tramvaj import topics
from tramvaj.api import STALE_S, LiveState, create_app


def vehicle(vid, ts, tram=True, route="6"):
    return {"vehicle_id": vid, "route_id": route, "tram": tram, "lat": 45.81, "lon": 15.97, "ts": ts, "speed_kmh": 12.0}


def window(route, start, mean):
    return {"route_id": route, "start": start, "end": start + 300_000, "mean_kmh": mean, "vehicles": 3}


def hotspot(cell, start):
    return {"cell": cell, "start": start, "end": start + 600_000, "mean_kmh": 4.0}


def test_newest_position_wins_even_when_an_older_one_arrives_later():
    state = LiveState()
    state.apply(topics.VEHICLES.name, "2201", vehicle("2201", 1000))
    state.apply(topics.VEHICLES.name, "2201", vehicle("2201", 990))
    assert state.vehicles["2201"]["ts"] == 1000
    assert state.version == 1


def test_stale_vehicles_and_buses_can_be_left_out():
    state = LiveState()
    state.apply(topics.VEHICLES.name, "old", vehicle("old", 1000))
    state.apply(topics.VEHICLES.name, "bus", vehicle("bus", 1000 + STALE_S, tram=False, route="280"))
    state.apply(topics.VEHICLES.name, "tram", vehicle("tram", 1001 + STALE_S))
    assert {v["vehicle_id"] for v in state.snapshot()["vehicles"]} == {"bus", "tram"}
    assert {v["vehicle_id"] for v in state.snapshot(trams_only=True)["vehicles"]} == {"tram"}


def test_changes_are_tracked_by_version_not_by_feed_time():
    state = LiveState()
    state.apply(topics.VEHICLES.name, "a", vehicle("a", 1000))
    version = state.snapshot()["version"]
    # b reports a moment that is older than a's, but it is still news to the map
    state.apply(topics.VEHICLES.name, "b", vehicle("b", 995))
    new_version, clock, changed = state.changed_since(version)
    assert [v["vehicle_id"] for v in changed] == ["b"]
    assert clock == 1000
    assert state.changed_since(new_version)[2] == []


def test_line_history_keeps_windows_in_order_and_skips_repeats():
    state = LiveState()
    for start, mean in [(0, 10.0), (300_000, 12.0), (300_000, 12.0), (0, 10.0)]:
        state.apply(topics.LINE_SPEED.name, "6", window("6", start, mean))
    assert [w["mean_kmh"] for w in state.snapshot()["lines"]["6"]] == [10.0, 12.0]


def test_only_the_newest_hotspot_window_is_shown():
    state = LiveState()
    state.apply(topics.HOTSPOTS.name, "1:1", hotspot("1:1", 0))
    state.apply(topics.HOTSPOTS.name, "1:2", hotspot("1:2", 600_000))
    assert [h["cell"] for h in state.snapshot()["hotspots"]] == ["1:2"]


def test_alerts_newest_first():
    state = LiveState()
    for n in range(3):
        state.apply(topics.ALERTS.name, "6", {"route_id": "6", "ts": n})
    assert [a["ts"] for a in state.snapshot()["alerts"]] == [2, 1, 0]


def test_http_snapshot():
    app = create_app("unused:9092", start_consumer=False)
    app.state.live.apply(topics.VEHICLES.name, "2201", vehicle("2201", 1000))
    with TestClient(app) as client:
        live = client.get("/api/live").json()
        assert live["clock"] == 1000
        assert live["vehicles"][0]["vehicle_id"] == "2201"
        assert client.get("/api/health").json()["vehicles"] == 1
