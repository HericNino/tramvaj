from feeds import ilica

from tramvaj.bunching import check


def movement(vehicle, ts, offset_m, heading=90, stopped=False, route="6", speed=18.0):
    lat, lon = ilica(ts, offset_m)
    return {
        "vehicle_id": vehicle,
        "route_id": route,
        "lat": lat,
        "lon": lon,
        "ts": ts,
        "heading": heading,
        "stopped": stopped,
        "speed_kmh": speed,
    }


def test_two_trams_close_and_same_way_are_bunched():
    alerts, state = check(None, movement("a", 0, 0))
    alerts, state = check(state, movement("b", 10, 170))  # a has moved on 50 m by now
    assert len(alerts) == 1
    assert alerts[0]["vehicles"] == ["a", "b"]
    assert alerts[0]["distance_m"] == 120


def test_opposite_directions_are_not_bunched():
    _, state = check(None, movement("a", 0, 0, heading=90))
    alerts, _ = check(state, movement("b", 10, 50, heading=270))
    assert alerts == []


def test_far_apart_is_fine():
    _, state = check(None, movement("a", 0, 0))
    alerts, _ = check(state, movement("b", 10, 800))
    assert alerts == []


def test_a_pair_is_reported_once_per_cooldown():
    _, state = check(None, movement("a", 0, 0))
    first, state = check(state, movement("b", 10, 100))
    again, state = check(state, movement("a", 20, 30))
    assert len(first) == 1 and again == []
    _, state = check(state, movement("b", 700, 100))
    later, _ = check(state, movement("a", 710, 30))
    assert len(later) == 1


def test_positions_from_different_moments_are_not_compared():
    _, state = check(None, movement("a", 0, 0))
    alerts, state = check(state, movement("b", 60, 50))
    assert alerts == []


def test_the_other_vehicle_is_projected_to_the_same_moment():
    # a was at 0 m ten seconds ago going 18 km/h, so it is at 50 m now; b at 150 m is 100 m ahead
    _, state = check(None, movement("a", 0, 0))
    alerts, _ = check(state, movement("b", 10, 150))
    assert alerts[0]["distance_m"] == 100


def test_stale_vehicles_are_forgotten():
    _, state = check(None, movement("a", 0, 0))
    _, state = check(state, movement("b", 400, 50))
    assert "a" not in state["vehicles"]


def test_out_of_order_arrival():
    # all of a's positions arrive before b's, as after a restart
    state = None
    for t in range(0, 300, 15):
        _, state = check(state, movement("a", t, t * 5))
    found = []
    for t in range(0, 300, 15):
        alerts, state = check(state, movement("b", t, t * 5 + 100))
        found += alerts
    assert len(found) == 1 and found[0]["distance_m"] == 100
