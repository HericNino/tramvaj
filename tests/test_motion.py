from feeds import ilica

from tramvaj.motion import step


def position(ts, offset_m, route="6", vehicle="2201"):
    lat, lon = ilica(ts, offset_m)
    return {"vehicle_id": vehicle, "route_id": route, "trip_id": "t", "lat": lat, "lon": lon, "ts": ts}


def run(*positions):
    state, out = None, []
    for p in positions:
        movement, state = step(state, p)
        out.append(movement)
    return out, state


def test_speed_and_heading_from_two_positions():
    out, state = run(position(0, 0), position(15, 75))  # 75 m in 15 s
    assert out[0] is None  # nothing to compare the first position with
    assert out[1]["speed_kmh"] == 18.0
    assert out[1]["heading"] == 90  # east
    assert out[1]["tram"] is True and out[1]["stopped"] is False
    assert state["ts"] == 15


def test_duplicates_and_late_positions_change_nothing():
    out, state = run(position(0, 0), position(15, 75), position(15, 75), position(10, 40))
    assert out[2] is None and out[3] is None
    assert state["ts"] == 15


def test_gps_jump_is_ignored():
    out, state = run(position(0, 0), position(15, 2_000), position(30, 150))  # 2 km in 15 s
    assert out[1] is None
    assert out[2]["speed_kmh"] == 18.0  # measured from the last good fix
    assert state["lon"] == ilica(30, 150)[1]


def test_long_silence_starts_over():
    out, _ = run(position(0, 0), position(600, 900))
    assert out[1] is None


def test_standing_still_keeps_the_last_heading():
    out, _ = run(position(0, 0), position(15, 75), position(30, 77))
    assert out[2]["stopped"] is True
    assert out[2]["heading"] == 90


def test_new_line_starts_over():
    out, _ = run(position(0, 0), position(15, 75, route="7"))
    assert out[1] is None
