from tramvaj.export import tracks


def move(vid, route, ts, heading=90.0):
    return {
        "vehicle_id": vid,
        "route_id": route,
        "tram": True,
        "lat": 45.8123456,
        "lon": 15.9765432,
        "ts": ts,
        "speed_kmh": 17.6,
        "heading": heading,
    }


def test_tracks_per_vehicle_and_line_in_time_order():
    start, end, out = tracks([move("2201", "6", 130), move("2201", "6", 100), move("2202", "6", 110, None), move("2201", "11", 160)])
    assert (start, end) == (100, 160)
    assert [(t["v"], t["r"], len(t["p"])) for t in out] == [("2201", "6", 2), ("2201", "11", 1), ("2202", "6", 1)]
    assert out[0]["p"][0] == [0, 45.81235, 15.97654, 18, 90]
    assert out[2]["p"][0][4] == -1  # heading unknown


def test_nothing_to_export():
    assert tracks([]) == (0, 0, [])
