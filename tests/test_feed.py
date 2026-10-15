import pytest
from feeds import feed

from tramvaj.feed import FeedError, decode, is_tram


def test_decode_positions_and_delays():
    data = feed(1_760_000_000, [("102241", "13", 45.81, 15.97), ("206", "206", 45.82, 16.03)], [("trip-1", "13", -22)])
    snapshot = decode(data)
    assert snapshot.ts == 1_760_000_000
    assert [(p.vehicle_id, p.route_id, p.ts) for p in snapshot.positions] == [
        ("102241", "13", 1_759_999_999),
        ("206", "206", 1_759_999_999),
    ]
    assert [(d.trip_id, d.delay_s, d.stop_id) for d in snapshot.delays] == [("trip-1", -22, "221_1")]


def test_positions_outside_zagreb_are_dropped():
    snapshot = decode(feed(1, [("1", "6", 0.0, 0.0), ("2", "6", 45.8, 15.98)]))
    assert [p.vehicle_id for p in snapshot.positions] == ["2"]


def test_garbage_is_an_error():
    with pytest.raises(FeedError):
        decode(b"<html>maintenance</html>")


@pytest.mark.parametrize("route, tram", [("1", True), ("17", True), ("34", True), ("18", False), ("206", False), ("N1", False)])
def test_is_tram(route, tram):
    assert is_tram(route) is tram
