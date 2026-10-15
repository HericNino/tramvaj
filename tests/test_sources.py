import gzip

from feeds import feed

from tramvaj import sources
from tramvaj.feed import decode


def write(directory, ts):
    (directory / f"{ts}.pb.gz").write_bytes(gzip.compress(feed(ts, [("2201", "6", 45.81, 15.97)])))


def test_replay_in_time_order(tmp_path):
    for ts in (1000, 985, 1015):
        write(tmp_path, ts)
    assert [decode(data).ts for data in sources.recorded(tmp_path, speed=0)] == [985, 1000, 1015]


def test_looping_moves_each_pass_forward(tmp_path):
    for ts in (1000, 1015, 1030):
        write(tmp_path, ts)
    played = sources.recorded(tmp_path, speed=0, loop=True)
    snapshots = [decode(next(played)) for _ in range(6)]
    assert [s.ts for s in snapshots] == [1000, 1015, 1030, 1045, 1060, 1075]
    assert snapshots[3].positions[0].ts == 1045 - 1  # vehicle timestamps move too


def test_replay_can_be_moved_to_now(tmp_path):
    for ts in (1000, 1015):
        write(tmp_path, ts)
    assert [decode(data).ts for data in sources.recorded(tmp_path, speed=0, start=5000.7)] == [5000, 5015]
