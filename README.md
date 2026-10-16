# tramvaj

Zagreb's trams, live. ZET publishes where every tram and bus is, about every ten seconds, as a GTFS-Realtime feed. tramvaj streams that feed through Kafka and works out what the raw positions don't say: how fast each vehicle is going, which lines are crawling, where in the city traffic stands still, and when two trams of the same line run right behind each other.

**Replay: [hericnino.github.io/tramvaj](https://hericnino.github.io/tramvaj/)**: an hour of a weekday morning, recorded from the feed, run through the same Kafka pipeline on GitHub Actions and played back in the browser. The live version needs Kafka running; see [Running it](#running-it).

It's a small but complete streaming pipeline: an idempotent producer, keyed and compacted topics, stateful processing with state backed up to Kafka, event-time windows that cope with late data, exactly-once processing, a dead-letter topic, and a map that updates over Server-Sent Events.

## How it works

```
ZET GTFS-RT feed (protobuf, whole network every ~10 s)
  └─ tramvaj produce        decode, send only positions that changed           → zet.positions  (key: vehicle)
                            unreadable snapshots                               → zet.dlq
                            delay at the next stop                             → zet.delays     (key: trip)
  └─ tramvaj process        Quix Streams, exactly-once
       ├─ per vehicle       speed and heading from the previous position       → zet.movements, zet.vehicles (compacted)
       ├─ regroup by line   bunching: two vehicles of a line within 250 m      → zet.alerts
       │                    5-minute windows: mean speed, share stopped        → zet.line-speed
       └─ regroup by area   10-minute windows on a 250 m grid, trams only      → zet.hotspots
  └─ tramvaj serve          reads the output topics into memory, serves the map and /api/live, /api/stream (SSE)
  └─ tramvaj export         or: writes the output topics to one JSON file the map plays back without a server
```

| topic | key | partitions | kept | what |
|---|---|---|---|---|
| `zet.positions` | vehicle | 6 | 3 days | raw positions, timestamped with the vehicle's report time |
| `zet.delays` | trip | 6 | 3 days | delay at the next stop, when it changes |
| `zet.movements` | vehicle | 6 | 3 days | positions with speed, heading, grid cell |
| `zet.vehicles` | vehicle | 6 | compacted | the latest movement of each vehicle |
| `zet.line-speed` | line | 3 | 30 days | 5-minute averages per line |
| `zet.hotspots` | cell | 3 | 30 days | 10-minute averages per 250 m square |
| `zet.alerts` | line | 3 | 30 days | bunching |
| `zet.dlq` | none | 1 | 7 days | snapshots that couldn't be decoded, as they arrived |

## Decisions, and what went wrong on the way

**Keys decide what can be computed.** Speed needs a vehicle's previous position, so positions are keyed by vehicle: all of a vehicle's reports land in one partition, in order, and the processor keeps the last one in state (RocksDB, backed up to a changelog topic, so a restarted processor carries on where it stopped). Bunching needs all vehicles of a line together and the hotspots need all vehicles in an area, so the stream is regrouped by line and by grid cell. Each regrouping is a repartition topic: a full round trip through Kafka.

**Changes, not copies.** Every snapshot holds the whole network, and most of it hasn't changed: a vehicle that hasn't reported again keeps its old timestamp. The producer remembers the newest timestamp it sent per vehicle and only sends newer ones. The producer is idempotent (`enable.idempotence`, `acks=all`), so a retry after a network error can't write a position twice.

**Event time.** Each message is timestamped with when the vehicle reported, not when the feed was read, and the windows group by that. A line's 5-minute average covers what happened in those five minutes, even if a position arrives a bit late.

**Out of order after regrouping.** Within a partition, positions are in order. After regrouping by line, a line's positions come from all six source partitions, in whatever order the processor reads them. When one source partition is ahead (a backlog after a restart, a fast replay), its events move the windows' clock forward and the others' look late, so windows close before their data has arrived. Three things came out of finding this:

- Windows wait for late events (`--grace`, 60 s by default). Live, a minute is plenty; a fast replay needs more. Events that still come too late are counted and logged.
- Windows close on partition time. By default Quix closes a key's window only when the same key shows up again: a grid cell's 10-minute window stayed open until a tram next drove through that cell, at night until morning. With `closing_strategy="partition"` a window closes once time on its partition passes its end plus the grace. The trade-off, found by testing both: in this mode an event that arrives shortly after its window closed is dropped without being reported as late (the default reports it, though it drops it too), so the logged count of late events is a lower bound.
- Bunching compares two vehicles at the same moment. Each vehicle's position is projected along its heading to the other's report time before measuring the gap. Comparing raw positions, two trams 100 m apart could look hundreds of metres apart or on top of each other, depending on which one had reported last.

**Start consuming before producing.** The pipeline test first waited a fixed 8 seconds for the processor before sending data. On GitHub Actions the processor took about 15 seconds to join its consumer group, the first positions piled up, and the processor then read that backlog one partition at a time: the same out-of-order problem as above, with windows missing events and a bunching alert missing. Producing without waiting reproduces it every time. Now the test, and `tramvaj produce --wait`, ask Kafka for the processor's consumer group and start once it is stable and has the positions topic assigned.

**Exactly-once.** With `processing_guarantee="exactly-once"` the processor's output, its state changes and its consumer offsets are committed in one Kafka transaction. A crash between writing an alert and committing the offset can't produce the alert twice. Anything reading the output topics uses `isolation.level=read_committed`, so it never sees a transaction that was rolled back. The cost is latency: output becomes visible when the transaction commits.

**Compaction for current state.** `zet.vehicles` is compacted: Kafka keeps the newest message per key. The map's backend reads it from the start when it starts and has every vehicle's current position, without replaying three days of history.

**Retention counts from the message timestamp.** The integration test used positions from a fixed day in 2025 and failed about one run in three: Kafka deletes a segment once its newest timestamp is older than the topic's retention, and the retention check (every five minutes) sometimes ran mid-test and deleted the input. Since the timestamp is event time, a recording replayed as it is would be deleted the same way. Replays are therefore shifted to start at the current time, and the test uses recent timestamps.

**Bad input goes to a dead-letter topic.** A snapshot that doesn't decode (an HTML error page instead of protobuf, a cut-off download) is written to `zet.dlq` with the error and its first 4 KB, base64-encoded. The pipeline carries on and the evidence is kept for a week.

**GPS noise.** A speed is only computed from reports 5 to 300 seconds apart. Anything over 90 km/h is a bad fix and is ignored, below 3 km/h counts as stopped, and the heading is only updated after at least 8 m of movement, since GPS jitter otherwise spins a standing tram around.

## Running it

Everything in Docker:

```bash
docker compose up --build                 # the live ZET feed
open http://localhost:8000
```

Recorded snapshots instead of the live feed: put `*.pb.gz` files in `./samples` (see below) and run

```bash
REPLAY_DIR=/app/samples REPLAY_SPEED=5 GRACE_S=300 docker compose up --build
```

For development, Kafka in Docker and the rest with [uv](https://docs.astral.sh/uv/):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
uv sync
uv run tramvaj process &                              # stream processor
uv run tramvaj produce &                              # live feed, or: --replay samples --speed 10 --loop
uv run tramvaj serve                                  # map on http://localhost:8000
```

### The replay page

The **Replay page** workflow records an hour of the feed on weekday mornings (or when run by hand), starts Kafka on the runner, runs the processor on the recording at ten times the speed, and exports everything it wrote with `tramvaj export`. The map finds no API on GitHub Pages and plays back `data/replay.json` instead, with a time slider. If the feed is down for the whole recording, the workflow publishes the site with the last recording it had. The same works locally:

```bash
uv run tramvaj process --group replay &
uv run tramvaj produce --replay samples --speed 10 --wait --group replay
uv run tramvaj export --wait --out site/data/replay.json
python -m http.server -d site                          # http://localhost:8000
```

### Recording the feed

`tramvaj record --minutes 60 --out samples` saves a snapshot every 15 seconds as `<feed timestamp>.pb.gz`. The **Record feed** workflow does the same on GitHub Actions (run it by hand from the Actions tab) and keeps the snapshots as an artifact for 30 days. A recorded morning can then be replayed as often as needed while working on the processing.

### Tests

```bash
uv run pytest                                         # unit tests
KAFKA_BROKER=localhost:9092 uv run pytest             # plus the pipeline against a real broker
```

The pipeline test runs the producer and the processor side by side on a broker, on topics with a random suffix, and checks every output topic: the dead-letter topic, speeds, the compacted latest state, bunching alerts (one pair, measured at 100 m, reported again after the cooldown), line windows and hotspots. CI runs it against Kafka in a service container.

## Limits

- Speed is the straight-line distance between two reports divided by the time between them. On a curve it's a little low.
- The feed has no direction of travel and no stops. Trams and buses are told apart by line number (trams run lines 1 to 17, and 31 to 34 at night). The static timetable (GTFS) would add stops and directions; it isn't used yet.
- Hotspots are squares of about 250 m, not street segments.
- Bunching keeps each vehicle's latest position only, so two trams waiting at a terminus can be reported as bunched.
- `zet.delays` is collected but not used yet.
- The replay page never shows its last 5 to 10 minutes of windows: nothing comes after the final snapshot to move time past them.
- The map's backend reads the windowed topics from the beginning when it starts, up to 30 days of them. Fine for one city; past that it should start from a recent offset.

## Data

Vehicle positions from ZET's public GTFS-Realtime feed (`https://www.zet.hr/gtfs-rt-protobuf`). Map tiles © OpenStreetMap contributors (OpenStreetMap's standard tiles, muted with a CSS filter). Typefaces: Barlow Semi Condensed and IBM Plex Mono (SIL Open Font License, in `site/fonts`). Map library: Leaflet (BSD-2-Clause, in `site/vendor/leaflet`).

## License

MIT
