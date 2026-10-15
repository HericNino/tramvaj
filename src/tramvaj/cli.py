"""tramvaj command line.

tramvaj topics    create the Kafka topics
tramvaj produce   read the feed (live, or --replay DIR) into Kafka
tramvaj process   run the stream processor
tramvaj serve     the live map and its API
tramvaj record    save live snapshots to a directory, for replaying later
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

log = logging.getLogger("tramvaj")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tramvaj", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["topics", "produce", "process", "serve", "record"])
    parser.add_argument("--broker", default=os.environ.get("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument(
        "--replay",
        type=Path,
        default=Path(os.environ["REPLAY_DIR"]) if os.environ.get("REPLAY_DIR") else None,
        help="produce: play recorded snapshots from this directory instead of the live feed",
    )
    parser.add_argument(
        "--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", "1")), help="replay speed, 0 = as fast as possible"
    )
    parser.add_argument("--loop", action="store_true", default=os.environ.get("REPLAY_LOOP") == "1", help="replay: start over at the end")
    parser.add_argument("--interval", type=float, default=10.0, help="live: seconds between feed reads")
    parser.add_argument("--minutes", type=float, default=30.0, help="record: how long")
    parser.add_argument("--out", type=Path, default=Path("samples"), help="record: where to save")
    parser.add_argument("--state-dir", default=os.environ.get("STATE_DIR", "state"))
    parser.add_argument(
        "--grace", type=float, default=float(os.environ.get("GRACE_S", "60")), help="process: seconds a window waits for late events"
    )
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "topics":
        from . import topics

        created = topics.ensure(args.broker)
        log.info("created: %s", ", ".join(created) or "nothing, all topics exist")
    elif args.command == "produce":
        from . import sources, topics
        from .producer import FeedProducer

        topics.ensure(args.broker)
        if args.replay:  # replayed as if it were happening now, see sources.recorded
            snapshots = sources.recorded(args.replay, args.speed, args.loop, start=time.time())
        else:
            snapshots = sources.live(interval_s=args.interval)
        stats = FeedProducer.connect(args.broker).run(snapshots)
        log.info("done: %s", stats)
    elif args.command == "process":
        from . import processor, topics

        topics.ensure(args.broker)
        from datetime import timedelta

        processor.run(args.broker, grace=timedelta(seconds=args.grace), state_dir=args.state_dir)
    elif args.command == "serve":
        import uvicorn

        from .api import create_app

        uvicorn.run(create_app(args.broker), host="0.0.0.0", port=args.port, log_level="warning")
    elif args.command == "record":
        from . import sources

        saved = sources.record(args.out, args.minutes)
        log.info("saved %d snapshots to %s", saved, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
