"""Unified `sn-confluent` CLI dispatcher (Phase 2 stub).

Subcommand handlers are wired in Phase 4 of the unified-CLI refactor.
For now each subcommand prints a "not yet wired" message and exits 0,
so the `pip install -e .` console-script entry point installs cleanly.
"""

from __future__ import annotations

import argparse
import sys
from typing import List, Optional

SUBCOMMANDS = ("extract", "link", "mirror", "replicate", "setup")


def _stub(name: str) -> int:
    print(f"sn-confluent {name}: not yet wired (Phase 4)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sn-confluent",
        description="ServiceNow <-> Confluent Cloud integration toolkit.",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    sub.add_parser("extract", help="Extract PEM files from JKS keystores")
    sub.add_parser("link", help="Create a Confluent Cloud cluster link")
    sub.add_parser("mirror", help="Mirror ServiceNow Kafka topics to Confluent Cloud")
    sub.add_parser("replicate", help="Deploy a Confluent Replicator connector")
    sub.add_parser("setup", help="Guided end-to-end setup wizard")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    # Only parse the first positional so subcommand-specific argv is preserved
    # for Phase 4 dispatch. Today we just acknowledge the subcommand and exit.
    args, _remaining = parser.parse_known_args(argv)
    if not args.command:
        parser.print_help()
        return 0
    if args.command in SUBCOMMANDS:
        return _stub(args.command)
    parser.error(f"unknown command: {args.command}")
    return 2  # pragma: no cover  # parser.error() exits, this is unreachable


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
