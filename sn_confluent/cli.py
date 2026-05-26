"""Unified `sn-confluent` CLI dispatcher.

Routes the first positional to a subcommand's `main(argv)` entry point and
forwards the remaining args. Subcommands are imported lazily so a simple
`sn-confluent --help` doesn't pull in every tool's dependencies.
"""

from __future__ import annotations

import sys
from typing import Callable, List, Optional


SUBCOMMAND_HELP = {
    "extract": "Extract PEM files from PKCS12 keystores",
    "link": "Create a Confluent Cloud cluster link",
    "mirror": "Mirror ServiceNow Kafka topics to Confluent Cloud",
    "deploy": "Deploy the ServiceNow Hermes Kafka Connector to Confluent Cloud (sink or source)",
    "api-key": "Create, list, or delete Confluent Cloud Kafka API keys",
    "setup": "Guided end-to-end setup wizard",
}


def _load_subcommand(name: str) -> Callable[[Optional[List[str]]], int]:
    if name == "extract":
        from sn_confluent.extract.main import main
    elif name == "link":
        from sn_confluent.link.main import main
    elif name == "mirror":
        from sn_confluent.mirror.main import main
    elif name == "deploy":
        from sn_confluent.deploy.main import main
    elif name == "api-key":
        from sn_confluent.api_key.main import main
    elif name == "setup":
        from sn_confluent.setup.wizard import main
    else:
        raise KeyError(name)
    return main


def _print_root_help() -> None:
    print("usage: sn-confluent <command> [<args>]")
    print()
    print("ServiceNow <-> Confluent Cloud integration toolkit.")
    print()
    print("Commands:")
    width = max(len(c) for c in SUBCOMMAND_HELP) + 2
    for cmd, help_text in SUBCOMMAND_HELP.items():
        print(f"  {cmd.ljust(width)}{help_text}")
    print()
    print("Run 'sn-confluent <command> --help' for command-specific options.")


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        _print_root_help()
        return 0
    cmd, rest = argv[0], argv[1:]
    if cmd not in SUBCOMMAND_HELP:
        print(f"sn-confluent: unknown command: {cmd}", file=sys.stderr)
        _print_root_help()
        return 2
    try:
        subcommand_main = _load_subcommand(cmd)
    except ImportError as exc:
        print(f"sn-confluent {cmd}: not yet wired ({exc})", file=sys.stderr)
        return 2
    result = subcommand_main(rest)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
