# Mirror topics across both ServiceNow cluster links with DC prefix.
#
# Setup: pip install -e .[dev]   (from repo root)
# Usage: python -m sn_confluent.mirror.main [--config PATH] [--pem-dir PATH] [--filter PREFIX] [--all] [--dry-run]
#        --all accepts optional filters: --include-prefixes, --exclude-prefixes, --include-topics, --exclude-topics

import argparse
import json
import os
import subprocess
import sys
import tempfile
from typing import List, Optional

from sn_confluent.core.pem import (
    SN_BROKERS_PER_CLUSTER,
    SN_SOURCE_CLUSTERS,
    check_auth,
    check_confluent_cli,
    load_pem_files,
)
from sn_confluent.core.config import (
    load_config as _load_config_base,
    expand_link_config,
    add_per_cluster_link_names,
)

from kafka import KafkaConsumer

INTERNAL_TOPIC_PREFIXES = ("__", "_confluent")
KAFKA_REQUEST_TIMEOUT_MS = 30_000
KAFKA_METADATA_MAX_AGE_MS = 10_000
KAFKA_CONNECTIONS_MAX_IDLE_MS = 30_000

REQUIRED_KEYS = ("environment_id", "cluster_id", "link_name", "source_host", "instance_name")


def load_config(path: str, profile: Optional[str] = None) -> dict:
    """Thin wrapper around the shared loader for mirror's required keys."""
    cfg = _load_config_base(path, REQUIRED_KEYS, profile=profile)
    cfg = expand_link_config(cfg)
    cfg = add_per_cluster_link_names(cfg)
    return cfg


def list_source_topics(
    host: str,
    base_port: int,
    ca_path: str,
    cert_path: str,
    key_path: str,
    filter_prefix: str = None,
    brokers_per_cluster: int = SN_BROKERS_PER_CLUSTER,
) -> list:
    bootstrap = ",".join(f"{host}:{base_port + i}" for i in range(brokers_per_cluster))
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap,
            security_protocol="SSL",
            ssl_cafile=ca_path,
            ssl_certfile=cert_path,
            ssl_keyfile=key_path,
            request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
            metadata_max_age_ms=KAFKA_METADATA_MAX_AGE_MS,
            connections_max_idle_ms=KAFKA_CONNECTIONS_MAX_IDLE_MS,
        )
        try:
            raw_topics = consumer.topics()
        finally:
            consumer.close()
    except Exception as exc:
        print(f"Error: Failed to connect to source brokers: {exc}", file=sys.stderr)
        sys.exit(1)

    topics = [
        t for t in raw_topics
        if not any(t.startswith(p) for p in INTERNAL_TOPIC_PREFIXES)
    ]
    if filter_prefix:
        topics = [t for t in topics if t.startswith(filter_prefix)]
    return sorted(topics)


def get_mirrored_source_topics(cfg: dict) -> set:
    source_names = set()
    for port in cfg.get("source_clusters", SN_SOURCE_CLUSTERS):
        link_key = f"link_name_{port}"
        prefix = f"{port}."
        result = subprocess.run(
            [
                "confluent", "kafka", "mirror", "list",
                "--link", cfg[link_key],
                "--environment", cfg["environment_id"],
                "--cluster", cfg["cluster_id"],
                "--output", "json",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            link = cfg.get(f"link_name_{port}", str(port))
            print(
                f"Warning: could not list existing mirrors for link {link} "
                f"({result.stderr.strip()}); [mirrored] labels will be absent.",
                file=sys.stderr,
            )
            continue
        try:
            mirrors = json.loads(result.stdout)
            for m in mirrors:
                name = m.get("mirror_topic", "")
                if name.startswith(prefix):
                    source_names.add(name[len(prefix):])
        except (json.JSONDecodeError, AttributeError):
            print(f"Warning: could not parse mirror list for cluster {port}", file=sys.stderr)
            continue
    return source_names


def build_mirror_filters(
    include_prefixes: list = None,
    exclude_prefixes: list = None,
    include_topics: list = None,
    exclude_topics: list = None,
) -> Optional[str]:
    """Build the auto.create.mirror.topics.filters JSON value, or None if no filters given."""
    entries = []
    for name in (include_prefixes or []):
        entries.append({"filterType": "INCLUDE", "name": name, "patternType": "PREFIXED"})
    for name in (exclude_prefixes or []):
        entries.append({"filterType": "EXCLUDE", "name": name, "patternType": "PREFIXED"})
    for name in (include_topics or []):
        entries.append({"filterType": "INCLUDE", "name": name, "patternType": "LITERAL"})
    for name in (exclude_topics or []):
        entries.append({"filterType": "EXCLUDE", "name": name, "patternType": "LITERAL"})
    return json.dumps({"topicFilters": entries}) if entries else None


def enable_auto_mirror(
    cfg: dict,
    dry_run: bool,
    include_prefixes: list = None,
    exclude_prefixes: list = None,
    include_topics: list = None,
    exclude_topics: list = None,
) -> None:
    filters_json = build_mirror_filters(include_prefixes, exclude_prefixes, include_topics, exclude_topics)
    lines = ["auto.create.mirror.topics.enable=true"]
    if filters_json:
        lines.append(f"auto.create.mirror.topics.filters={filters_json}")

    for port in cfg.get("source_clusters", SN_SOURCE_CLUSTERS):
        link = cfg[f"link_name_{port}"]
        # Write config to a temp file — the CLI's --config CSV parser can't handle
        # JSON values with embedded quotes when passed inline.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".properties", delete=False) as f:
            f.write("\n".join(lines))
            config_path = f.name
        try:
            cmd = [
                "confluent", "kafka", "link", "configuration", "update", link,
                "--environment", cfg["environment_id"],
                "--cluster", cfg["cluster_id"],
                "--config", config_path,
            ]
            if dry_run:
                print("Dry run — would execute:")
                print("  " + " ".join(cmd))
                print("  Config file contents:")
                for line in lines:
                    print(f"    {line}")
                continue
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(result.stderr, file=sys.stderr)
                sys.exit(1)
            print(f"Auto-mirror enabled on {link}.")
        finally:
            os.unlink(config_path)


def create_mirror_topics(cfg: dict, topics: list, dry_run: bool) -> list:
    failures = []
    for port in cfg.get("source_clusters", SN_SOURCE_CLUSTERS):
        link = cfg[f"link_name_{port}"]
        prefix = f"{port}."
        for topic in topics:
            dest = f"{prefix}{topic}"
            cmd = [
                "confluent", "kafka", "mirror", "create", dest,
                "--link", link,
                "--source-topic", topic,
                "--environment", cfg["environment_id"],
                "--cluster", cfg["cluster_id"],
            ]
            if dry_run:
                print("Dry run — would execute:")
                print("  " + " ".join(cmd))
                continue
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                msg = f"{dest} on {link}: {result.stderr.strip()}"
                print(f"Error: {msg}", file=sys.stderr)
                failures.append(msg)
            else:
                print(f"Created mirror topic {dest} on {link}.")
    return failures


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mirror ServiceNow Kafka topics to Confluent Cloud across both cluster links."
    )
    _default_config = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "link", "link.conf"
    )
    parser.add_argument("--config", default=_default_config,
                        help="Path to link.conf (default: ../link/link.conf)")
    parser.add_argument("--profile", default=None,
                        help="Named config profile (INI section in link.conf); omit to use [confluent]")
    parser.add_argument("--pem-dir", default=".",
                        help="Directory containing PEM files (default: ./)")
    parser.add_argument("--filter", default=None, metavar="PREFIX",
                        help="Pre-filter topics by prefix before showing UI")
    parser.add_argument("--all", action="store_true",
                        help="Enable auto-mirror on both links (skip UI)")
    parser.add_argument("--include-prefixes", nargs="+", metavar="PREFIX",
                        help="(--all only) Auto-mirror topics matching these prefixes")
    parser.add_argument("--exclude-prefixes", nargs="+", metavar="PREFIX",
                        help="(--all only) Skip topics matching these prefixes")
    parser.add_argument("--include-topics", nargs="+", metavar="TOPIC",
                        help="(--all only) Auto-mirror these exact topic names")
    parser.add_argument("--exclude-topics", nargs="+", metavar="TOPIC",
                        help="(--all only) Skip these exact topic names")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")
    args = parser.parse_args(argv)

    filter_args = [args.include_prefixes, args.exclude_prefixes, args.include_topics, args.exclude_topics]
    if any(filter_args) and not args.all:
        parser.error("--include-prefixes, --exclude-prefixes, --include-topics, and --exclude-topics require --all")
    return args


def main(argv: Optional[List[str]] = None) -> int:
    import questionary

    args = parse_args(argv)

    cfg = load_config(args.config, profile=args.profile)
    check_confluent_cli()
    check_auth(cfg["environment_id"], cfg["cluster_id"])
    ca, cert, key = load_pem_files(args.pem_dir, return_paths=True)

    if args.all:
        enable_auto_mirror(
            cfg,
            dry_run=args.dry_run,
            include_prefixes=args.include_prefixes,
            exclude_prefixes=args.exclude_prefixes,
            include_topics=args.include_topics,
            exclude_topics=args.exclude_topics,
        )
        return 0

    effective_filter = args.filter or cfg["instance_name"]
    topics = list_source_topics(
        cfg["source_host"], cfg["source_clusters"][0], ca, cert, key,
        filter_prefix=effective_filter, brokers_per_cluster=cfg["brokers_per_cluster"],
    )

    if not topics:
        msg = f"No topics found matching filter '{effective_filter}'."
        print(msg)
        return 0

    already_mirrored = get_mirrored_source_topics(cfg)

    choices = []
    for t in topics:
        label = f"{t}  [mirrored]" if t in already_mirrored else t
        choices.append(questionary.Choice(title=label, value=t))

    selected = questionary.checkbox(
        "Select topics to mirror (space to select, enter to confirm):",
        choices=choices,
    ).ask()

    if not selected:
        print("No topics selected.")
        return 0

    link_names = " and ".join(cfg[f"link_name_{port}"] for port in cfg["source_clusters"])
    print(f"\nWill create {len(selected)} mirror topic(s) on {link_names}:")
    for t in selected:
        for port in cfg["source_clusters"]:
            print(f"  {port}.{t}  →  {cfg[f'link_name_{port}']}")

    if not questionary.confirm("\nProceed?", default=False).ask():
        print("Aborted.")
        return 0

    failures = create_mirror_topics(cfg, selected, dry_run=args.dry_run)

    if failures:
        print(f"\n{len(failures)} failure(s):")
        for f in failures:
            print(f"  {f}")
        return 1
    if not args.dry_run:
        print(f"\nDone. {len(selected) * len(cfg['source_clusters'])} mirror topic(s) created.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
