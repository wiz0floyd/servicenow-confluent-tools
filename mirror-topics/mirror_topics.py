# Mirror topics across both ServiceNow cluster links with DC prefix.
#
# Setup: pip install -r requirements.txt
# Usage: python mirror_topics.py [--config PATH] [--pem-dir PATH] [--filter PREFIX] [--all] [--dry-run]

import argparse
import configparser
import json
import os
import shutil
import subprocess
import sys

from kafka import KafkaConsumer

INTERNAL_TOPIC_PREFIXES = ("__", "_confluent")
# Defaults; both can be overridden in link.conf.
SN_SOURCE_CLUSTERS = [4100, 4200]
SN_BROKERS_PER_CLUSTER = 4

CONFLUENT_INSTALL = """\
Confluent CLI not found. Install it:

  Windows : winget install Confluent.ConfluentCLI
  macOS   : brew install confluentinc/tap/confluent-cli
  Linux   : See https://docs.confluent.io/confluent-cli/current/install.html

Then authenticate: confluent login
"""


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        print(f"Error: Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(path)
    if "confluent" not in cfg:
        print("Error: link.conf is missing the [confluent] section.", file=sys.stderr)
        sys.exit(1)
    section = cfg["confluent"]
    for key in ("environment_id", "cluster_id", "link_name", "source_host", "instance_name"):
        if key not in section:
            print(f"Error: Missing key in link.conf: {key}", file=sys.stderr)
            sys.exit(1)
    result = dict(section)
    clusters_raw = result.get("source_clusters", "")
    source_clusters = (
        [int(x.strip()) for x in clusters_raw.split(",") if x.strip()]
        if clusters_raw
        else list(SN_SOURCE_CLUSTERS)
    )
    bpc_raw = result.get("brokers_per_cluster", "")
    result["source_clusters"] = source_clusters
    result["brokers_per_cluster"] = int(bpc_raw) if bpc_raw else SN_BROKERS_PER_CLUSTER
    for port in source_clusters:
        result[f"link_name_{port}"] = f"{result['link_name']}-{port}"
    return result


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
        )
        raw_topics = consumer.topics()
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
            continue
        try:
            mirrors = json.loads(result.stdout)
            for m in mirrors:
                name = m.get("mirror_topic", "")
                if name.startswith(prefix):
                    source_names.add(name[len(prefix):])
        except (json.JSONDecodeError, AttributeError):
            continue
    return source_names


def enable_auto_mirror(cfg: dict, dry_run: bool) -> None:
    for port in cfg.get("source_clusters", SN_SOURCE_CLUSTERS):
        link = cfg[f"link_name_{port}"]
        cmd = [
            "confluent", "kafka", "link", "configuration", "update", link,
            "--environment", cfg["environment_id"],
            "--cluster", cfg["cluster_id"],
            "--config", "auto.create.mirror.topics.enable=true",
        ]
        if dry_run:
            print("Dry run — would execute:")
            print("  " + " ".join(cmd))
            continue
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            sys.exit(1)
        print(f"Auto-mirror enabled on {link}.")


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


def check_confluent_cli() -> None:
    if shutil.which("confluent") is None:
        print(CONFLUENT_INSTALL)
        sys.exit(1)


def check_auth(environment_id: str, cluster_id: str) -> None:
    result = subprocess.run(
        ["confluent", "kafka", "cluster", "describe", cluster_id,
         "--environment", environment_id],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("Error: Not authenticated or cluster not accessible. Run: confluent login",
              file=sys.stderr)
        sys.exit(1)


def load_pem_files(pem_dir: str):
    """Return (ca_path, cert_path, key_path) — kafka-python takes file paths."""
    files = {"ca.pem": None, "client-cert.pem": None, "client-key.pem": None}
    for name in files:
        path = os.path.join(pem_dir, name)
        if not os.path.exists(path):
            print(f"Error: {name} not found in {pem_dir}. Run: python extract_pem.py first",
                  file=sys.stderr)
            sys.exit(1)
        files[name] = path
    return files["ca.pem"], files["client-cert.pem"], files["client-key.pem"]


def main() -> None:
    import questionary

    parser = argparse.ArgumentParser(
        description="Mirror ServiceNow Kafka topics to Confluent Cloud across both cluster links."
    )
    parser.add_argument("--config", default="../cluster-link/link.conf",
                        help="Path to link.conf (default: ../cluster-link/link.conf)")
    parser.add_argument("--pem-dir", default=".",
                        help="Directory containing PEM files (default: ./)")
    parser.add_argument("--filter", default=None, metavar="PREFIX",
                        help="Pre-filter topics by prefix before showing UI")
    parser.add_argument("--all", action="store_true",
                        help="Enable auto-mirror on both links (skip UI)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")
    args = parser.parse_args()

    cfg = load_config(args.config)
    check_confluent_cli()
    check_auth(cfg["environment_id"], cfg["cluster_id"])
    ca, cert, key = load_pem_files(args.pem_dir)

    if args.all:
        enable_auto_mirror(cfg, dry_run=args.dry_run)
        return

    effective_filter = args.filter or cfg["instance_name"]
    topics = list_source_topics(
        cfg["source_host"], cfg["source_clusters"][0], ca, cert, key,
        filter_prefix=effective_filter, brokers_per_cluster=cfg["brokers_per_cluster"],
    )

    if not topics:
        msg = f"No topics found matching filter '{effective_filter}'."
        print(msg)
        return

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
        return

    link_names = " and ".join(cfg[f"link_name_{port}"] for port in cfg["source_clusters"])
    print(f"\nWill create {len(selected)} mirror topic(s) on {link_names}:")
    for t in selected:
        for port in cfg["source_clusters"]:
            print(f"  {port}.{t}  →  {cfg[f'link_name_{port}']}")

    if not questionary.confirm("\nProceed?", default=False).ask():
        print("Aborted.")
        return

    failures = create_mirror_topics(cfg, selected, dry_run=args.dry_run)

    if failures:
        print(f"\n{len(failures)} failure(s):")
        for f in failures:
            print(f"  {f}")
        sys.exit(1)
    elif not args.dry_run:
        print(f"\nDone. {len(selected) * len(cfg['source_clusters'])} mirror topic(s) created.")


if __name__ == "__main__":
    main()
