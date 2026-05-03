# Deploy a Confluent Replicator (ReplicatorSourceConnector) via Connect REST API.
#
# Setup: pip install -r requirements.txt
# Usage: python setup_replicator.py [--config PATH] [--direction {cc-to-sn,sn-to-cc}]
#        [--topics T1,T2,...] [--all] [--dry-run] [--no-wizard]

import argparse
import configparser
import getpass
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

# Defaults; both can be overridden in link.conf.
SN_SOURCE_CLUSTERS = [4100, 4200]
SN_BROKERS_PER_CLUSTER = 4
SN_INBOUND_PORT = 4000

CONFLUENT_INSTALL = """\
Confluent CLI not found. Install it:

  Windows : winget install Confluent.ConfluentCLI
  macOS   : brew install confluentinc/tap/confluent-cli
  Linux   : See https://docs.confluent.io/confluent-cli/current/install.html

Then authenticate: confluent login
"""

OVERRIDE_POLICY_WARNING = """\
Note: cc-to-sn uses producer.override.* to write to SN Hermes.
Your Connect worker must have:
  connector.client.config.override.policy=All
If not set, the connector will start but silently write to the wrong cluster.
Docs: https://docs.confluent.io/platform/current/connect/security.html
"""


def load_config(path: str) -> dict:
    """Load link.conf; exit 1 with a clear message on any problem."""
    if not os.path.exists(path):
        print(f"Error: Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(path)
    if "confluent" not in cfg:
        print("Error: link.conf is missing the [confluent] section.", file=sys.stderr)
        sys.exit(1)
    section = cfg["confluent"]
    required = ("environment_id", "cluster_id", "connector_name", "source_host", "pem_dir")
    for key in required:
        if key not in section:
            print(f"Error: Missing key in link.conf: {key}", file=sys.stderr)
            sys.exit(1)
    result = dict(section)
    clusters_raw = result.get("source_clusters", "")
    result["source_clusters"] = (
        [int(x.strip()) for x in clusters_raw.split(",") if x.strip()]
        if clusters_raw
        else list(SN_SOURCE_CLUSTERS)
    )
    bpc_raw = result.get("brokers_per_cluster", "")
    result["brokers_per_cluster"] = int(bpc_raw) if bpc_raw else SN_BROKERS_PER_CLUSTER
    return result


def check_confluent_cli() -> None:
    if shutil.which("confluent") is None:
        print(CONFLUENT_INSTALL)
        sys.exit(1)


def load_pem_files(pem_dir: str) -> tuple:
    """Return (ca_bytes, cert_bytes, key_bytes). Exits 1 if any file is missing."""
    files = {"ca.pem": None, "client-cert.pem": None, "client-key.pem": None}
    for name in files:
        path = os.path.join(pem_dir, name)
        if not os.path.exists(path):
            print(
                f"Error: {name} not found in {pem_dir}. Run: python extract_pem.py first",
                file=sys.stderr,
            )
            sys.exit(1)
        with open(path, "rb") as fh:
            files[name] = fh.read()
    return files["ca.pem"], files["client-cert.pem"], files["client-key.pem"]


def resolve_key_password(client_key_bytes: bytes, cli_password: str | None) -> str | None:
    """Resolve the private key password from env, interactive prompt, or CLI flag."""
    env_pw = os.environ.get("REPLICATOR_KEY_PASSWORD")
    if env_pw:
        return env_pw
    if b"ENCRYPTED" in client_key_bytes:
        return getpass.getpass("Private key is encrypted. Enter password: ")
    if cli_password:
        print(
            "Warning: --key-password passes the password via command line. "
            "It may appear in shell history.",
            file=sys.stderr,
        )
        return cli_password
    return None


def get_cc_bootstrap(environment_id: str, cluster_id: str) -> str:
    """Look up CC cluster bootstrap via confluent CLI. Strips SASL_SSL:// prefix."""
    result = subprocess.run(
        [
            "confluent", "kafka", "cluster", "describe", cluster_id,
            "--environment", environment_id,
            "--output", "json",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error: Failed to describe cluster: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    try:
        data = json.loads(result.stdout)
        endpoint = data["endpoint"]
    except (json.JSONDecodeError, KeyError) as exc:
        print(f"Error: Unexpected cluster describe output: {exc}", file=sys.stderr)
        sys.exit(1)
    return endpoint.replace("SASL_SSL://", "")


def list_cc_topics(environment_id: str, cluster_id: str) -> list[str]:
    """List topics on the CC cluster via confluent CLI."""
    result = subprocess.run(
        [
            "confluent", "kafka", "topic", "list",
            "--cluster", cluster_id,
            "--environment", environment_id,
            "--output", "json",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error: Failed to list CC topics: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    try:
        topics = json.loads(result.stdout)
        return sorted(t["name"] for t in topics)
    except (json.JSONDecodeError, KeyError) as exc:
        print(f"Error: Unexpected topic list output: {exc}", file=sys.stderr)
        sys.exit(1)


def list_sn_topics(
    source_host: str,
    source_clusters: list[int],
    brokers_per_cluster: int,
    ca_pem_bytes: bytes,
    client_cert_bytes: bytes,
    client_key_bytes: bytes,
    key_password: str | None,
) -> list[str]:
    """List topics on SN Hermes via kafka-python AdminClient."""
    try:
        from kafka.admin import KafkaAdminClient
    except ImportError:
        print(
            "Error: kafka-python is required for SN topic listing.\n"
            "Install it: pip install kafka-python",
            file=sys.stderr,
        )
        sys.exit(1)

    base_port = source_clusters[0]
    bootstrap = ",".join(
        f"{source_host}:{base_port + i}" for i in range(brokers_per_cluster)
    )
    ssl_context_kwargs = {
        "bootstrap_servers": bootstrap,
        "security_protocol": "SSL",
        "ssl_cafile_data": ca_pem_bytes.decode("utf-8"),
        "ssl_certfile_data": client_cert_bytes.decode("utf-8"),
        "ssl_keyfile_data": client_key_bytes.decode("utf-8"),
    }
    if key_password:
        ssl_context_kwargs["ssl_password"] = key_password
    try:
        admin = KafkaAdminClient(**ssl_context_kwargs)
        raw = admin.list_topics()
        admin.close()
    except Exception as exc:
        print(f"Error: Failed to list SN topics: {exc}", file=sys.stderr)
        sys.exit(1)

    return sorted(t for t in raw if not t.startswith("__") and not t.startswith("_confluent"))


def build_topic_regex(topics: list[str] | None, use_all: bool) -> str:
    """Build topic.regex from selected topics or --all flag."""
    if use_all:
        return "(?!__)(?!_confluent).+"
    if topics:
        escaped = [t.replace(".", "\\.") for t in topics]
        return "^(" + "|".join(escaped) + ")$"
    raise ValueError("No topics specified")


def sn_bootstrap(host: str, base_port: int, brokers_per_cluster: int) -> str:
    return ",".join(f"{host}:{base_port + i}" for i in range(brokers_per_cluster))


def build_cc_to_sn_config(
    connector_name: str,
    source_host: str,
    inbound_port: int,
    brokers_per_cluster: int,
    ca_pem: bytes,
    client_cert: bytes,
    client_key: bytes,
    key_password: str | None,
    group_id: str,
    topic_regex: str,
    topic_rename_format: str,
) -> dict:
    """Build connector config for cc-to-sn direction."""
    bootstrap = sn_bootstrap(source_host, inbound_port, brokers_per_cluster)
    config = {
        "name": connector_name,
        "connector.class": "io.confluent.connect.replicator.ReplicatorSourceConnector",
        "producer.override.bootstrap.servers": bootstrap,
        "producer.override.security.protocol": "SSL",
        "producer.override.ssl.truststore.type": "PEM",
        "producer.override.ssl.truststore.certificates": ca_pem.decode("utf-8").strip(),
        "producer.override.ssl.keystore.type": "PEM",
        "producer.override.ssl.keystore.certificate.chain": client_cert.decode("utf-8").strip(),
        "producer.override.ssl.keystore.key": client_key.decode("utf-8").strip(),
        "src.consumer.group.id": group_id,
        "topic.regex": topic_regex,
        "topic.rename.format": topic_rename_format,
        "tasks.max": "4",
    }
    if key_password:
        config["producer.override.ssl.key.password"] = key_password
    return config


def build_sn_to_cc_configs(
    connector_name: str,
    source_host: str,
    source_clusters: list[int],
    brokers_per_cluster: int,
    ca_pem: bytes,
    client_cert: bytes,
    client_key: bytes,
    key_password: str | None,
    group_id: str,
    topic_regex: str,
    topic_rename_format: str,
) -> list[dict]:
    """Build connector configs for sn-to-cc direction (one per source cluster)."""
    configs = []
    for base_port in source_clusters:
        bootstrap = sn_bootstrap(source_host, base_port, brokers_per_cluster)
        config = {
            "name": f"{connector_name}-{base_port}",
            "connector.class": "io.confluent.connect.replicator.ReplicatorSourceConnector",
            "src.kafka.bootstrap.servers": bootstrap,
            "src.kafka.security.protocol": "SSL",
            "src.kafka.ssl.truststore.type": "PEM",
            "src.kafka.ssl.truststore.certificates": ca_pem.decode("utf-8").strip(),
            "src.kafka.ssl.keystore.type": "PEM",
            "src.kafka.ssl.keystore.certificate.chain": client_cert.decode("utf-8").strip(),
            "src.kafka.ssl.keystore.key": client_key.decode("utf-8").strip(),
            "src.consumer.group.id": f"{group_id}-{base_port}",
            "topic.regex": topic_regex,
            "topic.rename.format": topic_rename_format,
            "tasks.max": "4",
        }
        if key_password:
            config["src.kafka.ssl.key.password"] = key_password
        configs.append(config)
    return configs


def create_connector(connect_url: str, config: dict) -> str:
    """POST connector config to Connect REST API. Returns connector name on 201."""
    payload = json.dumps({"name": config["name"], "config": config}).encode("utf-8")
    url = f"{connect_url}/connectors"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body.get("name", config["name"])
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        print(
            f"Error: Failed to create connector '{config['name']}': "
            f"HTTP {exc.code} — {error_body}",
            file=sys.stderr,
        )
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(
            f"Error: Cannot reach Connect at {url}: {exc.reason}",
            file=sys.stderr,
        )
        sys.exit(1)


def poll_connector(connect_url: str, name: str, timeout: int = 120, interval: int = 5) -> None:
    """Poll connector status until RUNNING or FAILED."""
    url = f"{connect_url}/connectors/{name}/status"
    deadline = time.monotonic() + timeout
    print(f"Waiting for connector '{name}' (timeout: {timeout}s)...", flush=True)

    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, json.JSONDecodeError):
            print("  (status check failed, retrying...)", flush=True)
            time.sleep(interval)
            continue

        state = data.get("connector", {}).get("state", "UNKNOWN").upper()
        print(f"  State: {state}", flush=True)

        if state == "RUNNING":
            return
        if state == "FAILED":
            tasks = data.get("tasks", [])
            for task in tasks:
                trace = task.get("trace")
                if trace:
                    print(f"  Task {task.get('id', '?')} trace: {trace}", file=sys.stderr)
            print(f"Error: Connector '{name}' FAILED.", file=sys.stderr)
            sys.exit(1)

        time.sleep(interval)

    print(f"Error: Timed out after {timeout}s waiting for '{name}'.", file=sys.stderr)
    sys.exit(1)


def run_wizard(
    direction: str,
    connector_name: str,
    group_id: str,
    topic_rename_format: str,
    connect_url: str,
    available_topics: list[str],
) -> tuple:
    """Interactive wizard. Returns (selected_topics, connector_name, group_id,
    topic_rename_format, connect_url, confirmed)."""
    try:
        import questionary
    except ImportError:
        print(
            "Error: questionary is required for interactive mode.\n"
            "Install it: pip install questionary\n"
            "Or use --no-wizard with --topics/--all.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Direction: {direction}  (override with --direction)")

    source_label = "Confluent Cloud" if direction == "cc-to-sn" else "SN Hermes"
    print(f"Fetching topics from {source_label}...")

    selected = questionary.checkbox(
        "Select topics to replicate (space to select, enter to confirm):",
        choices=available_topics,
    ).ask()

    if not selected:
        print("No topics selected.")
        sys.exit(0)

    connector_name = questionary.text(
        "Connector name:", default=connector_name,
    ).ask()
    group_id = questionary.text(
        "Consumer group ID:", default=group_id,
    ).ask()
    topic_rename_format = questionary.text(
        "Destination topic format:", default=topic_rename_format,
    ).ask()
    connect_url = questionary.text(
        "Connect worker URL:", default=connect_url,
    ).ask()

    return selected, connector_name, group_id, topic_rename_format, connect_url


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy a Confluent Replicator via Connect REST API."
    )
    parser.add_argument(
        "--config", default="link.conf",
        help="Path to link.conf (default: ./link.conf)",
    )
    parser.add_argument(
        "--pem-dir", default=None,
        help="Override pem_dir in config",
    )
    parser.add_argument(
        "--direction", choices=("cc-to-sn", "sn-to-cc"), default=None,
        help="Replication direction (default: cc-to-sn)",
    )
    parser.add_argument(
        "--topics", default=None,
        help="Comma-separated topic list (skip wizard topic picker)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Use topic.regex catch-all pattern",
    )
    parser.add_argument(
        "--connect-url", default=None,
        help="Connect REST endpoint override",
    )
    parser.add_argument(
        "--key-password", default=None, metavar="PASSWORD",
        help="Private key password (warning: visible in shell history)",
    )
    parser.add_argument(
        "--timeout", type=int, default=120,
        help="Per-connector poll timeout in seconds (default: 120)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print config JSON and REST call details without creating",
    )
    parser.add_argument(
        "--no-wizard", action="store_true",
        help="Non-interactive mode; require topics via config, --topics, or --all",
    )
    args = parser.parse_args()

    # 1. Load config
    cfg = load_config(args.config)

    # 2. Check confluent CLI
    check_confluent_cli()

    # 3. Load PEM files
    pem_dir = args.pem_dir or cfg["pem_dir"]
    ca_pem, client_cert, client_key = load_pem_files(pem_dir)

    # 4. Resolve key password
    key_password = resolve_key_password(client_key, args.key_password)

    # 5. Resolve direction
    direction = args.direction or cfg.get("direction", "cc-to-sn")

    # 6. CC bootstrap (cc-to-sn only — needed for topic listing, not connector config)
    # cc-to-sn source is the CC cluster; the Connect worker reads from it natively.

    # 7. Resolve topics
    connector_name = cfg["connector_name"]
    group_id = cfg.get("group_id", f"{connector_name}-group")
    topic_rename_format = cfg.get("topic_rename_format", "${topic}")
    connect_url = args.connect_url or cfg.get("connect_url", "http://localhost:8083")
    inbound_port = int(cfg.get("inbound_port", SN_INBOUND_PORT))
    source_clusters = cfg["source_clusters"]
    brokers_per_cluster = cfg["brokers_per_cluster"]
    source_host = cfg["source_host"]

    use_all = args.all
    selected_topics = None
    wizard_ran = False

    if args.topics:
        selected_topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    elif not use_all:
        # Check config for topics
        config_topics = cfg.get("topics", "").strip()
        if config_topics:
            selected_topics = [t.strip() for t in config_topics.split(",") if t.strip()]
        elif args.no_wizard:
            print(
                "Error: No topics specified. Use --topics, --all, or set topics in config.",
                file=sys.stderr,
            )
            sys.exit(1)
        else:
            # Run wizard — need topic list first
            if direction == "cc-to-sn":
                print("Fetching topics from Confluent Cloud...")
                available = list_cc_topics(cfg["environment_id"], cfg["cluster_id"])
            else:
                print("Fetching topics from SN Hermes...")
                available = list_sn_topics(
                    source_host, source_clusters, brokers_per_cluster,
                    ca_pem, client_cert, client_key, key_password,
                )

            if not available:
                print("No topics found on source cluster.")
                sys.exit(1)

            (
                selected_topics, connector_name, group_id,
                topic_rename_format, connect_url,
            ) = run_wizard(
                direction, connector_name, group_id,
                topic_rename_format, connect_url, available,
            )
            wizard_ran = True

    # 8. Build topic regex
    topic_regex = build_topic_regex(selected_topics, use_all)

    # 9. Build connector config(s)
    if direction == "cc-to-sn":
        configs = [build_cc_to_sn_config(
            connector_name, source_host, inbound_port, brokers_per_cluster,
            ca_pem, client_cert, client_key, key_password,
            group_id, topic_regex, topic_rename_format,
        )]
    else:
        configs = build_sn_to_cc_configs(
            connector_name, source_host, source_clusters, brokers_per_cluster,
            ca_pem, client_cert, client_key, key_password,
            group_id, topic_regex, topic_rename_format,
        )

    # 10. Override policy warning (cc-to-sn only)
    if direction == "cc-to-sn":
        print(OVERRIDE_POLICY_WARNING)

    # 11. Dry run
    if args.dry_run:
        for config in configs:
            print(f"--- Connector: {config['name']} ---")
            print(f"POST {connect_url}/connectors")
            print(json.dumps({"name": config["name"], "config": config}, indent=2))
        return

    # 12. Wizard confirmation
    if wizard_ran:
        print("\n--- Preview ---")
        for config in configs:
            print(json.dumps({"name": config["name"], "config": config}, indent=2))
        try:
            import questionary
            if not questionary.confirm("Create connector(s)?", default=False).ask():
                print("Aborted.")
                return
        except ImportError:
            pass  # Should not happen — wizard already imported it
    elif not args.no_wizard and not args.topics and not use_all:
        # Interactive but topics came from config — still confirm
        pass

    # 13. Create and poll each connector
    for config in configs:
        print(f"Creating connector '{config['name']}'...")
        name = create_connector(connect_url, config)
        print(f"Connector '{name}' created. Polling status...")
        poll_connector(connect_url, name, timeout=args.timeout)
        print(f"Connector '{name}' is RUNNING.")


if __name__ == "__main__":
    main()
