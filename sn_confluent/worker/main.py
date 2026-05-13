# Generate a Connect distributed worker config and start script for running
# Confluent Replicator against a Confluent Cloud backing cluster.
#
# Usage: sn-confluent worker [--config PATH] [--out-dir PATH]
#        [--kafka-home PATH] [--auto-install] [--dry-run]

import argparse
import getpass
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
from typing import List, Optional

from sn_confluent.core.pem import check_confluent_cli
from sn_confluent.core.config import load_config as _core_load_config

REQUIRED_KEYS = ("environment_id", "cluster_id")

DEFAULT_PLUGIN_DIR = "./plugins"
DEFAULT_REST_PORT = 8083
DEFAULT_GROUP_ID = "connect-cluster"
DEFAULT_KAFKA_HOME = "${CONFLUENT_HOME:-/usr/local/confluent}"

PLUGIN_INSTALL_INSTRUCTIONS = """\
Replicator plugin not found in {plugin_dir}.

Install options:
  1. Confluent Hub (recommended):
       confluent-hub install confluentinc/kafka-connect-replicator:latest \\
         --component-dir {plugin_dir}

  2. Confluent Platform tarball:
       Download from https://www.confluent.io/installation/
       Copy share/java/kafka-connect-replicator/ into {plugin_dir}/

After installing, re-run: sn-confluent worker --config {config}
"""


def load_config(path: str) -> dict:
    return _core_load_config(path, REQUIRED_KEYS)


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


def resolve_api_credentials(cfg: dict) -> tuple[str, str]:
    """Resolve CC API key and secret.

    Resolution order (first non-empty value wins):
      1. CC_API_KEY / CC_API_SECRET env vars
      2. cc_api_key / cc_api_secret in config file
      3. Interactive getpass prompts
    """
    api_key = os.environ.get("CC_API_KEY", "").strip() or cfg.get("cc_api_key", "").strip()
    api_secret = os.environ.get("CC_API_SECRET", "").strip() or cfg.get("cc_api_secret", "").strip()

    if not api_key:
        api_key = input("CC API Key: ").strip()
    if not api_secret:
        api_secret = getpass.getpass("CC API Secret: ").strip()

    if not api_key or not api_secret:
        print(
            "Error: CC API key and secret are required.\n"
            "Set CC_API_KEY / CC_API_SECRET env vars, add them to your config, or enter them interactively.",
            file=sys.stderr,
        )
        sys.exit(1)

    return api_key, api_secret


def _plugin_present(plugin_dir: str) -> bool:
    """Return True if any kafka-connect-replicator jar exists under plugin_dir."""
    pattern = os.path.join(plugin_dir, "**", "*kafka-connect-replicator*.jar")
    return bool(glob.glob(pattern, recursive=True))


def ensure_plugin(plugin_dir: str, auto_install: bool, config_path: str = "worker.conf") -> bool:
    """Verify Replicator plugin is installed, or install / print instructions.

    Returns True if the plugin is confirmed present, False if instructions were
    printed but the plugin was not installed (worker will fail at runtime).
    """
    abs_dir = os.path.abspath(plugin_dir)

    if _plugin_present(abs_dir):
        print(f"Replicator plugin found in {abs_dir}.")
        return True

    if shutil.which("confluent-hub") is not None:
        if auto_install:
            print(f"Installing Replicator plugin into {abs_dir}...")
            os.makedirs(abs_dir, exist_ok=True)
            result = subprocess.run(
                [
                    "confluent-hub", "install",
                    "confluentinc/kafka-connect-replicator:latest",
                    "--component-dir", abs_dir,
                    "--no-prompt",
                ],
                text=True,
            )
            if result.returncode != 0:
                print("Error: confluent-hub install failed.", file=sys.stderr)
                sys.exit(1)
            print("Replicator plugin installed.")
            return True
        else:
            print(
                f"Replicator plugin not found in {abs_dir}.\n"
                f"Run with --auto-install to install via confluent-hub, or install manually:\n"
                f"  confluent-hub install confluentinc/kafka-connect-replicator:latest "
                f"--component-dir {abs_dir}"
            )
            return False
    else:
        print(
            PLUGIN_INSTALL_INSTRUCTIONS.format(
                plugin_dir=abs_dir,
                config=config_path,
            )
        )
        return False


def generate_worker_properties(
    bootstrap: str,
    api_key: str,
    api_secret: str,
    group_id: str,
    plugin_dir: str,
    rest_port: int,
) -> str:
    """Return the full connect-distributed.properties content as a string."""
    abs_plugin_dir = os.path.abspath(plugin_dir)
    jaas = (
        f"org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{api_key}" password="{api_secret}";'
    )
    return f"""\
# Generated by sn-confluent worker
# DO NOT EDIT — regenerate with: sn-confluent worker [--config <path>]
#
# IMPORTANT: connector.client.config.override.policy=All is required for
# cc-to-sn Replicator so that producer.override.* settings take effect.

# --- Confluent Cloud backing cluster ---
bootstrap.servers={bootstrap}
security.protocol=SASL_SSL
ssl.endpoint.identification.algorithm=https
sasl.mechanism=PLAIN
sasl.jaas.config={jaas}

# --- Distributed worker identity ---
group.id={group_id}

# --- Internal topics (created automatically on first start) ---
config.storage.topic=_connect-configs
offset.storage.topic=_connect-offsets
status.storage.topic=_connect-status
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3

# --- Plugin path (must contain the Replicator connector jar) ---
plugin.path={abs_plugin_dir}

# --- Override policy: required for cc-to-sn producer.override.* ---
connector.client.config.override.policy=All

# --- REST listener ---
rest.port={rest_port}

# --- Converters (byte passthrough; no schema registry required) ---
key.converter=org.apache.kafka.connect.converters.ByteArrayConverter
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

# --- Offset flush ---
offset.flush.interval.ms=10000
"""


def write_start_script(out_dir: str, props_filename: str, kafka_home: str) -> str:
    """Write start-worker.sh and return its absolute path."""
    script_path = os.path.join(out_dir, "start-worker.sh")
    # Confluent Platform uses `connect-distributed` (no .sh suffix).
    # For Apache Kafka, change to $KAFKA_HOME/bin/connect-distributed.sh
    content = f"""\
#!/usr/bin/env bash
# Start the Connect distributed worker for Confluent Replicator.
# Requires Confluent Platform (or Apache Kafka with the Replicator plugin).
#
# Set CONFLUENT_HOME or KAFKA_HOME before running, or pass --kafka-home to
# sn-confluent worker to hardcode the path at generation time.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
KAFKA_HOME="{kafka_home}"

# Confluent Platform binary
CONNECT_BIN="$KAFKA_HOME/bin/connect-distributed"

# Fallback to Apache Kafka .sh if Confluent binary not found
if [ ! -x "$CONNECT_BIN" ]; then
  CONNECT_BIN="$KAFKA_HOME/bin/connect-distributed.sh"
fi

if [ ! -x "$CONNECT_BIN" ]; then
  echo "Error: connect-distributed not found under $KAFKA_HOME" >&2
  echo "Set CONFLUENT_HOME or pass --kafka-home to sn-confluent worker" >&2
  exit 1
fi

exec "$CONNECT_BIN" "$SCRIPT_DIR/{props_filename}"
"""
    with open(script_path, "w", newline="\n") as fh:
        fh.write(content)
    os.chmod(script_path, 0o755)
    return os.path.abspath(script_path)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a Connect worker config and start script for Confluent Replicator."
    )
    parser.add_argument(
        "--config", default="worker.conf",
        help="Path to worker.conf (default: ./worker.conf)",
    )
    parser.add_argument(
        "--out-dir", default=None,
        help="Directory for generated files (default: out_dir from config, or '.')",
    )
    parser.add_argument(
        "--kafka-home", default=None,
        help=f"Kafka/Confluent home for start script (default: {DEFAULT_KAFKA_HOME!r})",
    )
    parser.add_argument(
        "--auto-install", action="store_true",
        help="Install Replicator plugin via confluent-hub without prompting",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print generated files without writing them",
    )
    args = parser.parse_args(argv)

    # 1. Load config
    cfg = load_config(args.config)

    # 2. Check confluent CLI
    check_confluent_cli()

    # 3. Resolve parameters
    environment_id = cfg["environment_id"]
    cluster_id = cfg["cluster_id"]
    plugin_dir = cfg.get("plugin_dir", DEFAULT_PLUGIN_DIR).strip() or DEFAULT_PLUGIN_DIR
    rest_port = int(cfg.get("rest_port", DEFAULT_REST_PORT) or DEFAULT_REST_PORT)
    group_id = cfg.get("group_id", DEFAULT_GROUP_ID).strip() or DEFAULT_GROUP_ID
    out_dir = args.out_dir or cfg.get("out_dir", ".").strip() or "."
    kafka_home = args.kafka_home or DEFAULT_KAFKA_HOME

    # 4. Get CC bootstrap
    print(f"Fetching bootstrap for cluster {cluster_id}...")
    bootstrap = get_cc_bootstrap(environment_id, cluster_id)
    print(f"  Bootstrap: {bootstrap}")

    # 5. Resolve API credentials
    api_key, api_secret = resolve_api_credentials(cfg)

    # 6. Check / install Replicator plugin
    plugin_ok = ensure_plugin(plugin_dir, args.auto_install, config_path=args.config)
    if not plugin_ok:
        print(
            "Warning: Replicator plugin not verified. The worker will fail to start until it is installed.",
            file=sys.stderr,
        )

    # 7. Generate properties
    props_content = generate_worker_properties(
        bootstrap, api_key, api_secret, group_id, plugin_dir, rest_port,
    )
    props_filename = "connect-worker.properties"

    # 8. Dry run — print only
    if args.dry_run:
        print(f"\n--- {props_filename} ---")
        print(props_content)
        print(f"\n--- start-worker.sh ---")
        with tempfile.TemporaryDirectory() as td:
            real_script = open(write_start_script(td, props_filename, kafka_home)).read()
        print(real_script)
        print("(dry-run: no files written)")
        return 0

    # 9. Write files
    os.makedirs(out_dir, exist_ok=True)
    props_path = os.path.join(out_dir, props_filename)
    with open(props_path, "w", newline="\n") as fh:
        fh.write(props_content)
    os.chmod(props_path, 0o600)
    print(f"Written: {os.path.abspath(props_path)}")

    script_path = write_start_script(out_dir, props_filename, kafka_home)
    print(f"Written: {script_path}")

    # 10. Next steps
    print(f"""
Next steps:
  1. Copy {out_dir}/ to your sandbox Linux host (or run there directly)
  2. Start the worker:
       bash start-worker.sh
  3. Deploy the Replicator connector:
       sn-confluent replicate --connect-url http://<worker-host>:{rest_port} --all --no-wizard
""")
    return 0


if __name__ == "__main__":
    sys.exit(main())
