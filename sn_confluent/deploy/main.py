# Deploy the ServiceNow Hermes Kafka Connector as a Confluent Cloud Custom Connector.
#
# Usage: sn-confluent deploy sink   [--config PATH] [--plugin-id ID | --plugin-file PATH]
#                                   [--cloud {aws,gcp,azure}] [--no-wizard] [--dry-run]
#        sn-confluent deploy source [same flags]

import argparse
import base64
import getpass
import glob as _glob
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from typing import List, Optional, Tuple

from sn_confluent.core.pem import (
    check_confluent_cli,
    ensure_authenticated,
    SN_SOURCE_CLUSTERS,
    SN_BROKERS_PER_CLUSTER,
)
from sn_confluent.core.config import load_config as _core_load_config
from sn_confluent.core.hermes import HermesClient
from sn_confluent.core.confluent import list_cc_topics

SINK_CONNECTOR_CLASS = "com.servicenow.kafka.connect.hermes.HermesSinkConnector"
SOURCE_CONNECTOR_CLASS = "com.servicenow.kafka.connect.hermes.HermesSourceConnector"

SENSITIVE_PROPERTIES_CSV = (
    "hermes.ssl.keystore.b64,"
    "hermes.ssl.keystore.password,"
    "hermes.ssl.truststore.b64,"
    "hermes.ssl.truststore.password"
)

# Conventional artifact path relative to CWD (sibling repo layout)
_DEFAULT_PLUGIN_GLOB = (
    "../ServiceNow-source-and-sink-connector/"
    "target/components/packages/servicenow-hermes-kafka-connector-*.zip"
)

REQUIRED_KEYS = ("environment_id", "cluster_id", "connector_name")

_SENSITIVE_CONFIG_KEYS = frozenset({
    "hermes.ssl.keystore.b64",
    "hermes.ssl.truststore.b64",
    "kafka.api.secret",
    "hermes.ssl.keystore.password",
    "hermes.ssl.truststore.password",
})

SINK_EGRESS_NOTE = """\
Action required — configure Confluent Cloud egress endpoint:
  {instance}.service-now.com:4000-4007

In the Confluent Cloud Console:
  Connectors > {connector} > Networking > Add egress endpoint
"""

SOURCE_EGRESS_NOTE = """\
Action required — configure Confluent Cloud egress endpoints for two Hermes clusters:
  {instance}.service-now.com:4100-4103
  {instance}.service-now.com:4200-4203

In the Confluent Cloud Console:
  Connectors > {connector} > Networking > Add egress endpoint
"""


def load_config(path: str) -> dict:
    return _core_load_config(path, REQUIRED_KEYS)


def _version_key(path: str) -> tuple:
    return tuple(int(n) for n in re.findall(r'\d+', os.path.basename(path)))


def find_plugin_file(configured_path: Optional[str]) -> Optional[str]:
    """Return the connector ZIP path, or None if not found."""
    if configured_path:
        return configured_path if os.path.isfile(configured_path) else None
    matches = _glob.glob(_DEFAULT_PLUGIN_GLOB)
    if matches:
        return max(matches, key=_version_key)
    return None


def encode_p12_file(path: str) -> str:
    """Read a PKCS12 file and return it base64-encoded."""
    try:
        with open(path, "rb") as fh:
            return base64.b64encode(fh.read()).decode("ascii")
    except OSError as exc:
        print(f"Error: Cannot read file {path}: {exc}", file=sys.stderr)
        sys.exit(1)


def resolve_keystore(cfg: dict, b64_key: str, path_key: str) -> str:
    """Return base64 keystore: from an already-encoded value in config, or by encoding a file."""
    b64_val = cfg.get(b64_key, "").strip()
    if b64_val:
        return b64_val
    path_val = cfg.get(path_key, "").strip()
    if path_val:
        return encode_p12_file(path_val)
    return ""


def validate_keystores(
    keystore_b64: str,
    keystore_password: str,
    truststore_b64: str,
    truststore_password: str,
) -> bool:
    """Decode both PKCS12 stores and print cert details. Returns False and prints error on failure."""
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.exceptions import InvalidTag

    ok = True
    for label, b64, password in [
        ("keystore",    keystore_b64,    keystore_password),
        ("truststore",  truststore_b64,  truststore_password),
    ]:
        try:
            raw = base64.b64decode(b64)
            key, cert, extra = pkcs12.load_key_and_certificates(
                raw, password.encode("utf-8")
            )
        except (ValueError, InvalidTag):
            print(f"Error: {label}: wrong password or corrupt file.", file=sys.stderr)
            ok = False
            continue
        except Exception as exc:
            print(f"Error: {label}: {exc}", file=sys.stderr)
            ok = False
            continue

        certs = ([cert] if cert else []) + list(extra or [])
        for i, c in enumerate(certs):
            tag = "client cert" if i == 0 and key else f"CA cert [{i}]"
            print(f"  {label} {tag}: {c.subject.rfc4514_string()}")
            expiry = getattr(c, "not_valid_after_utc", None) or c.not_valid_after
            print(f"    expires: {expiry}")
        if key:
            print(f"  {label} private key: present")
        else:
            print(f"  {label} private key: none (CA/trust store)")

    return ok


def resolve_api_credentials(
    cfg: dict,
    environment_id: str = "",
    cluster_id: str = "",
) -> tuple:
    """Resolve Kafka API key/secret for the connector.

    Order: CC_API_KEY/CC_API_SECRET env vars -> config kafka_api_key/kafka_api_secret
    -> auto-generate via confluent api-key create (when environment_id + cluster_id are
    available) -> interactive prompt.
    """
    api_key = os.environ.get("CC_API_KEY", "").strip() or cfg.get("kafka_api_key", "").strip()
    api_secret = os.environ.get("CC_API_SECRET", "").strip() or cfg.get("kafka_api_secret", "").strip()

    if not api_key or not api_secret:
        if environment_id and cluster_id:
            from sn_confluent.api_key.main import create_kafka_api_key
            print(
                f"No Kafka API key configured — generating one for cluster {cluster_id}...",
                flush=True,
            )
            api_key, api_secret = create_kafka_api_key(
                environment_id=environment_id,
                cluster_id=cluster_id,
                description="sn-confluent deploy (auto-generated)",
            )
            print(f"Generated API key: {api_key}")
            print("Add to deploy.conf to reuse on subsequent runs:")
            print(f"  kafka_api_key    = {api_key}")
            print(f"  kafka_api_secret = {api_secret}")
        else:
            if not api_key:
                api_key = input("Confluent Cloud API Key: ").strip()
            if not api_secret:
                api_secret = getpass.getpass("Confluent Cloud API Secret: ").strip()

    if not api_key or not api_secret:
        print(
            "Error: Confluent Cloud API key and secret are required.\n"
            "Set CC_API_KEY / CC_API_SECRET env vars, or add kafka_api_key / "
            "kafka_api_secret to deploy.conf.",
            file=sys.stderr,
        )
        sys.exit(1)
    return api_key, api_secret


def _extract_pem_from_p12(
    keystore_b64: str,
    keystore_password: str,
    truststore_b64: str,
    truststore_password: str,
) -> Tuple[bytes, bytes, bytes]:
    """Return (ca_pem, client_cert_pem, client_key_pem) extracted from PKCS12 bytes.

    Uses the cryptography library directly on in-memory bytes — no temp files.
    """
    from cryptography.hazmat.primitives.serialization import (
        pkcs12, Encoding, PrivateFormat, NoEncryption,
    )

    def _load(b64: str, password: str):
        raw = base64.b64decode(b64)
        pw = password.encode("utf-8")
        return pkcs12.load_key_and_certificates(raw, pw)

    _key, ts_cert, ts_extra = _load(truststore_b64, truststore_password)
    ca_certs = ([ts_cert] if ts_cert else []) + list(ts_extra or [])
    ca_pem = b"".join(c.public_bytes(Encoding.PEM) for c in ca_certs)

    ks_key, ks_cert, _extra = _load(keystore_b64, keystore_password)
    if ks_cert is None:
        raise ValueError("keystore PKCS12 contains no certificate")
    client_cert_pem = ks_cert.public_bytes(Encoding.PEM)
    client_key_pem = ks_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())

    return ca_pem, client_cert_pem, client_key_pem


def _resolve_hermes_pem(
    cfg: dict,
    pem_dir: Optional[str],
) -> Optional[Tuple[bytes, bytes, bytes]]:
    """Return (ca_pem, client_cert_pem, client_key_pem) bytes for Hermes AdminClient.

    Tries pem_dir (extracted PEM files) first; falls back to in-memory PKCS12 extraction.
    Returns None if neither source is available or extraction fails.
    """
    if pem_dir:
        try:
            from sn_confluent.core.pem import load_pem_files
            return load_pem_files(pem_dir)
        except (Exception, SystemExit):
            return None

    keystore_b64 = resolve_keystore(cfg, "keystore_b64", "keystore_path")
    truststore_b64 = resolve_keystore(cfg, "truststore_b64", "truststore_path")
    if not keystore_b64 or not truststore_b64:
        return None
    ks_password = cfg.get("keystore_password", "")
    ts_password = cfg.get("truststore_password", "")
    try:
        return _extract_pem_from_p12(keystore_b64, ks_password, truststore_b64, ts_password)
    except Exception as exc:
        print(f"Warning: Could not decode keystores for Hermes connection: {exc}", file=sys.stderr)
        return None


def upload_plugin(
    plugin_file: str,
    plugin_name: str,
    cloud: str,
    connector_type: str,
    connector_class: str,
) -> str:
    """Upload the connector ZIP as a Confluent Cloud custom plugin. Returns plugin ID."""
    print(f"Uploading plugin from {plugin_file}...")
    result = subprocess.run(
        [
            "confluent", "connect", "custom-plugin", "create", plugin_name,
            "--plugin-file", plugin_file,
            "--connector-type", connector_type,
            "--connector-class", connector_class,
            "--sensitive-properties", SENSITIVE_PROPERTIES_CSV,
            "--cloud", cloud,
            "--output", "json",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error: Plugin upload failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    try:
        data = json.loads(result.stdout)
        plugin_id = data.get("id") or data.get("plugin_id")
        if not plugin_id:
            raise KeyError("id")
    except (json.JSONDecodeError, KeyError) as exc:
        print(f"Error: Unexpected plugin create output ({exc}): {result.stdout[:400]}", file=sys.stderr)
        sys.exit(1)
    print(f"Plugin uploaded: {plugin_id}")
    return plugin_id


def update_plugin(plugin_id: str, plugin_file: str) -> None:
    """Update an existing custom plugin with a new JAR/ZIP without changing its ID."""
    print(f"Updating plugin {plugin_id} from {plugin_file}...")
    result = subprocess.run(
        [
            "confluent", "connect", "custom-plugin", "update", plugin_id,
            "--plugin-file", plugin_file,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error: Plugin update failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    print(f"Plugin {plugin_id} updated.")


def build_sink_config(
    plugin_id: str,
    connector_name: str,
    topics: str,
    api_key: str,
    api_secret: str,
    instance_name: str,
    hermes_topic: str,
    keystore_b64: str,
    keystore_password: str,
    truststore_b64: str,
    truststore_password: str,
    tasks_max: int = 1,
) -> dict:
    return {
        "name": connector_name,
        "connector.class": SINK_CONNECTOR_CLASS,
        "confluent.custom.plugin.id": plugin_id,
        "confluent.connector.type": "CUSTOM",
        "tasks.max": str(tasks_max),
        "topics": topics,
        "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "kafka.auth.mode": "KAFKA_API_KEY",
        "kafka.api.key": api_key,
        "kafka.api.secret": api_secret,
        "hermes.instance.name": instance_name,
        "hermes.topic": hermes_topic,
        "hermes.ssl.keystore.b64": keystore_b64,
        "hermes.ssl.keystore.password": keystore_password,
        "hermes.ssl.truststore.b64": truststore_b64,
        "hermes.ssl.truststore.password": truststore_password,
        "confluent.custom.connection.endpoints": (
            f"{instance_name}.service-now.com:"
            + ",".join(str(p) for p in range(4000, 4008))
        ),
    }


def build_source_config(
    plugin_id: str,
    connector_name: str,
    destination_topic: str,
    api_key: str,
    api_secret: str,
    instance_name: str,
    hermes_source_topic: str,
    keystore_b64: str,
    keystore_password: str,
    truststore_b64: str,
    truststore_password: str,
    tasks_max: int = 1,
    consumer_group_id: str = "hermes-connect-source",
    endpoints: Optional[str] = None,
) -> dict:
    return {
        "name": connector_name,
        "connector.class": SOURCE_CONNECTOR_CLASS,
        "confluent.custom.plugin.id": plugin_id,
        "confluent.connector.type": "CUSTOM",
        "tasks.max": str(tasks_max),
        "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
        "kafka.auth.mode": "KAFKA_API_KEY",
        "kafka.api.key": api_key,
        "kafka.api.secret": api_secret,
        "hermes.instance.name": instance_name,
        "hermes.source.topic": hermes_source_topic,
        "hermes.consumer.group.id": consumer_group_id,
        "hermes.destination.topic": destination_topic,
        "hermes.ssl.keystore.b64": keystore_b64,
        "hermes.ssl.keystore.password": keystore_password,
        "hermes.ssl.truststore.b64": truststore_b64,
        "hermes.ssl.truststore.password": truststore_password,
        "confluent.custom.connection.endpoints": endpoints or ",".join(
            f"{instance_name if '.' in instance_name else instance_name + '.service-now.com'}:{base + i}"
            for base in SN_SOURCE_CLUSTERS
            for i in range(SN_BROKERS_PER_CLUSTER)
        ),
    }


def create_connector(environment_id: str, cluster_id: str, config: dict) -> str:
    """Create the connector via confluent CLI. Returns connector ID (or name on parse failure)."""
    wrapped = {"name": config["name"], "config": config}
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as fh:
        json.dump(wrapped, fh, indent=2)
        tmp_path = fh.name

    try:
        result = subprocess.run(
            [
                "confluent", "connect", "cluster", "create",
                "--config-file", tmp_path,
                "--environment", environment_id,
                "--cluster", cluster_id,
                "--output", "json",
            ],
            capture_output=True,
            text=True,
        )
    finally:
        os.unlink(tmp_path)

    if result.returncode != 0:
        print(f"Error: Connector creation failed: {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)

    try:
        data = json.loads(result.stdout)
        conn_id = data.get("id") or data.get("connector", {}).get("id")
        return conn_id if conn_id else config["name"]
    except (json.JSONDecodeError, AttributeError):
        return config["name"]


def poll_connector(
    environment_id: str,
    cluster_id: str,
    connector_id: str,
    timeout: int = 900,
    interval: int = 5,
) -> None:
    """Poll connector status until RUNNING or FAILED."""
    deadline = time.monotonic() + timeout
    print(f"Waiting for connector '{connector_id}' (timeout: {timeout}s)...", flush=True)

    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                "confluent", "connect", "cluster", "describe", connector_id,
                "--environment", environment_id,
                "--cluster", cluster_id,
                "--output", "json",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("  (status check failed, retrying...)", flush=True)
            time.sleep(interval)
            continue

        try:
            data = json.loads(result.stdout)
            # CLI v4.x returns {"connector": {"status": "RUNNING", ...}}
            # Older versions returned {"status": {"state": "RUNNING"}}
            raw = (
                data.get("connector", {}).get("status")
                or data.get("status")
            )
            if isinstance(raw, str):
                state = raw.upper()
            elif isinstance(raw, dict):
                state = raw.get("state", "UNKNOWN").upper()
            else:
                state = "UNKNOWN"
        except (json.JSONDecodeError, AttributeError):
            state = "UNKNOWN"

        print(f"  State: {state}", flush=True)

        if state == "RUNNING":
            return
        if state == "FAILED":
            print(f"Error: Connector '{connector_id}' FAILED.", file=sys.stderr)
            sys.exit(1)

        time.sleep(interval)

    print(f"Error: Timed out after {timeout}s waiting for '{connector_id}'.", file=sys.stderr)
    sys.exit(1)


def _mask(config: dict) -> dict:
    return {k: "[hidden]" if k in _SENSITIVE_CONFIG_KEYS else v for k, v in config.items()}


def _build_arg_parser(description: str) -> argparse.ArgumentParser:
    """Shared argument parser for sink and source subcommands."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--config", default="deploy.conf",
        help="Path to deploy.conf (default: ./deploy.conf)",
    )
    parser.add_argument(
        "--plugin-id", default=None,
        help="Existing custom plugin ID — skip the upload step",
    )
    parser.add_argument(
        "--update-plugin", action="store_true", default=False,
        help="Update the plugin at --plugin-id (or plugin_id in deploy.conf) with a new JAR instead of creating a new plugin",
    )
    parser.add_argument(
        "--plugin-file", default=None,
        help="Connector ZIP path (overrides config; default: auto-detect in sibling repo)",
    )
    parser.add_argument(
        "--cloud", default=None, choices=("aws", "gcp", "azure"),
        help="Cloud provider for plugin upload (default: aws)",
    )
    parser.add_argument(
        "--timeout", type=int, default=900,
        help="Connector status poll timeout in seconds (default: 900)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the connector config JSON without creating anything",
    )
    parser.add_argument(
        "--pem-dir", default=None,
        help="Directory containing ca.pem, client-cert.pem, client-key.pem for Hermes topic listing",
    )
    parser.add_argument(
        "--no-wizard", action="store_true",
        help="Non-interactive: fail if any required value is missing from config",
    )
    return parser


def _run_sink_wizard(cfg: dict, missing: set) -> dict:
    """Prompt for any values absent from sink config. Returns the updated cfg dict."""
    try:
        import questionary
    except ImportError:
        print(
            "Error: questionary is required for interactive mode.\n"
            "Install it: pip install questionary\n"
            "Or use --no-wizard with a fully populated deploy.conf.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("\n--- Hermes Sink Connector Deployment Wizard ---\n")

    # Collect instance name and keystores first — needed for Hermes topic discovery.
    if "instance_name" in missing:
        cfg["instance_name"] = questionary.text(
            "ServiceNow instance name (bare, e.g. myinstance):"
        ).ask()

    if "keystore" in missing:
        source = questionary.select(
            "Provide keystore as:",
            choices=["File path (encode automatically)", "Base64 string (paste)"],
        ).ask()
        if source and "File path" in source:
            cfg["keystore_path"] = questionary.text("Path to keystore.p12:").ask()
        else:
            cfg["keystore_b64"] = questionary.password("Base64-encoded keystore.p12:").ask()

    if "keystore_password" in missing:
        cfg["keystore_password"] = questionary.password("Keystore password:").ask()

    if "truststore" in missing:
        source = questionary.select(
            "Provide truststore as:",
            choices=["File path (encode automatically)", "Base64 string (paste)"],
        ).ask()
        if source and "File path" in source:
            cfg["truststore_path"] = questionary.text("Path to truststore.p12:").ask()
        else:
            cfg["truststore_b64"] = questionary.password("Base64-encoded truststore.p12:").ask()

    if "truststore_password" in missing:
        cfg["truststore_password"] = questionary.password("Truststore password:").ask()

    # CC topic picker — sink reads FROM Confluent Cloud
    if "topics" in missing:
        print("Fetching topics from Confluent Cloud...")
        cc_topics = list_cc_topics(cfg["environment_id"], cfg["cluster_id"])
        if cc_topics:
            selected = questionary.checkbox(
                "Select Confluent Cloud topic(s) to read from:",
                choices=cc_topics,
            ).ask()
            cfg["topics"] = ",".join(selected) if selected else ""
        else:
            cfg["topics"] = questionary.text(
                "Confluent Cloud topic(s) to read from (comma-separated):"
            ).ask()

    # Hermes destination topic picker — sink writes TO Hermes
    if "hermes_topic" in missing:
        instance_name = cfg.get("instance_name", "")
        hermes_pem = _resolve_hermes_pem(cfg, cfg.get("_pem_dir"))
        if hermes_pem and instance_name:
            print(f"Fetching topics from Hermes ({instance_name}.service-now.com)...")
            hermes_topics = HermesClient(instance_name, *hermes_pem).list_topics()
        else:
            hermes_topics = None

        if hermes_topics:
            selected = questionary.select(
                "Select Hermes destination topic:",
                choices=hermes_topics,
            ).ask()
            cfg["hermes_topic"] = selected or ""
        else:
            default = f"snc.{instance_name or 'myinstance'}.sn_streamconnect.my-topic"
            cfg["hermes_topic"] = questionary.text(
                "Hermes topic to write to:", default=default,
            ).ask()

    return cfg


def _run_source_wizard(cfg: dict, missing: set) -> dict:
    """Prompt for any values absent from source config. Returns the updated cfg dict."""
    try:
        import questionary
    except ImportError:
        print(
            "Error: questionary is required for interactive mode.\n"
            "Install it: pip install questionary\n"
            "Or use --no-wizard with a fully populated deploy.conf.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("\n--- Hermes Source Connector Deployment Wizard ---\n")

    # Collect instance name and keystores first — needed for Hermes topic discovery.
    if "instance_name" in missing:
        cfg["instance_name"] = questionary.text(
            "ServiceNow instance name (bare, e.g. myinstance):"
        ).ask()

    if "keystore" in missing:
        source = questionary.select(
            "Provide keystore as:",
            choices=["File path (encode automatically)", "Base64 string (paste)"],
        ).ask()
        if source and "File path" in source:
            cfg["keystore_path"] = questionary.text("Path to keystore.p12:").ask()
        else:
            cfg["keystore_b64"] = questionary.password("Base64-encoded keystore.p12:").ask()

    if "keystore_password" in missing:
        cfg["keystore_password"] = questionary.password("Keystore password:").ask()

    if "truststore" in missing:
        source = questionary.select(
            "Provide truststore as:",
            choices=["File path (encode automatically)", "Base64 string (paste)"],
        ).ask()
        if source and "File path" in source:
            cfg["truststore_path"] = questionary.text("Path to truststore.p12:").ask()
        else:
            cfg["truststore_b64"] = questionary.password("Base64-encoded truststore.p12:").ask()

    if "truststore_password" in missing:
        cfg["truststore_password"] = questionary.password("Truststore password:").ask()

    # Hermes source topic picker — source reads FROM Hermes (source-peer clusters at 4100+)
    if "hermes_topic" in missing:
        instance_name = cfg.get("instance_name", "")
        hermes_pem = _resolve_hermes_pem(cfg, cfg.get("_pem_dir"))
        if hermes_pem and instance_name:
            print(f"Fetching topics from Hermes ({instance_name}.service-now.com)...")
            hermes_topics = HermesClient(instance_name, *hermes_pem).list_topics(
                base_port=SN_SOURCE_CLUSTERS[0]
            )
        else:
            hermes_topics = None

        if hermes_topics:
            selected = questionary.select(
                "Select Hermes topic to read from:",
                choices=hermes_topics,
            ).ask()
            cfg["hermes_topic"] = selected or ""
        else:
            default = f"snc.{instance_name or 'myinstance'}.sn_streamconnect.my-topic"
            cfg["hermes_topic"] = questionary.text(
                "Hermes topic to read from:", default=default,
            ).ask()

    # CC topic picker — source writes TO Confluent Cloud
    if "topics" in missing:
        print("Fetching topics from Confluent Cloud...")
        cc_topics = list_cc_topics(cfg["environment_id"], cfg["cluster_id"])
        if cc_topics:
            selected = questionary.select(
                "Select Confluent Cloud topic to write to:",
                choices=cc_topics,
            ).ask()
            cfg["topics"] = selected or ""
        else:
            cfg["topics"] = questionary.text(
                "Confluent Cloud topic to write to:"
            ).ask()

    return cfg


def _sink_main(argv: Optional[List[str]] = None) -> int:
    parser = _build_arg_parser(
        "Deploy the ServiceNow HermesSinkConnector to Confluent Cloud."
        " Data flows: Confluent Cloud topics -> ServiceNow Hermes."
    )
    args = parser.parse_args(argv)

    # 1. Load config
    cfg = load_config(args.config)

    # 2. Check confluent CLI is on PATH and authenticated
    ensure_authenticated(cfg)

    environment_id = cfg["environment_id"]
    cluster_id = cfg["cluster_id"]
    connector_name = cfg["connector_name"]
    cloud = args.cloud or cfg.get("cloud", "aws").strip() or "aws"
    tasks_max = max(1, int(cfg.get("tasks_max", "1") or "1"))

    # 3. Resolve plugin ID (upload if not already known)
    plugin_id = args.plugin_id or cfg.get("plugin_id", "").strip()

    if args.update_plugin:
        if not plugin_id:
            print("Error: --update-plugin requires --plugin-id or plugin_id in deploy.conf.", file=sys.stderr)
            sys.exit(1)
        plugin_file = args.plugin_file or find_plugin_file(cfg.get("plugin_file", "").strip() or None)
        if not plugin_file:
            print(
                "Error: Connector ZIP not found.\n"
                "Build it first:\n"
                "  cd ../ServiceNow-source-and-sink-connector && mvn package -DskipTests\n"
                "Or pass --plugin-file <path>.",
                file=sys.stderr,
            )
            sys.exit(1)
        if not args.dry_run:
            update_plugin(plugin_id, plugin_file)
        else:
            print(f"[dry-run] Would update plugin {plugin_id} from: {os.path.abspath(plugin_file)}")
        return 0
    elif not plugin_id:
        plugin_file = args.plugin_file or find_plugin_file(cfg.get("plugin_file", "").strip() or None)
        if not plugin_file:
            print(
                "Error: Connector ZIP not found.\n"
                "Build it first:\n"
                "  cd ../ServiceNow-source-and-sink-connector && mvn package -DskipTests\n"
                "Or pass --plugin-file <path> or set plugin_id in deploy.conf to skip upload.",
                file=sys.stderr,
            )
            sys.exit(1)
        if args.dry_run:
            print(f"[dry-run] Would upload plugin from: {os.path.abspath(plugin_file)}")
            plugin_id = "ccp-dryrun"
        else:
            plugin_name = cfg.get("plugin_name", "").strip() or f"{connector_name}-plugin"
            plugin_id = upload_plugin(
                plugin_file, plugin_name, cloud,
                connector_type="sink",
                connector_class=SINK_CONNECTOR_CLASS,
            )

    # 4. Resolve Confluent Cloud API credentials (used in kafka.api.key/secret)
    api_key, api_secret = resolve_api_credentials(cfg, environment_id, cluster_id)

    # 5. Resolve keystore/truststore as base64
    pem_dir = args.pem_dir or cfg.get("pem_dir", "").strip() or None
    cfg["_pem_dir"] = pem_dir  # pass through to wizard via cfg dict

    keystore_b64 = resolve_keystore(cfg, "keystore_b64", "keystore_path")
    truststore_b64 = resolve_keystore(cfg, "truststore_b64", "truststore_path")
    keystore_password = cfg.get("keystore_password", "").strip()
    truststore_password = cfg.get("truststore_password", "").strip()

    topics = cfg.get("topics", "").strip()
    instance_name = cfg.get("instance_name", "").strip()
    hermes_topic = cfg.get("hermes_topic", "").strip()

    # 6. Identify missing values.
    missing = set()
    if not topics:
        missing.add("topics")
    if not instance_name:
        missing.add("instance_name")
    if not hermes_topic:
        missing.add("hermes_topic")
    if not keystore_b64:
        missing.add("keystore")
    if not keystore_password:
        missing.add("keystore_password")
    if not truststore_b64:
        missing.add("truststore")
    if not truststore_password:
        missing.add("truststore_password")

    if missing:
        if args.no_wizard:
            for key in sorted(missing):
                print(f"Error: Missing required value: {key}", file=sys.stderr)
            sys.exit(1)
        cfg = _run_sink_wizard(cfg, missing)
        keystore_b64 = resolve_keystore(cfg, "keystore_b64", "keystore_path")
        truststore_b64 = resolve_keystore(cfg, "truststore_b64", "truststore_path")
        keystore_password = cfg.get("keystore_password", "").strip()
        truststore_password = cfg.get("truststore_password", "").strip()
        topics = cfg.get("topics", "").strip()
        instance_name = cfg.get("instance_name", "").strip()
        hermes_topic = cfg.get("hermes_topic", "").strip()

        if not topics:
            print("Error: At least one topic is required.", file=sys.stderr)
            sys.exit(1)
        if not hermes_topic:
            print("Error: hermes_topic is required.", file=sys.stderr)
            sys.exit(1)

    # 7. Validate keystores
    if keystore_b64 and truststore_b64:
        print("Validating keystores...")
        if not validate_keystores(keystore_b64, keystore_password, truststore_b64, truststore_password):
            sys.exit(1)

    # 8. Build connector config
    connector_cfg = build_sink_config(
        plugin_id=plugin_id,
        connector_name=connector_name,
        topics=topics,
        api_key=api_key,
        api_secret=api_secret,
        instance_name=instance_name,
        hermes_topic=hermes_topic,
        keystore_b64=keystore_b64,
        keystore_password=keystore_password,
        truststore_b64=truststore_b64,
        truststore_password=truststore_password,
        tasks_max=tasks_max,
    )

    # 9. Dry-run: show masked config and exit
    if args.dry_run:
        print("--- Connector config (dry-run) ---")
        print(json.dumps(_mask(connector_cfg), indent=2))
        print(
            f"\nWould run: confluent connect cluster create --config-file <config.json>"
            f" --environment {environment_id} --cluster {cluster_id}"
        )
        print(SINK_EGRESS_NOTE.format(instance=instance_name or "<instance>", connector=connector_name))
        return 0

    # 10. Wizard confirmation
    if not args.no_wizard:
        try:
            import questionary
            print("\n--- Connector config preview ---")
            print(json.dumps(_mask(connector_cfg), indent=2))
            if not questionary.confirm("Create connector?", default=False).ask():
                print("Aborted.")
                return 0
        except ImportError:
            pass

    # 11. Create connector
    print(f"Creating connector '{connector_name}'...")
    connector_id = create_connector(environment_id, cluster_id, connector_cfg)
    print(f"Connector created: {connector_id}")

    # 12. Poll until RUNNING
    poll_connector(environment_id, cluster_id, connector_id, timeout=args.timeout)
    print(f"Connector '{connector_id}' is RUNNING.")

    # 13. Remind about egress endpoints
    print(SINK_EGRESS_NOTE.format(instance=instance_name, connector=connector_name))

    return 0


def _source_main(argv: Optional[List[str]] = None) -> int:
    parser = _build_arg_parser(
        "Deploy the ServiceNow HermesSourceConnector to Confluent Cloud."
        " Data flows: ServiceNow Hermes -> Confluent Cloud topics."
    )
    args = parser.parse_args(argv)

    # 1. Load config
    cfg = load_config(args.config)

    # 2. Check confluent CLI is on PATH and authenticated
    ensure_authenticated(cfg)

    environment_id = cfg["environment_id"]
    cluster_id = cfg["cluster_id"]
    connector_name = cfg["connector_name"]
    cloud = args.cloud or cfg.get("cloud", "aws").strip() or "aws"
    tasks_max = max(1, int(cfg.get("tasks_max", "1") or "1"))

    # 3. Resolve plugin ID (upload if not already known)
    plugin_id = args.plugin_id or cfg.get("plugin_id", "").strip()

    if args.update_plugin:
        if not plugin_id:
            print("Error: --update-plugin requires --plugin-id or plugin_id in deploy.conf.", file=sys.stderr)
            sys.exit(1)
        plugin_file = args.plugin_file or find_plugin_file(cfg.get("plugin_file", "").strip() or None)
        if not plugin_file:
            print(
                "Error: Connector ZIP not found.\n"
                "Build it first:\n"
                "  cd ../ServiceNow-source-and-sink-connector && mvn package -DskipTests\n"
                "Or pass --plugin-file <path>.",
                file=sys.stderr,
            )
            sys.exit(1)
        if not args.dry_run:
            update_plugin(plugin_id, plugin_file)
        else:
            print(f"[dry-run] Would update plugin {plugin_id} from: {os.path.abspath(plugin_file)}")
        return 0
    elif not plugin_id:
        plugin_file = args.plugin_file or find_plugin_file(cfg.get("plugin_file", "").strip() or None)
        if not plugin_file:
            print(
                "Error: Connector ZIP not found.\n"
                "Build it first:\n"
                "  cd ../ServiceNow-source-and-sink-connector && mvn package -DskipTests\n"
                "Or pass --plugin-file <path> or set plugin_id in deploy.conf to skip upload.",
                file=sys.stderr,
            )
            sys.exit(1)
        if args.dry_run:
            print(f"[dry-run] Would upload plugin from: {os.path.abspath(plugin_file)}")
            plugin_id = "ccp-dryrun"
        else:
            plugin_name = cfg.get("plugin_name", "").strip() or f"{connector_name}-plugin"
            plugin_id = upload_plugin(
                plugin_file, plugin_name, cloud,
                connector_type="source",
                connector_class=SOURCE_CONNECTOR_CLASS,
            )

    # 4. Resolve Confluent Cloud API credentials
    api_key, api_secret = resolve_api_credentials(cfg, environment_id, cluster_id)

    # 5. Resolve keystore/truststore as base64
    pem_dir = args.pem_dir or cfg.get("pem_dir", "").strip() or None
    cfg["_pem_dir"] = pem_dir

    keystore_b64 = resolve_keystore(cfg, "keystore_b64", "keystore_path")
    truststore_b64 = resolve_keystore(cfg, "truststore_b64", "truststore_path")
    keystore_password = cfg.get("keystore_password", "").strip()
    truststore_password = cfg.get("truststore_password", "").strip()

    destination_topic = cfg.get("topics", "").strip()
    instance_name = cfg.get("instance_name", "").strip()
    hermes_source_topic = cfg.get("hermes_topic", "").strip()
    consumer_group_id = cfg.get("consumer_group_id", "hermes-connect-source").strip() or "hermes-connect-source"

    # 6. Identify missing values.
    missing = set()
    if not destination_topic:
        missing.add("topics")
    if not instance_name:
        missing.add("instance_name")
    if not hermes_source_topic:
        missing.add("hermes_topic")
    if not keystore_b64:
        missing.add("keystore")
    if not keystore_password:
        missing.add("keystore_password")
    if not truststore_b64:
        missing.add("truststore")
    if not truststore_password:
        missing.add("truststore_password")

    if missing:
        if args.no_wizard:
            for key in sorted(missing):
                print(f"Error: Missing required value: {key}", file=sys.stderr)
            sys.exit(1)
        cfg = _run_source_wizard(cfg, missing)
        keystore_b64 = resolve_keystore(cfg, "keystore_b64", "keystore_path")
        truststore_b64 = resolve_keystore(cfg, "truststore_b64", "truststore_path")
        keystore_password = cfg.get("keystore_password", "").strip()
        truststore_password = cfg.get("truststore_password", "").strip()
        destination_topic = cfg.get("topics", "").strip()
        instance_name = cfg.get("instance_name", "").strip()
        hermes_source_topic = cfg.get("hermes_topic", "").strip()

        if not destination_topic:
            print("Error: A destination Confluent Cloud topic is required.", file=sys.stderr)
            sys.exit(1)
        if not hermes_source_topic:
            print("Error: hermes_topic is required.", file=sys.stderr)
            sys.exit(1)

    # 7. Validate keystores
    if keystore_b64 and truststore_b64:
        print("Validating keystores...")
        if not validate_keystores(keystore_b64, keystore_password, truststore_b64, truststore_password):
            sys.exit(1)

    # 7b. Probe actual source cluster broker topology to build precise egress list
    source_endpoints = None
    if keystore_b64 and truststore_b64 and instance_name:
        hermes_pem = _resolve_hermes_pem(cfg, pem_dir)
        if hermes_pem:
            print(f"Probing Hermes source cluster topology for {instance_name}...")
            source_endpoints = HermesClient(instance_name, *hermes_pem).source_egress_endpoints()
            if source_endpoints:
                print(f"Discovered egress endpoints: {source_endpoints}")
            else:
                print(
                    "Warning: Could not discover source cluster topology; "
                    "falling back to default egress ranges (4100-4103, 4200-4203).",
                    file=sys.stderr,
                )

    # 8. Build connector config
    connector_cfg = build_source_config(
        plugin_id=plugin_id,
        connector_name=connector_name,
        destination_topic=destination_topic,
        api_key=api_key,
        api_secret=api_secret,
        instance_name=instance_name,
        hermes_source_topic=hermes_source_topic,
        keystore_b64=keystore_b64,
        keystore_password=keystore_password,
        truststore_b64=truststore_b64,
        truststore_password=truststore_password,
        tasks_max=tasks_max,
        consumer_group_id=consumer_group_id,
        endpoints=source_endpoints,
    )

    # 9. Dry-run: show masked config and exit
    if args.dry_run:
        print("--- Connector config (dry-run) ---")
        print(json.dumps(_mask(connector_cfg), indent=2))
        print(
            f"\nWould run: confluent connect cluster create --config-file <config.json>"
            f" --environment {environment_id} --cluster {cluster_id}"
        )
        print(SOURCE_EGRESS_NOTE.format(instance=instance_name or "<instance>", connector=connector_name))
        return 0

    # 10. Wizard confirmation
    if not args.no_wizard:
        try:
            import questionary
            print("\n--- Connector config preview ---")
            print(json.dumps(_mask(connector_cfg), indent=2))
            if not questionary.confirm("Create connector?", default=False).ask():
                print("Aborted.")
                return 0
        except ImportError:
            pass

    # 11. Create connector
    print(f"Creating connector '{connector_name}'...")
    connector_id = create_connector(environment_id, cluster_id, connector_cfg)
    print(f"Connector created: {connector_id}")

    # 12. Poll until RUNNING
    poll_connector(environment_id, cluster_id, connector_id, timeout=args.timeout)
    print(f"Connector '{connector_id}' is RUNNING.")

    # 13. Remind about egress endpoints
    print(SOURCE_EGRESS_NOTE.format(instance=instance_name, connector=connector_name))

    return 0


def _print_deploy_help() -> None:
    print("usage: sn-confluent deploy <subcommand> [<args>]")
    print()
    print("Deploy the ServiceNow Hermes Kafka Connector to Confluent Cloud.")
    print()
    print("Subcommands:")
    print("  sink    Deploy HermesSinkConnector   (Confluent Cloud topics -> ServiceNow Hermes)")
    print("  source  Deploy HermesSourceConnector (ServiceNow Hermes -> Confluent Cloud topics)")
    print()
    print("Run 'sn-confluent deploy <subcommand> --help' for subcommand-specific options.")


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv and argv[0] in ("-h", "--help"):
        _print_deploy_help()
        return 0
    if not argv:
        _print_deploy_help()
        return 2
    sub, rest = argv[0], argv[1:]
    if sub == "sink":
        return _sink_main(rest)
    elif sub == "source":
        return _source_main(rest)
    print(f"sn-confluent deploy: unknown subcommand '{sub}'", file=sys.stderr)
    _print_deploy_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
