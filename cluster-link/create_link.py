# Create a Confluent Cloud Cluster Link using PEM files from extract_pem.py.
#
# Setup: pip install -r requirements.txt  (no extra deps beyond cryptography)
# Usage: python create_link.py [--config PATH] [--pem-dir PATH] [--dry-run] [--timeout SECS]
#
# Prereqs:
#   1. Confluent CLI installed and 'confluent login' completed
#   2. PEM files generated:  python extract_pem.py
#   3. link.conf filled in with your environment/cluster IDs

import argparse
import configparser
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time


REQUIRED_KEYS = ("environment_id", "cluster_id", "link_name")

# ServiceNow active-active: base port for each source cluster, 4 brokers each.
# These are the defaults; both can be overridden in link.conf.
SN_SOURCE_CLUSTERS = [4100, 4200]
SN_BROKERS_PER_CLUSTER = 4


def sn_bootstrap(host: str, base_port: int, brokers_per_cluster: int = SN_BROKERS_PER_CLUSTER) -> str:
    """Build a bootstrap string for one ServiceNow source cluster."""
    return ",".join(f"{host}:{base_port + i}" for i in range(brokers_per_cluster))

CONFLUENT_INSTALL = """\
Confluent CLI not found. Install it:

  Windows : winget install Confluent.ConfluentCLI
  macOS   : brew install confluentinc/tap/confluent-cli
  Linux   : See https://docs.confluent.io/confluent-cli/current/install.html

Then authenticate: confluent login
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
    for key in REQUIRED_KEYS:
        if key not in section:
            print(f"Error: Missing key in link.conf: {key}", file=sys.stderr)
            sys.exit(1)
    if "source_bootstrap" not in section and "source_host" not in section:
        print(
            "Error: Missing key in link.conf: source_bootstrap "
            "(or source_host for ServiceNow dual-cluster mode)",
            file=sys.stderr,
        )
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
    """Exit 1 with install instructions if 'confluent' is not in PATH."""
    if shutil.which("confluent") is None:
        print(CONFLUENT_INSTALL)
        sys.exit(1)


def check_auth(environment_id: str, cluster_id: str) -> None:
    """Verify Confluent CLI auth by describing the destination cluster."""
    result = subprocess.run(
        [
            "confluent", "kafka", "cluster", "describe",
            cluster_id,
            "--environment", environment_id,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            "Error: Not authenticated or cluster not accessible. Run: confluent login",
            file=sys.stderr,
        )
        sys.exit(1)


def load_pem_files(pem_dir: str):
    """Load ca.pem, client-cert.pem, client-key.pem; exit 1 if any are missing."""
    files = {
        "ca.pem": None,
        "client-cert.pem": None,
        "client-key.pem": None,
    }
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


def _inline(pem_bytes: bytes, literal_newlines: bool = False) -> str:
    """Encode PEM bytes as a Kafka properties value.

    literal_newlines=False (default): escapes newlines as \\n for properties files and CLI.
    literal_newlines=True: keeps actual newlines for paste into web console text areas.
    """
    text = pem_bytes.decode("utf-8").strip()
    if literal_newlines:
        return text
    return text.replace("\n", "\\n")


def build_ssl_properties(
    ca_pem: bytes,
    client_cert: bytes,
    client_key: bytes,
    literal_newlines: bool = False,
    key_password: str = None,
) -> str:
    """Return SSL properties block with inline PEM values."""
    inline = lambda b: _inline(b, literal_newlines)  # noqa: E731
    lines = [
        "security.protocol=SSL",
        "ssl.truststore.type=PEM",
        f"ssl.truststore.certificates={inline(ca_pem)}",
        "ssl.keystore.type=PEM",
        f"ssl.keystore.certificate.chain={inline(client_cert)}",
        f"ssl.keystore.key={inline(client_key)}",
    ]
    if key_password:
        lines.append(f"ssl.key.password={key_password}")
    lines.append("")  # trailing newline
    return "\n".join(lines)


def build_link_command(cfg: dict, properties_file: str) -> list:
    """Return the confluent kafka link create command as a list of strings."""
    cmd = [
        "confluent", "kafka", "link", "create", cfg["link_name"],
        "--environment", cfg["environment_id"],
        "--cluster", cfg["cluster_id"],
        "--source-bootstrap-server", cfg["source_bootstrap"],
        "--config-file", properties_file,
    ]
    return cmd


def run_link_create(command: list, dry_run: bool) -> None:
    """Execute the link create command, or print it if dry_run."""
    if dry_run:
        print("Dry run — would execute:")
        print("  " + " ".join(command))
        return
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    print(result.stdout)


def copy_security_config(ssl_props: str) -> None:
    """Copy the SSL properties block to the system clipboard."""
    try:
        import pyperclip
        pyperclip.copy(ssl_props)
    except ImportError:
        # Fallback: platform-native clipboard commands
        if sys.platform == "win32":
            subprocess.run("clip", input=ssl_props, text=True, check=True)
        elif sys.platform == "darwin":
            subprocess.run("pbcopy", input=ssl_props, text=True, check=True)
        else:
            subprocess.run(
                ["xclip", "-selection", "clipboard"],
                input=ssl_props, text=True, check=True,
            )
    print(
        "Security configuration copied to clipboard.\n"
        "Paste it into the Confluent Cloud web console under:\n"
        "  Cluster Linking > Create link > Source cluster configuration"
    )


def wait_for_link_active(cfg: dict, timeout: int = 60, interval: int = 5) -> None:
    """Poll link state until ACTIVE or FAILED, or timeout. Exits 1 on failure."""
    link_name = cfg["link_name"]
    deadline = time.monotonic() + timeout
    print(f"Waiting for link '{link_name}' to become ACTIVE (timeout: {timeout}s)...", flush=True)

    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                "confluent", "kafka", "link", "describe", link_name,
                "--environment", cfg["environment_id"],
                "--cluster", cfg["cluster_id"],
                "--output", "json",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("  (describe failed, retrying...)", flush=True)
            time.sleep(interval)
            continue

        try:
            data = json.loads(result.stdout)
            # CLI returns a list of config entries for link describe; the link-level
            # status lives in the top-level "link_state" key when output is a dict,
            # or in the first entry's value when it's a list of key/value pairs.
            if isinstance(data, dict):
                state = (data.get("state") or data.get("link_state") or "UNKNOWN").upper()
            else:
                state = "UNKNOWN"
        except (json.JSONDecodeError, AttributeError):
            state = "UNKNOWN"

        print(f"  LinkState: {state}", flush=True)

        if state == "ACTIVE":
            return
        if state == "FAILED":
            print(
                f"Error: Cluster Link '{link_name}' entered FAILED state. "
                "Run 'confluent kafka link describe' for details.",
                file=sys.stderr,
            )
            sys.exit(1)

        time.sleep(interval)

    print(
        f"Error: Timed out after {timeout}s waiting for link to become ACTIVE.",
        file=sys.stderr,
    )
    sys.exit(1)


def build_link_properties(port: int) -> str:
    """Return the cluster.link.prefix line for the given source port."""
    return f"cluster.link.prefix={port}.\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a Confluent Cloud Cluster Link using extracted PEM files."
    )
    parser.add_argument(
        "--config", default="link.conf",
        help="Path to link.conf (default: ./link.conf)",
    )
    parser.add_argument(
        "--pem-dir", default=".",
        help="Directory containing ca.pem, client-cert.pem, client-key.pem (default: ./)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the confluent CLI command without executing it",
    )
    parser.add_argument(
        "--timeout", type=int, default=60,
        help="Seconds to wait for link to become ACTIVE (default: 60)",
    )
    parser.add_argument(
        "--copy-config", action="store_true",
        help="Copy source cluster security configuration to clipboard (for web console); skips link creation",
    )
    parser.add_argument(
        "--literal-newlines", action="store_true",
        help="Use actual newlines in PEM values instead of \\\\n escapes (try this if the web console rejects the default format)",
    )
    parser.add_argument(
        "--key-password", default=None, metavar="PASSWORD",
        help="Password for an encrypted private key; adds ssl.key.password to the properties",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    ca_pem, client_cert, client_key = load_pem_files(args.pem_dir)

    if args.copy_config:
        ssl_props = build_ssl_properties(ca_pem, client_cert, client_key,
                                         literal_newlines=args.literal_newlines,
                                         key_password=args.key_password)
        copy_security_config(ssl_props)
        return

    check_confluent_cli()
    check_auth(cfg["environment_id"], cfg["cluster_id"])

    # Expand ServiceNow dual-cluster: one link per source cluster (4100, 4200).
    # Both links run simultaneously and are always active — this is not a
    # primary/failover setup. ServiceNow publishes to both clusters; Confluent
    # mirrors both. Topic prefixes make the source explicit:
    #   4100.<topic>  — mirrored from the 4100 source cluster
    #   4200.<topic>  — mirrored from the 4200 source cluster
    # Downstream consumers subscribe to both and handle deduplication.
    dual_cluster = "source_host" in cfg
    if dual_cluster:
        links = [
            {
                **cfg,
                "link_name": f"{cfg['link_name']}-{port}",
                "source_bootstrap": sn_bootstrap(cfg["source_host"], port, cfg["brokers_per_cluster"]),
                "_port": port,
            }
            for port in cfg["source_clusters"]
        ]
    else:
        links = [cfg]

    for link_cfg in links:
        ssl_props = build_ssl_properties(
            ca_pem, client_cert, client_key,
            literal_newlines=args.literal_newlines,
            key_password=args.key_password,
        )
        props = ssl_props + build_link_properties(link_cfg["_port"]) if dual_cluster else ssl_props

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".properties", delete=False, encoding="utf-8"
        )
        try:
            tmp.write(props)
            tmp.flush()
            tmp.close()
            command = build_link_command(link_cfg, tmp.name)
            run_link_create(command, dry_run=args.dry_run)
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

        if not args.dry_run:
            wait_for_link_active(link_cfg, timeout=args.timeout)
            print(f"Cluster Link '{link_cfg['link_name']}' is ACTIVE.")


if __name__ == "__main__":
    main()
