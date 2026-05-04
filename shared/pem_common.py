import os
import shutil
import subprocess
import sys

SN_SOURCE_CLUSTERS = [4100, 4200]
SN_BROKERS_PER_CLUSTER = 4

CONFLUENT_INSTALL = """\
Confluent CLI not found. Install it:

  Windows : winget install Confluent.ConfluentCLI
  macOS   : brew install confluentinc/tap/confluent-cli
  Linux   : See https://docs.confluent.io/confluent-cli/current/install.html

Then authenticate: confluent login
"""


def check_confluent_cli() -> None:
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


def load_pem_files(pem_dir: str, return_paths: bool = False) -> tuple:
    """Return (ca, cert, key) as bytes (default) or file paths when return_paths=True.

    kafka-python takes file paths; Confluent SSL properties take raw bytes.
    """
    files = {"ca.pem": None, "client-cert.pem": None, "client-key.pem": None}
    for name in files:
        path = os.path.join(pem_dir, name)
        if not os.path.exists(path):
            print(
                f"Error: {name} not found in {pem_dir}. Run: python extract_pem.py first",
                file=sys.stderr,
            )
            sys.exit(1)
        if return_paths:
            files[name] = path
        else:
            with open(path, "rb") as fh:
                files[name] = fh.read()
    return files["ca.pem"], files["client-cert.pem"], files["client-key.pem"]
