"""PEM / Confluent CLI helpers shared across all subcommands.

Public API: `SN_SOURCE_CLUSTERS`, `SN_BROKERS_PER_CLUSTER`,
`CONFLUENT_INSTALL`, `check_confluent_cli()`, `check_auth()`,
`load_pem_files()`. See `sn_confluent/core/CONTRACT.md`.
"""

from __future__ import annotations

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


def ensure_authenticated(cfg: dict) -> None:
    """Ensure the Confluent CLI is installed and authenticated.

    Resolution order:
    1. Cloud API key — if cloud_api_key/cloud_api_secret are present in cfg
       (or CC_CLOUD_API_KEY/CC_CLOUD_API_SECRET env vars), log in with them
       non-interactively. Best for automation; tokens never expire.
    2. Saved credentials — if a prior `confluent login --save` stored a
       DPAPI-encrypted password, the CLI re-authenticates silently on Windows
       without a browser prompt. Tried automatically when no API key is set.
    3. Manual — if neither works, the user must run `confluent login`.
    """
    if shutil.which("confluent") is None:
        print(CONFLUENT_INSTALL)
        sys.exit(1)

    cloud_key = (
        os.environ.get("CC_CLOUD_API_KEY", "").strip()
        or cfg.get("cloud_api_key", "").strip()
    )
    cloud_secret = (
        os.environ.get("CC_CLOUD_API_SECRET", "").strip()
        or cfg.get("cloud_api_secret", "").strip()
    )

    if cloud_key and cloud_secret:
        # Cloud API keys are consumed via env vars — the CLI picks them up
        # automatically and skips the SSO session requirement entirely.
        os.environ["CONFLUENT_CLOUD_API_KEY"] = cloud_key
        os.environ["CONFLUENT_CLOUD_API_SECRET"] = cloud_secret
        return

    # No API key — check if already authenticated; if not, try to refresh
    # from the DPAPI-encrypted saved credentials (Windows silent re-auth).
    probe = subprocess.run(
        ["confluent", "whoami"],
        capture_output=True,
        text=True,
    )
    if probe.returncode == 0:
        return  # already authenticated

    print("Confluent CLI session expired — attempting re-login with saved credentials...")
    relogin = subprocess.run(
        ["confluent", "login", "--save"],
        capture_output=True,
        text=True,
    )
    if relogin.returncode != 0:
        print(
            "Error: Confluent CLI is not authenticated and re-login failed.\n"
            "Run: confluent login --save\n"
            "Or add cloud_api_key / cloud_api_secret to deploy.conf for non-interactive auth.",
            file=sys.stderr,
        )
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
                f"Error: {name} not found in {pem_dir}. Run: sn-confluent extract first",
                file=sys.stderr,
            )
            sys.exit(1)
        if return_paths:
            files[name] = path
        else:
            with open(path, "rb") as fh:
                files[name] = fh.read()
    return files["ca.pem"], files["client-cert.pem"], files["client-key.pem"]


__all__ = [
    "SN_SOURCE_CLUSTERS",
    "SN_BROKERS_PER_CLUSTER",
    "CONFLUENT_INSTALL",
    "check_confluent_cli",
    "ensure_authenticated",
    "check_auth",
    "load_pem_files",
]
