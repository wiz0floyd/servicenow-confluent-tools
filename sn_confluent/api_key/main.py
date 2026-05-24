# Manage Confluent Cloud Kafka API keys.
#
# Usage: sn-confluent api-key create --environment ENV_ID --cluster CLUSTER_ID
#        sn-confluent api-key list --cluster CLUSTER_ID [--environment ENV_ID]
#        sn-confluent api-key delete KEY_ID

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import List, Optional, Tuple

from sn_confluent.core.pem import check_confluent_cli


def create_kafka_api_key(
    environment_id: str,
    cluster_id: str,
    description: str = "",
    service_account: str = "",
) -> Tuple[str, str]:
    """Create a Kafka API key scoped to cluster_id. Returns (key, secret).

    The secret is returned only at creation time and cannot be retrieved again.
    Callers are responsible for persisting it.
    """
    cmd = [
        "confluent", "api-key", "create",
        "--resource", cluster_id,
        "--environment", environment_id,
        "--output", "json",
    ]
    if description:
        cmd += ["--description", description]
    if service_account:
        cmd += ["--service-account", service_account]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(
            f"Error: confluent api-key create failed: {result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        data = json.loads(result.stdout)
        # Confluent CLI uses "key"/"secret" in JSON output; guard against both field names.
        api_key = (data.get("key") or data.get("api_key") or "").strip()
        api_secret = (data.get("secret") or data.get("api_secret") or "").strip()
        if not api_key or not api_secret:
            raise KeyError("key/secret fields missing")
    except (json.JSONDecodeError, KeyError) as exc:
        print(
            f"Error: Unexpected api-key create output ({exc}): {result.stdout[:400]}",
            file=sys.stderr,
        )
        sys.exit(1)

    return api_key, api_secret


def list_kafka_api_keys(
    cluster_id: str,
    environment_id: str = "",
) -> Optional[List[dict]]:
    """Return API keys for cluster_id, or None on CLI failure."""
    cmd = [
        "confluent", "api-key", "list",
        "--resource", cluster_id,
        "--output", "json",
    ]
    if environment_id:
        cmd += ["--environment", environment_id]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(
            f"Error: confluent api-key list failed: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return None

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def delete_kafka_api_key(key_id: str) -> bool:
    """Delete api_key key_id. Returns True on success."""
    result = subprocess.run(
        ["confluent", "api-key", "delete", key_id, "--force"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            f"Error: confluent api-key delete failed: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return False
    return True


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Manage Confluent Cloud Kafka API keys."
    )
    sub = parser.add_subparsers(dest="action", metavar="ACTION")
    sub.required = True

    create_p = sub.add_parser("create", help="Create a new Kafka API key")
    create_p.add_argument("--environment", required=True, metavar="ENV_ID",
                          help="Confluent Cloud environment ID")
    create_p.add_argument("--cluster", required=True, metavar="CLUSTER_ID",
                          help="Kafka cluster ID")
    create_p.add_argument("--description", default="", metavar="TEXT",
                          help="Human-readable label for the key")
    create_p.add_argument("--service-account", default="", metavar="SA_ID",
                          help="Service account to own the key (default: current user)")

    list_p = sub.add_parser("list", help="List Kafka API keys for a cluster")
    list_p.add_argument("--environment", default="", metavar="ENV_ID",
                        help="Confluent Cloud environment ID")
    list_p.add_argument("--cluster", required=True, metavar="CLUSTER_ID",
                        help="Kafka cluster ID")

    delete_p = sub.add_parser("delete", help="Delete a Kafka API key")
    delete_p.add_argument("key_id", metavar="KEY_ID", help="API key to delete")

    args = parser.parse_args(argv)
    check_confluent_cli()

    if args.action == "create":
        api_key, api_secret = create_kafka_api_key(
            environment_id=args.environment,
            cluster_id=args.cluster,
            description=args.description,
            service_account=args.service_account,
        )
        print(f"API Key:    {api_key}")
        print(f"API Secret: {api_secret}")
        print("\nSave the secret now — it cannot be retrieved again.")
        print(
            f"\nTo use with sn-confluent deploy, add to deploy.conf:\n"
            f"  kafka_api_key    = {api_key}\n"
            f"  kafka_api_secret = {api_secret}"
        )
        return 0

    if args.action == "list":
        keys = list_kafka_api_keys(args.cluster, args.environment)
        if keys is None:
            return 1
        if not keys:
            print(f"No API keys found for cluster {args.cluster}.")
            return 0
        print(f"{'Key':<20}  {'Owner':<30}  Description")
        print("-" * 72)
        for k in keys:
            key_id = (k.get("key") or k.get("api_key") or k.get("id") or "").strip()
            owner = (k.get("owner_resource_id") or k.get("owner") or "").strip()
            desc = k.get("description", "")
            print(f"{key_id:<20}  {owner:<30}  {desc}")
        return 0

    if args.action == "delete":
        if not delete_kafka_api_key(args.key_id):
            return 1
        print(f"Deleted API key: {args.key_id}")
        return 0

    return 0  # unreachable but satisfies type checkers


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
