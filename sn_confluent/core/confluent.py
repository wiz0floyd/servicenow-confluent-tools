"""Confluent CLI subprocess helpers shared across subcommands."""

from __future__ import annotations

import json
import subprocess
import sys
from typing import List, Optional


def list_cc_topics(environment_id: str, cluster_id: str) -> Optional[List[str]]:
    """Return sorted CC topic names, or None on failure.

    Callers that need hard-fail behaviour should check for None and sys.exit(1).
    """
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
        print(
            f"Warning: Could not list CC topics: {result.stderr.strip()}",
            file=sys.stderr,
        )
        return None
    try:
        topics = json.loads(result.stdout)
        return sorted(t["name"] for t in topics)
    except (json.JSONDecodeError, KeyError):
        return None


__all__ = ["list_cc_topics"]
