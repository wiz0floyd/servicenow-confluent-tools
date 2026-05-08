"""Unified config loader for `link.conf`-style files.

Each subcommand has a different set of required keys (see CONTRACT.md):
    - link:      environment_id, cluster_id, link_name
    - mirror:    environment_id, cluster_id, link_name, source_host, instance_name
    - replicate: environment_id, cluster_id, connector_name, source_host, pem_dir

The loader is intentionally minimal: read the file, validate required keys,
and return the section as a dict. Subcommand-specific post-processing (such
as expanding `link_name_{port}` for mirror-topics) lives in dedicated
helpers below so the loader stays focused.
"""

from __future__ import annotations

import configparser
import os
import sys
from typing import Iterable, List

from .pem import SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER

CONFIG_SECTION = "confluent"


def load_config(path: str, required_keys: Iterable[str]) -> dict:
    """Load an INI-style config file and validate required keys.

    Exits with status 1 and a clear stderr message on any failure:
        - file does not exist
        - missing `[confluent]` section
        - any of `required_keys` absent from that section

    Returns the section as a plain `dict[str, str]`. Subcommands are
    responsible for any further coercion (ints, lists, derived keys);
    use `expand_link_config()` for the common ServiceNow expansions.
    """
    if not os.path.exists(path):
        print(f"Error: Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(path)
    if CONFIG_SECTION not in cfg:
        print(
            f"Error: link.conf is missing the [{CONFIG_SECTION}] section.",
            file=sys.stderr,
        )
        sys.exit(1)
    section = cfg[CONFIG_SECTION]
    for key in required_keys:
        if key not in section:
            print(f"Error: Missing key in link.conf: {key}", file=sys.stderr)
            sys.exit(1)
    return dict(section)


def expand_link_config(cfg: dict) -> dict:
    """Apply the ServiceNow-flavoured post-processing common to link/mirror.

    Mutates and returns `cfg`:
        - `source_clusters`: comma list -> `list[int]`, defaults to
          `SN_SOURCE_CLUSTERS`.
        - `brokers_per_cluster`: str -> int, defaults to
          `SN_BROKERS_PER_CLUSTER`.

    Does NOT add `link_name_{port}` keys — those are mirror-topics-specific;
    callers that need them should follow up with
    `add_per_cluster_link_names()`.
    """
    clusters_raw = cfg.get("source_clusters", "")
    cfg["source_clusters"] = (
        [int(x.strip()) for x in clusters_raw.split(",") if x.strip()]
        if clusters_raw
        else list(SN_SOURCE_CLUSTERS)
    )
    bpc_raw = cfg.get("brokers_per_cluster", "")
    cfg["brokers_per_cluster"] = int(bpc_raw) if bpc_raw else SN_BROKERS_PER_CLUSTER
    return cfg


def add_per_cluster_link_names(cfg: dict) -> dict:
    """Add `link_name_{port}` keys derived from `link_name` and source_clusters.

    mirror-topics-specific helper. Requires that `expand_link_config()` has
    already been applied (so `source_clusters` is a list of ints) and that
    `link_name` is present.
    """
    link_name = cfg["link_name"]
    clusters: List[int] = cfg["source_clusters"]
    for port in clusters:
        cfg[f"link_name_{port}"] = f"{link_name}-{port}"
    return cfg
