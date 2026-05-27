"""Unified config loader for `link.conf`-style files.

Each subcommand has a different set of required keys (see CONTRACT.md):
    - link:   environment_id, cluster_id, link_name
    - mirror: environment_id, cluster_id, link_name, source_host, instance_name

The loader is intentionally minimal: read the file, validate required keys,
and return the section as a dict. Subcommand-specific post-processing (such
as expanding `link_name_{port}` for mirror-topics) lives in dedicated
helpers below so the loader stays focused.

Named profiles
--------------
Pass `profile="dev"` to read a named section instead of `[confluent]`.
Shared defaults belong in a `[DEFAULT]` section; configparser automatically
merges DEFAULT into every named section, so only the keys that differ need to
appear in the profile section:

    [DEFAULT]
    environment_id = env-prod123
    cluster_id     = lkc-prod456
    link_name      = sn-prod-link
    source_host    = hermes1.service-now.com

    [dev]
    environment_id = env-dev789
    cluster_id     = lkc-dev012

Single-section files (`[confluent]` only, no profiles) are unaffected.
"""

from __future__ import annotations

import configparser
import os
import sys
from typing import Iterable, List, Optional

from .pem import SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER

CONFIG_SECTION = "confluent"


def load_config(path: str, required_keys: Iterable[str], profile: Optional[str] = None) -> dict:
    """Load an INI-style config file and validate required keys.

    Exits with status 1 and a clear stderr message on any failure:
        - file does not exist
        - missing `[confluent]` section (when profile is None)
        - named profile section not found in file (when profile is set)
        - any of `required_keys` absent from the resolved section

    When `profile` is None (default), reads the `[confluent]` section —
    identical to the pre-profile behaviour; all existing callers are unaffected.

    When `profile` is a non-empty string, reads that named section instead.
    Keys absent from the profile section are inherited from `[DEFAULT]` via
    configparser's standard DEFAULT-merging behaviour.

    Returns the section as a plain `dict[str, str]`. Subcommands are
    responsible for any further coercion (ints, lists, derived keys);
    use `expand_link_config()` for the common ServiceNow expansions.
    """
    if not os.path.exists(path):
        print(f"Error: Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(path)

    if profile is None:
        if CONFIG_SECTION not in cfg:
            print(
                f"Error: {path} is missing the [{CONFIG_SECTION}] section.",
                file=sys.stderr,
            )
            sys.exit(1)
        section = dict(cfg[CONFIG_SECTION])
    else:
        # cfg.sections() excludes DEFAULT, so 'DEFAULT' is always rejected here.
        # configparser treats DEFAULT as a reserved pseudo-section, not a profile.
        if profile not in cfg.sections():
            available = cfg.sections()
            if available:
                available_str = ", ".join(available)
            else:
                available_str = "(none — add a named section such as [dev] to your config file)"
            ci_match = next((s for s in available if s.lower() == profile.lower()), None)
            hint = f" Did you mean '{ci_match}'?" if ci_match else ""
            print(
                f"Error: Profile '{profile}' not found in {path}.{hint} "
                f"Available profiles: {available_str}",
                file=sys.stderr,
            )
            sys.exit(1)
        section = dict(cfg[profile])

    for key in required_keys:
        if key not in section:
            print(f"Error: Missing required key '{key}' in {path}", file=sys.stderr)
            sys.exit(1)
    return section


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
    if clusters_raw:
        try:
            cfg["source_clusters"] = [int(x.strip()) for x in clusters_raw.split(",") if x.strip()]
        except ValueError:
            print(
                f"Error: source_clusters in link.conf must be a comma-separated list of integers: {clusters_raw!r}",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        cfg["source_clusters"] = list(SN_SOURCE_CLUSTERS)
    bpc_raw = cfg.get("brokers_per_cluster", "")
    if bpc_raw:
        try:
            cfg["brokers_per_cluster"] = int(bpc_raw)
        except ValueError:
            print(
                f"Error: brokers_per_cluster in link.conf must be an integer: {bpc_raw!r}",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        cfg["brokers_per_cluster"] = SN_BROKERS_PER_CLUSTER
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
