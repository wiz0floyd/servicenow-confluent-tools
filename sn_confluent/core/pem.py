"""Transitional re-exports of PEM / Confluent CLI helpers.

Phase 2 of the unified-CLI refactor wraps `shared.pem_common` (created in
PR #24) so future Phase 3 / Phase 4 code can depend on a stable
`sn_confluent.core.pem` import path. Once the legacy `shared/` directory is
retired, this module's contents will be inlined here without changing the
public API.

Public API: `SN_SOURCE_CLUSTERS`, `SN_BROKERS_PER_CLUSTER`,
`CONFLUENT_INSTALL`, `check_confluent_cli()`, `check_auth()`,
`load_pem_files()`. See `sn_confluent/core/CONTRACT.md`.
"""

from __future__ import annotations

import os
import sys

# Bootstrap: when run from the repo root (not yet pip-installed) the `shared`
# package lives at the repo root, not on sys.path. Mirror the same trick the
# existing tools use so this module works both when installed and when run in
# place.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.pem_common import (  # noqa: E402  (sys.path bootstrap above)
    SN_SOURCE_CLUSTERS,
    SN_BROKERS_PER_CLUSTER,
    CONFLUENT_INSTALL,
    check_confluent_cli,
    check_auth,
    load_pem_files,
)

__all__ = [
    "SN_SOURCE_CLUSTERS",
    "SN_BROKERS_PER_CLUSTER",
    "CONFLUENT_INSTALL",
    "check_confluent_cli",
    "check_auth",
    "load_pem_files",
]
