# sn_confluent.core — Frozen Contract (Phase 2)

Phase 3 migration agents read this. **Do not modify `sn_confluent/core/`.**
If you need new core behavior, escalate — do not edit in place.

## Public API

### `sn_confluent.core.pem`

Re-exports from `shared/pem_common.py`. Identical signatures, identical
behavior. Phase 3 imports must use the `sn_confluent.core.pem` path.

- `SN_SOURCE_CLUSTERS: list[int]` — `[4100, 4200]`
- `SN_BROKERS_PER_CLUSTER: int` — `4`
- `CONFLUENT_INSTALL: str` — install instructions message
- `check_confluent_cli() -> None` — exits 1 if `confluent` not on PATH
- `check_auth(environment_id: str, cluster_id: str) -> None` — exits 1 if
  not logged in or cluster not accessible
- `load_pem_files(pem_dir: str, return_paths: bool = False) -> tuple` —
  returns `(ca, cert, key)` as bytes (default) or paths

### `sn_confluent.core.config`

- `load_config(path: str, required_keys: Iterable[str]) -> dict` — loads
  INI file, validates `[confluent]` section + required keys, exits 1 with
  stderr message on any failure. Returns raw `dict[str, str]`.
- `expand_link_config(cfg: dict) -> dict` — coerces `source_clusters`
  (list[int]) and `brokers_per_cluster` (int), with ServiceNow defaults.
- `add_per_cluster_link_names(cfg: dict) -> dict` — mirror-only helper;
  adds `link_name_{port}` keys.

### Required keys per subcommand

| Subcommand | Required keys |
|---|---|
| `link` | `environment_id`, `cluster_id`, `link_name` (+ either `source_bootstrap` or `source_host` — validate in tool, not core) |
| `mirror` | `environment_id`, `cluster_id`, `link_name`, `source_host`, `instance_name` |
| `replicate` | `environment_id`, `cluster_id`, `connector_name`, `source_host` (`pem_dir` resolved from `--pem-dir` or config in main, not validated by core) |

## Phase 3 package layout

Each tool migrates to:

```
sn_confluent/<subcmd>/__init__.py     # empty
sn_confluent/<subcmd>/main.py         # exports `main(argv=None) -> int`
```

Where `<subcmd>` is one of: `extract`, `link`, `mirror`, `replicate`.

`main(argv=None)` parses its own argv slice (uses `sys.argv[1:]` if argv is
None) so it can be invoked both directly (legacy CLI) and via the unified
dispatcher in `sn_confluent.cli`. Tests calling `from <subcmd>.main import
main` plus a patched `sys.argv` continue to work.

## Test layout

Tests stay co-located with each subcommand:
`sn_confluent/<subcmd>/tests/test_*.py`. A repo-root `pytest` run picks
them all up. Phase 3 owns the import-path rewrites for its own subcommand.
