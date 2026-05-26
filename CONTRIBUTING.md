# Contributing

## Repo layout

```
sn-confluent/
├── pyproject.toml
├── sn_confluent/
│   ├── cli.py                # unified entry-point dispatcher
│   ├── core/                 # shared API (frozen — see core/CONTRACT.md)
│   │   ├── pem.py            #   PEM / Confluent CLI helpers
│   │   └── config.py         #   link.conf loader + post-processing
│   ├── extract/              # subcommand: PEM extraction
│   ├── link/                 # subcommand: cluster link creation
│   ├── mirror/               # subcommand: topic mirroring
│   ├── deploy/               # subcommand: Hermes custom connector (sink/source)
│   ├── api_key/              # subcommand: Confluent Cloud API key management
│   └── setup/                # subcommand: end-to-end wizard
└── README.md
```

Each subcommand is a Python package under `sn_confluent/` with its own `main.py`, `README.md`, tests, and (if applicable) a `link.conf.example`. The unified CLI dispatcher in `sn_confluent/cli.py` lazy-imports each subcommand's `main(argv)` and forwards the remaining argv slice.

## Adding a new subcommand

1. Create `sn_confluent/<name>/` with:
   ```
   __init__.py
   main.py            # exports `main(argv: Optional[List[str]] = None) -> int`
   README.md
   tests/
       __init__.py
       test_<name>.py
   ```
2. Wire dispatch in `sn_confluent/cli.py` (`SUBCOMMAND_HELP` + `_load_subcommand`).
3. If the subcommand needs orchestration in `sn-confluent setup`, add it to `sn_confluent/setup/wizard.py`.
4. Update the table in the root `README.md`.

## Using `sn_confluent.core`

`sn_confluent/core/` is the **frozen** shared API used by every subcommand. See `sn_confluent/core/CONTRACT.md` for the full contract.

```python
from sn_confluent.core.pem import (
    SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER, CONFLUENT_INSTALL,
    check_confluent_cli, check_auth, load_pem_files,
)
from sn_confluent.core.config import load_config, expand_link_config, add_per_cluster_link_names
```

If you need new behaviour from `core/`, propose the API change in a separate PR rather than amending `core/` mid-feature. The contract is what other subcommand maintainers rely on.

## Development setup

```bash
pip install -e .[dev]
pytest sn_confluent/
```

`pytest` from the repo root picks up every subcommand's test suite.

## Pull requests

- One subcommand or fix per PR.
- All tests must pass before requesting review.
- Update the affected subcommand's `README.md` if flags or behaviour change.

## Secrets and credentials

- Never commit PEM files, keystores, passwords, API keys, or live cluster IDs.
- Use `link.conf.example` as the pattern for config templates — real configs are gitignored.
- New subcommands with config files must provide an `.example` file and gitignore the live one.
