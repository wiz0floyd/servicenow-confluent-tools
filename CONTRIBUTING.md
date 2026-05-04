# Contributing

## Repo structure

```
servicenow-confluent-tools/
├── shared/               # Cross-cutting constants and utilities (stdlib only)
│   └── pem_common.py
├── extract-pem/          # Tool: PEM extraction from Java keystores
├── cluster-link/         # Tool: Confluent Cloud cluster link creation
├── mirror-topics/        # Tool: topic mirroring with interactive UI
└── connect-replicator/   # Tool: Confluent Replicator deployment
```

Each tool directory is independently runnable. `shared/` contains code used by two or more tools — it has no dependencies beyond the Python standard library.

## Adding a new tool

Each tool lives in its own top-level directory:

```
your-tool-name/
├── README.md
├── your_tool.py
├── requirements.txt
├── requirements-dev.txt
└── tests/
    └── test_your_tool.py
```

- Include a `README.md` with setup, usage, and all CLI flags documented
- Tests go in `tests/` and must pass with `pytest` run from the tool directory
- Add the tool to the table in the root `README.md`

## Using and extending `shared/`

Tools import from `shared/` using a `sys.path` insert at the top of the script:

```python
import os, sys
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
from shared.pem_common import check_confluent_cli, load_pem_files, ...
```

**What belongs in `shared/`:**
- Constants used by 2+ tools (e.g. `SN_SOURCE_CLUSTERS`, `CONFLUENT_INSTALL`)
- Utility functions called by 2+ tools with identical or near-identical behavior
- stdlib-only code — no third-party dependencies

**What stays tool-local:**
- Business logic specific to one tool
- Functions that differ meaningfully between tools
- Anything requiring a third-party library

When adding to `shared/`, update every tool that should use it and add a test in the relevant tool's test suite (shared code is tested through its callers, not standalone).

## Development setup

```bash
cd <tool-directory>
pip install -r requirements-dev.txt
pytest
```

No separate install is needed for `shared/` — it uses only the standard library.

## Pull requests

- One tool or fix per PR
- All tests must pass before requesting review
- Update the tool's `README.md` if flags or behavior change

## Secrets and credentials

- Never commit PEM files, keystores, passwords, API keys, or live cluster IDs
- Use `link.conf.example` as the pattern for config templates — real configs are gitignored
- If you add a new tool with config, gitignore the live config and provide an `.example` file
