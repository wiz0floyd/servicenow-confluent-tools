# Contributing

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

- Keep tools self-contained — no cross-tool imports
- Include a `README.md` with setup, usage, and all CLI flags documented
- Tests go in `tests/` and must pass with `pytest` run from the tool directory

## Development setup

```bash
cd <tool-directory>
pip install -r requirements-dev.txt
pytest
```

## Pull requests

- One tool or fix per PR
- All tests must pass before requesting review
- Update the tool's `README.md` if flags or behavior change
- Add the tool to the table in the root `README.md` if it's new

## Secrets and credentials

- Never commit PEM files, keystores, passwords, API keys, or live cluster IDs
- Use `link.conf.example` as the pattern for config templates — real configs are gitignored
- If you add a new tool with config, gitignore the live config and provide an `.example` file
