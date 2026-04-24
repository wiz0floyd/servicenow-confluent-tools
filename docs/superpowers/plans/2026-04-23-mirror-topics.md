# Mirror Topics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add DC-prefixed topic mirroring across both active-active cluster links via a checkbox TUI or --all auto-mirror flag.

**Architecture:** `create_link.py` gets `cluster.link.prefix` added to link creation properties. New `mirror-topics/mirror_topics.py` lists source topics via kafka-python AdminClient (mTLS), shows a questionary checkbox UI, then creates mirror topics on both links using the Confluent CLI. `--all` instead sets `auto.create.mirror.topics.enable=true` on both links.

**Tech Stack:** Python 3, kafka-python, questionary, Confluent CLI, existing PEM files for mTLS.

---

**Project Rule Overrides:** No project CLAUDE.md found. Using skill defaults.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `cluster-link/create_link.py` | Modify | Add `cluster.link.prefix` to per-link properties at creation |
| `cluster-link/tests/test_create_link.py` | Modify | Tests for prefix in generated properties |
| `mirror-topics/mirror_topics.py` | Create | Main script: topic listing, UI, mirror creation, --all mode |
| `mirror-topics/requirements.txt` | Create | kafka-python, questionary |
| `mirror-topics/tests/test_mirror_topics.py` | Create | Unit tests for all functions |

---

### Task 1: Add cluster.link.prefix to create_link.py

**Files:**
- Modify: `cluster-link/create_link.py`
- Modify: `cluster-link/tests/test_create_link.py`

- [ ] **Step 1: Write failing tests**

Add to `cluster-link/tests/test_create_link.py`:

```python
def test_dual_link_properties_include_4100_prefix(tmp_path):
    from create_link import build_link_properties
    props = build_link_properties(4100)
    assert "cluster.link.prefix=4100." in props

def test_dual_link_properties_include_4200_prefix(tmp_path):
    from create_link import build_link_properties
    props = build_link_properties(4200)
    assert "cluster.link.prefix=4200." in props
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd cluster-link && pytest tests/test_create_link.py::test_dual_link_properties_include_4100_prefix tests/test_create_link.py::test_dual_link_properties_include_4200_prefix -v
```

Expected: FAIL — `build_link_properties` not defined.

- [ ] **Step 3: Extract build_link_properties and add prefix**

In `cluster-link/create_link.py`, extract the per-link properties builder. Currently `main()` calls `build_ssl_properties()` once and reuses it for both links. Change the dual-link branch so each link gets its own properties block with `cluster.link.prefix` appended.

Replace the block in `main()` starting at `props = build_ssl_properties(...)` with:

```python
    for link_cfg in links:
        port = 4100 if link_cfg["link_name"].endswith("-4100") else 4200
        props = build_ssl_properties(
            ca_pem, client_cert, client_key,
            literal_newlines=args.literal_newlines,
            key_password=args.key_password,
        ) + f"cluster.link.prefix={port}.\n"

        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".properties", delete=False, encoding="utf-8"
        )
        try:
            tmp.write(props)
            tmp.flush()
            tmp.close()
            command = build_link_command(link_cfg, tmp.name)
            run_link_create(command, dry_run=args.dry_run)
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

        if not args.dry_run:
            wait_for_link_active(link_cfg, timeout=args.timeout)
            print(f"Cluster Link '{link_cfg['link_name']}' is ACTIVE.")
```

Also add the standalone helper (used by tests):

```python
def build_link_properties(port: int) -> str:
    """Return the cluster.link.prefix line for the given source port."""
    return f"cluster.link.prefix={port}.\n"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd cluster-link && pytest tests/test_create_link.py -v
```

Expected: all pass including the two new tests.

- [ ] **Step 5: Commit**

```bash
git add cluster-link/create_link.py cluster-link/tests/test_create_link.py
git commit -m "feat: add cluster.link.prefix to dual-link creation"
```

---

### Task 2: Scaffold mirror-topics directory and requirements

**Files:**
- Create: `mirror-topics/requirements.txt`
- Create: `mirror-topics/tests/__init__.py` (empty)

- [ ] **Step 1: Create requirements.txt**

```
kafka-python>=2.0.0
questionary>=2.0.0
```

- [ ] **Step 2: Create directory structure**

```bash
mkdir -p mirror-topics/tests
touch mirror-topics/tests/__init__.py
```

- [ ] **Step 3: Commit**

```bash
git add mirror-topics/
git commit -m "chore: scaffold mirror-topics directory"
```

---

### Task 3: Config loading and source topic listing

**Files:**
- Create: `mirror-topics/mirror_topics.py` (initial)
- Create: `mirror-topics/tests/test_mirror_topics.py` (initial)

- [ ] **Step 1: Write failing tests**

Create `mirror-topics/tests/test_mirror_topics.py`:

```python
import sys
import textwrap
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, ".")

VALID_CONFIG = textwrap.dedent("""\
    [confluent]
    environment_id = env-abc123
    cluster_id     = lkc-abc123
    link_name      = servicenow-link
    source_host    = hermes1.service-now.com
""")


def write_config(tmp_path, content=VALID_CONFIG):
    p = tmp_path / "link.conf"
    p.write_text(content)
    return str(p)


# --- load_config ---

def test_load_config_returns_dict_with_link_names(tmp_path):
    from mirror_topics import load_config
    cfg = load_config(write_config(tmp_path))
    assert cfg["environment_id"] == "env-abc123"
    assert cfg["link_name_4100"] == "servicenow-link-4100"
    assert cfg["link_name_4200"] == "servicenow-link-4200"


def test_load_config_exits_if_missing():
    from mirror_topics import load_config
    with pytest.raises(SystemExit) as exc:
        load_config("/nonexistent/link.conf")
    assert exc.value.code == 1


def test_load_config_exits_if_no_source_host(tmp_path):
    from mirror_topics import load_config
    bad = textwrap.dedent("""\
        [confluent]
        environment_id = env-abc123
        cluster_id     = lkc-abc123
        link_name      = servicenow-link
        source_bootstrap = broker:9093
    """)
    with pytest.raises(SystemExit) as exc:
        load_config(write_config(tmp_path, bad))
    assert exc.value.code == 1


# --- list_source_topics ---

def test_list_source_topics_returns_sorted_list():
    from mirror_topics import list_source_topics
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value = MagicMock(
        topics={"zebra": None, "alpha": None, "__consumer_offsets": None}
    )
    with patch("mirror_topics.AdminClient", return_value=mock_admin):
        topics = list_source_topics("hermes1.service-now.com", 4100, "ca", "cert", "key")
    assert topics == ["alpha", "zebra"]
    assert "__consumer_offsets" not in topics


def test_list_source_topics_applies_filter():
    from mirror_topics import list_source_topics
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value = MagicMock(
        topics={"sn_foo": None, "sn_bar": None, "other": None}
    )
    with patch("mirror_topics.AdminClient", return_value=mock_admin):
        topics = list_source_topics("hermes1.service-now.com", 4100, "ca", "cert", "key", filter_prefix="sn_")
    assert topics == ["sn_bar", "sn_foo"]
    assert "other" not in topics


def test_list_source_topics_exits_on_connection_error(capsys):
    from mirror_topics import list_source_topics
    mock_admin = MagicMock()
    mock_admin.list_topics.side_effect = Exception("Connection refused")
    with patch("mirror_topics.AdminClient", return_value=mock_admin):
        with pytest.raises(SystemExit) as exc:
            list_source_topics("hermes1.service-now.com", 4100, "ca", "cert", "key")
    assert exc.value.code == 1
    assert "Connection refused" in capsys.readouterr().err
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd mirror-topics && pip install -r requirements.txt
pytest tests/test_mirror_topics.py::test_load_config_returns_dict_with_link_names tests/test_mirror_topics.py::test_list_source_topics_returns_sorted_list -v
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement config loading and topic listing**

Create `mirror-topics/mirror_topics.py`:

```python
# Mirror topics across both ServiceNow cluster links with DC prefix.
#
# Setup: pip install -r requirements.txt
# Usage: python mirror_topics.py [--config PATH] [--pem-dir PATH] [--filter PREFIX] [--all] [--dry-run]

import argparse
import configparser
import os
import subprocess
import sys

from kafka import KafkaAdminClient
from kafka.admin import NewTopic  # noqa: F401 — imported for type context


INTERNAL_TOPIC_PREFIXES = ("__", "_confluent")
SN_BROKERS_PER_CLUSTER = 4


def load_config(path: str) -> dict:
    """Load link.conf; derive link names; exit 1 on any problem."""
    if not os.path.exists(path):
        print(f"Error: Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    cfg = configparser.ConfigParser()
    cfg.read(path)
    if "confluent" not in cfg:
        print("Error: link.conf is missing the [confluent] section.", file=sys.stderr)
        sys.exit(1)
    section = cfg["confluent"]
    for key in ("environment_id", "cluster_id", "link_name", "source_host"):
        if key not in section:
            print(f"Error: Missing key in link.conf: {key}", file=sys.stderr)
            sys.exit(1)
    result = dict(section)
    result["link_name_4100"] = f"{result['link_name']}-4100"
    result["link_name_4200"] = f"{result['link_name']}-4200"
    return result


def list_source_topics(
    host: str,
    base_port: int,
    ca_path: str,
    cert_path: str,
    key_path: str,
    filter_prefix: str = None,
) -> list[str]:
    """Connect to source brokers via mTLS and return sorted non-internal topic names."""
    bootstrap = ",".join(f"{host}:{base_port + i}" for i in range(SN_BROKERS_PER_CLUSTER))
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            security_protocol="SSL",
            ssl_cafile=ca_path,
            ssl_certfile=cert_path,
            ssl_keyfile=key_path,
        )
        metadata = admin.list_topics()
        admin.close()
    except Exception as exc:
        print(f"Error: Failed to connect to source brokers: {exc}", file=sys.stderr)
        sys.exit(1)

    topics = [
        t for t in metadata.topics
        if not any(t.startswith(p) for p in INTERNAL_TOPIC_PREFIXES)
    ]
    if filter_prefix:
        topics = [t for t in topics if t.startswith(filter_prefix)]
    return sorted(topics)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd mirror-topics && pytest tests/test_mirror_topics.py::test_load_config_returns_dict_with_link_names tests/test_mirror_topics.py::test_load_config_exits_if_missing tests/test_mirror_topics.py::test_load_config_exits_if_no_source_host tests/test_mirror_topics.py::test_list_source_topics_returns_sorted_list tests/test_mirror_topics.py::test_list_source_topics_applies_filter tests/test_mirror_topics.py::test_list_source_topics_exits_on_connection_error -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add mirror-topics/mirror_topics.py mirror-topics/tests/test_mirror_topics.py
git commit -m "feat: mirror-topics config loading and source topic listing"
```

---

### Task 4: Already-mirrored detection

**Files:**
- Modify: `mirror-topics/mirror_topics.py`
- Modify: `mirror-topics/tests/test_mirror_topics.py`

- [ ] **Step 1: Write failing tests**

Append to `mirror-topics/tests/test_mirror_topics.py`:

```python
# --- get_mirrored_source_topics ---

def test_get_mirrored_source_topics_strips_prefix():
    from mirror_topics import get_mirrored_source_topics
    import json
    link_cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    mirrors_4100 = json.dumps([{"mirror_topic": "4100.foo"}, {"mirror_topic": "4100.bar"}])
    mirrors_4200 = json.dumps([{"mirror_topic": "4200.foo"}, {"mirror_topic": "4200.baz"}])

    def fake_run(cmd, **kwargs):
        if "servicenow-link-4100" in cmd:
            return MagicMock(returncode=0, stdout=mirrors_4100)
        return MagicMock(returncode=0, stdout=mirrors_4200)

    with patch("subprocess.run", side_effect=fake_run):
        result = get_mirrored_source_topics(link_cfg)

    assert result == {"foo", "bar", "baz"}


def test_get_mirrored_source_topics_returns_empty_on_cli_failure():
    from mirror_topics import get_mirrored_source_topics
    link_cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("subprocess.run", return_value=MagicMock(returncode=1, stdout="")):
        result = get_mirrored_source_topics(link_cfg)
    assert result == set()
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd mirror-topics && pytest tests/test_mirror_topics.py::test_get_mirrored_source_topics_strips_prefix tests/test_mirror_topics.py::test_get_mirrored_source_topics_returns_empty_on_cli_failure -v
```

Expected: FAIL — `get_mirrored_source_topics` not defined.

- [ ] **Step 3: Implement**

Add to `mirror-topics/mirror_topics.py`:

```python
import json


def get_mirrored_source_topics(cfg: dict) -> set[str]:
    """Return set of source topic names already mirrored on either link.

    Queries both links, strips the link prefix from mirror topic names,
    and unions the results. Returns empty set if CLI call fails.
    """
    source_names = set()
    for link_key, prefix in [("link_name_4100", "4100."), ("link_name_4200", "4200.")]:
        result = subprocess.run(
            [
                "confluent", "kafka", "mirror", "list",
                "--link", cfg[link_key],
                "--environment", cfg["environment_id"],
                "--cluster", cfg["cluster_id"],
                "--output", "json",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        try:
            mirrors = json.loads(result.stdout)
            for m in mirrors:
                name = m.get("mirror_topic", "")
                if name.startswith(prefix):
                    source_names.add(name[len(prefix):])
        except (json.JSONDecodeError, AttributeError):
            continue
    return source_names
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd mirror-topics && pytest tests/test_mirror_topics.py::test_get_mirrored_source_topics_strips_prefix tests/test_mirror_topics.py::test_get_mirrored_source_topics_returns_empty_on_cli_failure -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add mirror-topics/mirror_topics.py mirror-topics/tests/test_mirror_topics.py
git commit -m "feat: detect already-mirrored topics from both links"
```

---

### Task 5: --all mode (auto-mirror)

**Files:**
- Modify: `mirror-topics/mirror_topics.py`
- Modify: `mirror-topics/tests/test_mirror_topics.py`

- [ ] **Step 1: Write failing tests**

Append to `mirror-topics/tests/test_mirror_topics.py`:

```python
# --- enable_auto_mirror ---

def test_enable_auto_mirror_dry_run_prints_commands(capsys):
    from mirror_topics import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("subprocess.run") as mock_run:
        enable_auto_mirror(cfg, dry_run=True)
        mock_run.assert_not_called()
    out = capsys.readouterr().out
    assert "servicenow-link-4100" in out
    assert "servicenow-link-4200" in out
    assert "auto.create.mirror.topics.enable=true" in out


def test_enable_auto_mirror_calls_update_on_both_links():
    from mirror_topics import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    mock_result = MagicMock(returncode=0, stdout="Updated.")
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        enable_auto_mirror(cfg, dry_run=False)
    assert mock_run.call_count == 2
    calls = [str(c) for c in mock_run.call_args_list]
    assert any("servicenow-link-4100" in c for c in calls)
    assert any("servicenow-link-4200" in c for c in calls)


def test_enable_auto_mirror_exits_on_cli_failure(capsys):
    from mirror_topics import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("subprocess.run", return_value=MagicMock(returncode=1, stderr="fail")):
        with pytest.raises(SystemExit) as exc:
            enable_auto_mirror(cfg, dry_run=False)
    assert exc.value.code == 1
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd mirror-topics && pytest tests/test_mirror_topics.py::test_enable_auto_mirror_dry_run_prints_commands tests/test_mirror_topics.py::test_enable_auto_mirror_calls_update_on_both_links tests/test_mirror_topics.py::test_enable_auto_mirror_exits_on_cli_failure -v
```

Expected: FAIL — `enable_auto_mirror` not defined.

- [ ] **Step 3: Implement**

Add to `mirror-topics/mirror_topics.py`:

```python
def enable_auto_mirror(cfg: dict, dry_run: bool) -> None:
    """Set auto.create.mirror.topics.enable=true on both links."""
    for link_key in ("link_name_4100", "link_name_4200"):
        link = cfg[link_key]
        cmd = [
            "confluent", "kafka", "link", "configuration", "update", link,
            "--environment", cfg["environment_id"],
            "--cluster", cfg["cluster_id"],
            "--config", "auto.create.mirror.topics.enable=true",
        ]
        if dry_run:
            print("Dry run — would execute:")
            print("  " + " ".join(cmd))
            continue
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            sys.exit(1)
        print(f"Auto-mirror enabled on {link}.")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd mirror-topics && pytest tests/test_mirror_topics.py::test_enable_auto_mirror_dry_run_prints_commands tests/test_mirror_topics.py::test_enable_auto_mirror_calls_update_on_both_links tests/test_mirror_topics.py::test_enable_auto_mirror_exits_on_cli_failure -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add mirror-topics/mirror_topics.py mirror-topics/tests/test_mirror_topics.py
git commit -m "feat: --all mode enables auto-mirror on both links"
```

---

### Task 6: Per-topic mirror creation

**Files:**
- Modify: `mirror-topics/mirror_topics.py`
- Modify: `mirror-topics/tests/test_mirror_topics.py`

- [ ] **Step 1: Write failing tests**

Append to `mirror-topics/tests/test_mirror_topics.py`:

```python
# --- create_mirror_topics ---

def test_create_mirror_topics_builds_correct_commands():
    from mirror_topics import create_mirror_topics
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    mock_result = MagicMock(returncode=0, stdout="Created.")
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        results = create_mirror_topics(cfg, ["foo", "bar"], dry_run=False)
    # 2 topics × 2 links = 4 calls
    assert mock_run.call_count == 4
    calls_flat = " ".join(str(c) for c in mock_run.call_args_list)
    assert "4100.foo" in calls_flat
    assert "4200.foo" in calls_flat
    assert "--source-topic" in calls_flat


def test_create_mirror_topics_dry_run_no_subprocess(capsys):
    from mirror_topics import create_mirror_topics
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("subprocess.run") as mock_run:
        create_mirror_topics(cfg, ["foo"], dry_run=True)
        mock_run.assert_not_called()
    out = capsys.readouterr().out
    assert "4100.foo" in out
    assert "4200.foo" in out


def test_create_mirror_topics_continues_after_failure():
    from mirror_topics import create_mirror_topics
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    responses = [
        MagicMock(returncode=1, stderr="already exists", stdout=""),
        MagicMock(returncode=0, stdout="Created."),
        MagicMock(returncode=0, stdout="Created."),
        MagicMock(returncode=0, stdout="Created."),
    ]
    with patch("subprocess.run", side_effect=responses):
        failed = create_mirror_topics(cfg, ["foo", "bar"], dry_run=False)
    assert len(failed) == 1
    assert "foo" in failed[0]
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd mirror-topics && pytest tests/test_mirror_topics.py::test_create_mirror_topics_builds_correct_commands tests/test_mirror_topics.py::test_create_mirror_topics_dry_run_no_subprocess tests/test_mirror_topics.py::test_create_mirror_topics_continues_after_failure -v
```

Expected: FAIL — `create_mirror_topics` not defined.

- [ ] **Step 3: Implement**

Add to `mirror-topics/mirror_topics.py`:

```python
def create_mirror_topics(cfg: dict, topics: list[str], dry_run: bool) -> list[str]:
    """Create mirror topics for each selected topic on both links.

    Returns list of failure descriptions. Does not abort on individual failures.
    """
    failures = []
    for link_key, prefix in [("link_name_4100", "4100."), ("link_name_4200", "4200.")]:
        link = cfg[link_key]
        for topic in topics:
            dest = f"{prefix}{topic}"
            cmd = [
                "confluent", "kafka", "mirror", "create", dest,
                "--link", link,
                "--source-topic", topic,
                "--environment", cfg["environment_id"],
                "--cluster", cfg["cluster_id"],
            ]
            if dry_run:
                print("Dry run — would execute:")
                print("  " + " ".join(cmd))
                continue
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                msg = f"{dest} on {link}: {result.stderr.strip()}"
                print(f"Error: {msg}", file=sys.stderr)
                failures.append(msg)
            else:
                print(f"Created mirror topic {dest} on {link}.")
    return failures
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd mirror-topics && pytest tests/test_mirror_topics.py::test_create_mirror_topics_builds_correct_commands tests/test_mirror_topics.py::test_create_mirror_topics_dry_run_no_subprocess tests/test_mirror_topics.py::test_create_mirror_topics_continues_after_failure -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add mirror-topics/mirror_topics.py mirror-topics/tests/test_mirror_topics.py
git commit -m "feat: per-topic mirror creation across both links"
```

---

### Task 7: Checkbox UI and main() wiring

**Files:**
- Modify: `mirror-topics/mirror_topics.py`

No new unit tests for the questionary UI (interactive TTY — integration tested manually). The existing tests cover all called functions. `main()` is a thin orchestrator.

- [ ] **Step 1: Add shared CLI guards (check_confluent_cli, check_auth, load_pem_files)**

These are identical in behavior to `create_link.py`. Add to `mirror-topics/mirror_topics.py`:

```python
import shutil

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


def check_auth(environment_id: str, cluster_id: str) -> None:
    result = subprocess.run(
        ["confluent", "kafka", "cluster", "describe", cluster_id,
         "--environment", environment_id],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("Error: Not authenticated or cluster not accessible. Run: confluent login",
              file=sys.stderr)
        sys.exit(1)


def load_pem_files(pem_dir: str):
    files = {"ca.pem": None, "client-cert.pem": None, "client-key.pem": None}
    for name in files:
        path = os.path.join(pem_dir, name)
        if not os.path.exists(path):
            print(f"Error: {name} not found in {pem_dir}. Run: python extract_pem.py first",
                  file=sys.stderr)
            sys.exit(1)
        files[name] = path  # pass paths to kafka-python, not bytes
    return files["ca.pem"], files["client-cert.pem"], files["client-key.pem"]
```

- [ ] **Step 2: Add main()**

```python
import questionary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mirror ServiceNow Kafka topics to Confluent Cloud across both cluster links."
    )
    parser.add_argument("--config", default="../cluster-link/link.conf",
                        help="Path to link.conf (default: ../cluster-link/link.conf)")
    parser.add_argument("--pem-dir", default=".",
                        help="Directory containing PEM files (default: ./)")
    parser.add_argument("--filter", default=None, metavar="PREFIX",
                        help="Pre-filter topics by prefix before showing UI")
    parser.add_argument("--all", action="store_true",
                        help="Enable auto-mirror on both links (skip UI)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")
    args = parser.parse_args()

    cfg = load_config(args.config)
    check_confluent_cli()
    check_auth(cfg["environment_id"], cfg["cluster_id"])
    ca, cert, key = load_pem_files(args.pem_dir)

    if args.all:
        enable_auto_mirror(cfg, dry_run=args.dry_run)
        return

    topics = list_source_topics(
        cfg["source_host"], 4100, ca, cert, key, filter_prefix=args.filter
    )

    if not topics:
        msg = f"No topics found matching filter '{args.filter}'." if args.filter else "No topics found on source cluster."
        print(msg)
        return

    already_mirrored = get_mirrored_source_topics(cfg)

    choices = []
    for t in topics:
        label = f"{t}  [mirrored]" if t in already_mirrored else t
        choices.append(questionary.Choice(title=label, value=t))

    selected = questionary.checkbox(
        "Select topics to mirror (space to select, enter to confirm):",
        choices=choices,
    ).ask()

    if not selected:
        print("No topics selected.")
        return

    print(f"\nWill create {len(selected)} mirror topic(s) on {cfg['link_name_4100']} and {cfg['link_name_4200']}:")
    for t in selected:
        print(f"  4100.{t}  →  {cfg['link_name_4100']}")
        print(f"  4200.{t}  →  {cfg['link_name_4200']}")

    if not questionary.confirm("\nProceed?", default=False).ask():
        print("Aborted.")
        return

    failures = create_mirror_topics(cfg, selected, dry_run=args.dry_run)

    if failures:
        print(f"\n{len(failures)} failure(s):")
        for f in failures:
            print(f"  {f}")
        sys.exit(1)
    elif not args.dry_run:
        print(f"\nDone. {len(selected) * 2} mirror topic(s) created.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run full test suite**

```bash
cd mirror-topics && pytest tests/ -v
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add mirror-topics/mirror_topics.py
git commit -m "feat: checkbox UI and main() wiring for mirror-topics"
```

---

### Task 8: Feature branch and push

- [ ] **Step 1: Verify on feature branch**

```bash
git log --oneline main..HEAD
```

All commits from Tasks 1–7 should appear. If work was done on main, create the branch now:

```bash
git checkout -b feature/mirror-topic-config
```

- [ ] **Step 2: Push**

```bash
git push -u origin feature/mirror-topic-config
```

- [ ] **Step 3: Smoke test --dry-run**

```bash
cd mirror-topics
python mirror_topics.py --dry-run --filter sn_
```

Verify: topics listed, confirmation printed, no CLI calls executed.

```bash
python mirror_topics.py --all --dry-run
```

Verify: two `confluent kafka link configuration update` commands printed, no execution.
