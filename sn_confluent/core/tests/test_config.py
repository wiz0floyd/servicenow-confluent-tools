import sys
import textwrap
import pytest

from sn_confluent.core.config import load_config


def _write(tmp_path, content):
    p = tmp_path / "link.conf"
    p.write_text(textwrap.dedent(content))
    return str(p)


SINGLE_SECTION = """\
    [confluent]
    environment_id = env-prod
    cluster_id     = lkc-prod
    link_name      = sn-prod
    source_host    = hermes1.example.com
"""

PROFILE_FILE = """\
    [DEFAULT]
    environment_id = env-prod
    cluster_id     = lkc-prod
    link_name      = sn-prod
    source_host    = hermes1.example.com

    [dev]
    environment_id = env-dev
    cluster_id     = lkc-dev

    [staging]
    link_name = sn-staging
"""

REQUIRED = ["environment_id", "cluster_id", "link_name", "source_host"]


# ---------------------------------------------------------------------------
# profile=None — existing behaviour unchanged
# ---------------------------------------------------------------------------

def test_no_profile_reads_confluent_section(tmp_path):
    path = _write(tmp_path, SINGLE_SECTION)
    cfg = load_config(path, REQUIRED)
    assert cfg["environment_id"] == "env-prod"
    assert cfg["cluster_id"] == "lkc-prod"
    assert cfg["link_name"] == "sn-prod"
    assert cfg["source_host"] == "hermes1.example.com"


def test_no_profile_missing_confluent_section_exits(tmp_path):
    path = _write(tmp_path, "[other]\nkey = val\n")
    with pytest.raises(SystemExit) as exc:
        load_config(path, ["key"])
    assert exc.value.code == 1


def test_no_profile_missing_required_key_exits(tmp_path):
    path = _write(tmp_path, "[confluent]\nenvironment_id = env-x\n")
    with pytest.raises(SystemExit) as exc:
        load_config(path, ["environment_id", "cluster_id"])
    assert exc.value.code == 1


def test_no_profile_file_not_found_exits(tmp_path):
    with pytest.raises(SystemExit) as exc:
        load_config(str(tmp_path / "missing.conf"), [])
    assert exc.value.code == 1


# ---------------------------------------------------------------------------
# profile set — named section with DEFAULT inheritance
# ---------------------------------------------------------------------------

def test_profile_reads_named_section(tmp_path):
    path = _write(tmp_path, PROFILE_FILE)
    cfg = load_config(path, REQUIRED, profile="dev")
    assert cfg["environment_id"] == "env-dev"
    assert cfg["cluster_id"] == "lkc-dev"


def test_profile_inherits_default_keys(tmp_path):
    path = _write(tmp_path, PROFILE_FILE)
    cfg = load_config(path, REQUIRED, profile="dev")
    # link_name and source_host not in [dev]; inherited from [DEFAULT]
    assert cfg["link_name"] == "sn-prod"
    assert cfg["source_host"] == "hermes1.example.com"


def test_profile_override_different_key(tmp_path):
    path = _write(tmp_path, PROFILE_FILE)
    cfg = load_config(path, ["link_name", "environment_id", "cluster_id", "source_host"], profile="staging")
    assert cfg["link_name"] == "sn-staging"
    # non-overridden keys still come from DEFAULT
    assert cfg["environment_id"] == "env-prod"


def test_profile_not_found_exits(tmp_path, capsys):
    path = _write(tmp_path, PROFILE_FILE)
    with pytest.raises(SystemExit) as exc:
        load_config(path, REQUIRED, profile="missing")
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "missing" in err
    assert "Available profiles" in err


def test_profile_not_found_lists_available(tmp_path, capsys):
    path = _write(tmp_path, PROFILE_FILE)
    with pytest.raises(SystemExit):
        load_config(path, REQUIRED, profile="nope")
    err = capsys.readouterr().err
    assert "dev" in err
    assert "staging" in err


def test_profile_required_key_missing_exits(tmp_path):
    # [dev] and [DEFAULT] together don't satisfy all required keys
    content = "[DEFAULT]\nenvironment_id = env-x\n[dev]\ncluster_id = lkc-x\n"
    path = _write(tmp_path, content)
    with pytest.raises(SystemExit) as exc:
        load_config(path, ["environment_id", "cluster_id", "link_name"], profile="dev")
    assert exc.value.code == 1


def test_profile_no_default_section(tmp_path):
    # Named section with all keys present, no [DEFAULT] needed
    content = "[prod]\nenvironment_id = e\ncluster_id = c\nlink_name = l\nsource_host = h\n"
    path = _write(tmp_path, content)
    cfg = load_config(path, REQUIRED, profile="prod")
    assert cfg["environment_id"] == "e"
    assert cfg["source_host"] == "h"

# ---------------------------------------------------------------------------
# edge cases: DEFAULT reserved name, case sensitivity, error message quality
# ---------------------------------------------------------------------------

def test_profile_default_reserved_name_exits(tmp_path, capsys):
    # 'DEFAULT' is configparser's reserved pseudo-section; must never succeed as a profile
    path = _write(tmp_path, PROFILE_FILE)
    with pytest.raises(SystemExit) as exc:
        load_config(path, REQUIRED, profile="DEFAULT")
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "DEFAULT" in err


def test_profile_case_mismatch_suggests_correction(tmp_path, capsys):
    # Section names are case-sensitive; a near-miss should produce a "did you mean?" hint
    path = _write(tmp_path, PROFILE_FILE)
    with pytest.raises(SystemExit) as exc:
        load_config(path, REQUIRED, profile="DEV")
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "Did you mean 'dev'" in err


def test_missing_section_error_includes_path(tmp_path, capsys):
    path = _write(tmp_path, "[other]\nkey = val\n")
    with pytest.raises(SystemExit):
        load_config(path, ["key"])
    err = capsys.readouterr().err
    assert str(path) in err


def test_missing_key_error_includes_path(tmp_path, capsys):
    path = _write(tmp_path, "[confluent]\nenvironment_id = env-x\n")
    with pytest.raises(SystemExit):
        load_config(path, ["environment_id", "cluster_id"])
    err = capsys.readouterr().err
    assert str(path) in err


def test_profile_only_default_section_shows_helpful_message(tmp_path, capsys):
    # When only [DEFAULT] exists, error should hint to add a named section
    content = "[DEFAULT]\nenvironment_id = e\ncluster_id = c\nlink_name = l\nsource_host = h\n"
    path = _write(tmp_path, content)
    with pytest.raises(SystemExit) as exc:
        load_config(path, REQUIRED, profile="dev")
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "add a named section" in err
