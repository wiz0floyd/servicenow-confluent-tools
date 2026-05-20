"""Tests for sn_confluent.api_key.main."""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from sn_confluent.api_key import main as ak


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_proc(returncode=0, stdout="", stderr=""):
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


# ---------------------------------------------------------------------------
# create_kafka_api_key
# ---------------------------------------------------------------------------

def test_create_kafka_api_key_success():
    payload = json.dumps({"key": "MYAPIKEY", "secret": "MYSECRET"})
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        key, secret = ak.create_kafka_api_key("env-1", "lkc-1")
    assert key == "MYAPIKEY"
    assert secret == "MYSECRET"


def test_create_kafka_api_key_alternate_field_names():
    payload = json.dumps({"api_key": "ALTKEY", "api_secret": "ALTSECRET"})
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        key, secret = ak.create_kafka_api_key("env-1", "lkc-1")
    assert key == "ALTKEY"
    assert secret == "ALTSECRET"


def test_create_kafka_api_key_with_description_and_sa():
    captured = {}

    def fake_run(cmd, **_kwargs):
        captured["cmd"] = cmd
        payload = json.dumps({"key": "K", "secret": "S"})
        return _make_proc(stdout=payload)

    with patch("subprocess.run", side_effect=fake_run):
        ak.create_kafka_api_key("env-1", "lkc-1", description="test desc", service_account="sa-abc")

    assert "--description" in captured["cmd"]
    assert "test desc" in captured["cmd"]
    assert "--service-account" in captured["cmd"]
    assert "sa-abc" in captured["cmd"]


def test_create_kafka_api_key_cli_failure_exits():
    with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="auth error")):
        with pytest.raises(SystemExit):
            ak.create_kafka_api_key("env-1", "lkc-1")


def test_create_kafka_api_key_bad_json_exits():
    with patch("subprocess.run", return_value=_make_proc(stdout="not-json")):
        with pytest.raises(SystemExit):
            ak.create_kafka_api_key("env-1", "lkc-1")


def test_create_kafka_api_key_missing_fields_exits():
    with patch("subprocess.run", return_value=_make_proc(stdout=json.dumps({"other": "data"}))):
        with pytest.raises(SystemExit):
            ak.create_kafka_api_key("env-1", "lkc-1")


# ---------------------------------------------------------------------------
# list_kafka_api_keys
# ---------------------------------------------------------------------------

def test_list_kafka_api_keys_success():
    payload = json.dumps([
        {"key": "KEY1", "owner_resource_id": "u-abc", "description": "test"},
        {"key": "KEY2", "owner_resource_id": "u-def", "description": ""},
    ])
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        result = ak.list_kafka_api_keys("lkc-1", "env-1")
    assert len(result) == 2
    assert result[0]["key"] == "KEY1"


def test_list_kafka_api_keys_no_environment():
    captured = {}

    def fake_run(cmd, **_kwargs):
        captured["cmd"] = cmd
        return _make_proc(stdout="[]")

    with patch("subprocess.run", side_effect=fake_run):
        ak.list_kafka_api_keys("lkc-1")

    assert "--environment" not in captured["cmd"]


def test_list_kafka_api_keys_cli_failure_returns_none():
    with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="bad")):
        assert ak.list_kafka_api_keys("lkc-1") is None


def test_list_kafka_api_keys_bad_json_returns_none():
    with patch("subprocess.run", return_value=_make_proc(stdout="not-json")):
        assert ak.list_kafka_api_keys("lkc-1") is None


# ---------------------------------------------------------------------------
# delete_kafka_api_key
# ---------------------------------------------------------------------------

def test_delete_kafka_api_key_success():
    with patch("subprocess.run", return_value=_make_proc()):
        assert ak.delete_kafka_api_key("MYAPIKEY") is True


def test_delete_kafka_api_key_cli_failure_returns_false():
    with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="not found")):
        assert ak.delete_kafka_api_key("MYAPIKEY") is False


# ---------------------------------------------------------------------------
# main — create action
# ---------------------------------------------------------------------------

def test_main_create_prints_key_and_hint(capsys):
    payload = json.dumps({"key": "NEWKEY", "secret": "NEWSECRET"})
    with patch("sn_confluent.api_key.main.check_confluent_cli"):
        with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
            rc = ak.main(["create", "--environment", "env-1", "--cluster", "lkc-1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "NEWKEY" in out
    assert "NEWSECRET" in out
    assert "deploy.conf" in out


def test_main_create_missing_args_exits():
    with patch("sn_confluent.api_key.main.check_confluent_cli"):
        with pytest.raises(SystemExit):
            ak.main(["create", "--environment", "env-1"])  # missing --cluster


# ---------------------------------------------------------------------------
# main — list action
# ---------------------------------------------------------------------------

def test_main_list_prints_table(capsys):
    payload = json.dumps([
        {"key": "KEY1", "owner_resource_id": "u-abc", "description": "prod"},
    ])
    with patch("sn_confluent.api_key.main.check_confluent_cli"):
        with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
            rc = ak.main(["list", "--cluster", "lkc-1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "KEY1" in out
    assert "u-abc" in out


def test_main_list_empty(capsys):
    with patch("sn_confluent.api_key.main.check_confluent_cli"):
        with patch("subprocess.run", return_value=_make_proc(stdout="[]")):
            rc = ak.main(["list", "--cluster", "lkc-1"])
    assert rc == 0
    assert "No API keys" in capsys.readouterr().out


def test_main_list_cli_failure_returns_1():
    with patch("sn_confluent.api_key.main.check_confluent_cli"):
        with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="err")):
            rc = ak.main(["list", "--cluster", "lkc-1"])
    assert rc == 1


# ---------------------------------------------------------------------------
# main — delete action
# ---------------------------------------------------------------------------

def test_main_delete_success(capsys):
    with patch("sn_confluent.api_key.main.check_confluent_cli"):
        with patch("subprocess.run", return_value=_make_proc()):
            rc = ak.main(["delete", "MYAPIKEY"])
    assert rc == 0
    assert "MYAPIKEY" in capsys.readouterr().out


def test_main_delete_failure_returns_1():
    with patch("sn_confluent.api_key.main.check_confluent_cli"):
        with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="not found")):
            rc = ak.main(["delete", "MYAPIKEY"])
    assert rc == 1
