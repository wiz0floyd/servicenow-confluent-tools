"""Tests for sn_confluent.worker.main."""

import json
import os
import stat
from unittest.mock import MagicMock, call, patch

import pytest

from sn_confluent.worker.main import (
    DEFAULT_GROUP_ID,
    DEFAULT_KAFKA_HOME,
    DEFAULT_PLUGIN_DIR,
    DEFAULT_REST_PORT,
    ensure_plugin,
    generate_worker_properties,
    get_cc_bootstrap,
    load_config,
    resolve_api_credentials,
    write_start_script,
    main,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write_conf(tmp_path, content: str) -> str:
    p = tmp_path / "worker.conf"
    p.write_text(content)
    return str(p)


MINIMAL_CONF = """\
[confluent]
environment_id = env-abc123
cluster_id     = lkc-xyz789
"""


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_loads_required_keys(self, tmp_path):
        path = _write_conf(tmp_path, MINIMAL_CONF)
        cfg = load_config(path)
        assert cfg["environment_id"] == "env-abc123"
        assert cfg["cluster_id"] == "lkc-xyz789"

    def test_missing_required_key_exits(self, tmp_path):
        path = _write_conf(tmp_path, "[confluent]\nenvironment_id = env-x\n")
        with pytest.raises(SystemExit) as exc:
            load_config(path)
        assert exc.value.code == 1

    def test_optional_keys_absent_returns_none(self, tmp_path):
        path = _write_conf(tmp_path, MINIMAL_CONF)
        cfg = load_config(path)
        assert cfg.get("cc_api_key") is None
        assert cfg.get("plugin_dir") is None

    def test_file_not_found_exits(self, tmp_path):
        with pytest.raises(SystemExit) as exc:
            load_config(str(tmp_path / "nonexistent.conf"))
        assert exc.value.code == 1


# ---------------------------------------------------------------------------
# get_cc_bootstrap
# ---------------------------------------------------------------------------

class TestGetCcBootstrap:
    @patch("sn_confluent.worker.main.subprocess.run")
    def test_strips_sasl_ssl_prefix(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps({"endpoint": "SASL_SSL://pkc-abc.us-east1.gcp.confluent.cloud:9092"}),
        )
        result = get_cc_bootstrap("env-x", "lkc-y")
        assert result == "pkc-abc.us-east1.gcp.confluent.cloud:9092"

    @patch("sn_confluent.worker.main.subprocess.run")
    def test_cli_failure_exits(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="not found")
        with pytest.raises(SystemExit) as exc:
            get_cc_bootstrap("env-x", "lkc-y")
        assert exc.value.code == 1

    @patch("sn_confluent.worker.main.subprocess.run")
    def test_missing_endpoint_key_exits(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout=json.dumps({"name": "my-cluster"}))
        with pytest.raises(SystemExit) as exc:
            get_cc_bootstrap("env-x", "lkc-y")
        assert exc.value.code == 1


# ---------------------------------------------------------------------------
# resolve_api_credentials
# ---------------------------------------------------------------------------

class TestResolveApiCredentials:
    def test_env_vars_win(self, monkeypatch):
        monkeypatch.setenv("CC_API_KEY", "key-from-env")
        monkeypatch.setenv("CC_API_SECRET", "secret-from-env")
        key, secret = resolve_api_credentials({})
        assert key == "key-from-env"
        assert secret == "secret-from-env"

    def test_config_fallback(self, monkeypatch):
        monkeypatch.delenv("CC_API_KEY", raising=False)
        monkeypatch.delenv("CC_API_SECRET", raising=False)
        cfg = {"cc_api_key": "cfg-key", "cc_api_secret": "cfg-secret"}
        key, secret = resolve_api_credentials(cfg)
        assert key == "cfg-key"
        assert secret == "cfg-secret"

    def test_env_beats_config(self, monkeypatch):
        monkeypatch.setenv("CC_API_KEY", "env-key")
        monkeypatch.setenv("CC_API_SECRET", "env-secret")
        cfg = {"cc_api_key": "cfg-key", "cc_api_secret": "cfg-secret"}
        key, secret = resolve_api_credentials(cfg)
        assert key == "env-key"
        assert secret == "env-secret"

    def test_prompt_fallback(self, monkeypatch):
        monkeypatch.delenv("CC_API_KEY", raising=False)
        monkeypatch.delenv("CC_API_SECRET", raising=False)
        with patch("sn_confluent.worker.main.input", return_value="prompt-key"), \
             patch("sn_confluent.worker.main.getpass.getpass", return_value="prompt-secret"):
            key, secret = resolve_api_credentials({})
        assert key == "prompt-key"
        assert secret == "prompt-secret"

    def test_empty_key_exits(self, monkeypatch):
        monkeypatch.delenv("CC_API_KEY", raising=False)
        monkeypatch.delenv("CC_API_SECRET", raising=False)
        with patch("sn_confluent.worker.main.input", return_value=""), \
             patch("sn_confluent.worker.main.getpass.getpass", return_value=""), \
             pytest.raises(SystemExit) as exc:
            resolve_api_credentials({})
        assert exc.value.code == 1


# ---------------------------------------------------------------------------
# generate_worker_properties
# ---------------------------------------------------------------------------

class TestGenerateWorkerProperties:
    def _gen(self, **kwargs):
        defaults = dict(
            bootstrap="pkc-abc.confluent.cloud:9092",
            api_key="MY_KEY",
            api_secret="MY_SECRET",
            group_id="connect-cluster",
            plugin_dir="./plugins",
            rest_port=8083,
        )
        defaults.update(kwargs)
        return generate_worker_properties(**defaults)

    def test_bootstrap_present(self):
        props = self._gen(bootstrap="host:9092")
        assert "bootstrap.servers=host:9092" in props

    def test_override_policy_hardcoded(self):
        props = self._gen()
        assert "connector.client.config.override.policy=All" in props

    def test_ssl_endpoint_identification(self):
        props = self._gen()
        assert "ssl.endpoint.identification.algorithm=https" in props

    def test_sasl_jaas_contains_credentials(self):
        props = self._gen(api_key="K", api_secret="S")
        assert 'username="K"' in props
        assert 'password="S"' in props

    def test_byte_array_converters(self):
        props = self._gen()
        assert "ByteArrayConverter" in props
        assert "key.converter.schemas.enable=false" in props
        assert "value.converter.schemas.enable=false" in props

    def test_internal_topic_names(self):
        props = self._gen()
        assert "config.storage.topic=_connect-configs" in props
        assert "offset.storage.topic=_connect-offsets" in props
        assert "status.storage.topic=_connect-status" in props

    def test_replication_factor_3(self):
        props = self._gen()
        assert "config.storage.replication.factor=3" in props
        assert "offset.storage.replication.factor=3" in props
        assert "status.storage.replication.factor=3" in props

    def test_rest_port(self):
        props = self._gen(rest_port=9083)
        assert "rest.port=9083" in props

    def test_group_id(self):
        props = self._gen(group_id="my-group")
        assert "group.id=my-group" in props

    def test_plugin_dir_is_absolute(self, tmp_path):
        props = self._gen(plugin_dir=str(tmp_path))
        assert f"plugin.path={tmp_path}" in props


# ---------------------------------------------------------------------------
# ensure_plugin
# ---------------------------------------------------------------------------

class TestEnsurePlugin:
    def test_found_prints_and_returns(self, tmp_path, capsys):
        jar = tmp_path / "kafka-connect-replicator-1.0.jar"
        jar.write_bytes(b"")
        ensure_plugin(str(tmp_path), auto_install=False)
        out = capsys.readouterr().out
        assert "found" in out

    @patch("sn_confluent.worker.main.shutil.which", return_value="/usr/bin/confluent-hub")
    @patch("sn_confluent.worker.main.subprocess.run")
    def test_auto_install_calls_confluent_hub(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0)
        ensure_plugin(str(tmp_path), auto_install=True)
        assert mock_run.called
        cmd = mock_run.call_args[0][0]
        assert "confluent-hub" in cmd
        assert "confluentinc/kafka-connect-replicator:latest" in cmd

    @patch("sn_confluent.worker.main.shutil.which", return_value="/usr/bin/confluent-hub")
    @patch("sn_confluent.worker.main.subprocess.run", return_value=MagicMock(returncode=1))
    def test_auto_install_failure_exits(self, mock_run, mock_which, tmp_path):
        with pytest.raises(SystemExit) as exc:
            ensure_plugin(str(tmp_path), auto_install=True)
        assert exc.value.code == 1

    @patch("sn_confluent.worker.main.shutil.which", return_value=None)
    def test_no_hub_prints_instructions(self, mock_which, tmp_path, capsys):
        ensure_plugin(str(tmp_path), auto_install=False)
        out = capsys.readouterr().out
        assert "confluent.io" in out or "confluent-hub" in out

    @patch("sn_confluent.worker.main.shutil.which", return_value="/usr/bin/confluent-hub")
    def test_hub_available_no_auto_prints_hint(self, mock_which, tmp_path, capsys):
        ensure_plugin(str(tmp_path), auto_install=False)
        out = capsys.readouterr().out
        assert "--auto-install" in out


# ---------------------------------------------------------------------------
# write_start_script
# ---------------------------------------------------------------------------

class TestWriteStartScript:
    def test_file_is_created(self, tmp_path):
        path = write_start_script(str(tmp_path), "connect-worker.properties", "/opt/confluent")
        assert os.path.exists(path)

    def test_file_is_executable(self, tmp_path):
        path = write_start_script(str(tmp_path), "connect-worker.properties", "/opt/confluent")
        if os.name != "nt":
            mode = os.stat(path).st_mode
            assert mode & stat.S_IXUSR

    def test_kafka_home_in_content(self, tmp_path):
        path = write_start_script(str(tmp_path), "connect-worker.properties", "/opt/confluent")
        content = open(path).read()
        assert "/opt/confluent" in content

    def test_props_filename_in_content(self, tmp_path):
        path = write_start_script(str(tmp_path), "connect-worker.properties", "/opt/confluent")
        content = open(path).read()
        assert "connect-worker.properties" in content

    def test_shebang_present(self, tmp_path):
        path = write_start_script(str(tmp_path), "connect-worker.properties", "/opt/confluent")
        first_line = open(path).readline()
        assert first_line.startswith("#!/usr/bin/env bash")


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------

class TestMain:
    def _make_conf(self, tmp_path) -> str:
        conf = tmp_path / "worker.conf"
        conf.write_text(MINIMAL_CONF)
        return str(conf)

    @patch("sn_confluent.worker.main.check_confluent_cli")
    @patch("sn_confluent.worker.main.get_cc_bootstrap", return_value="host:9092")
    @patch("sn_confluent.worker.main.resolve_api_credentials", return_value=("K", "S"))
    @patch("sn_confluent.worker.main.ensure_plugin")
    def test_dry_run_prints_no_files(
        self, mock_plugin, mock_creds, mock_bootstrap, mock_cli, tmp_path, capsys
    ):
        conf = self._make_conf(tmp_path)
        rc = main(["--config", conf, "--dry-run"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "bootstrap.servers=host:9092" in out
        assert "dry-run" in out
        # No files written to tmp_path beyond the conf
        written = [f for f in os.listdir(tmp_path) if f != "worker.conf"]
        assert written == []

    @patch("sn_confluent.worker.main.check_confluent_cli")
    @patch("sn_confluent.worker.main.get_cc_bootstrap", return_value="host:9092")
    @patch("sn_confluent.worker.main.resolve_api_credentials", return_value=("K", "S"))
    @patch("sn_confluent.worker.main.ensure_plugin")
    def test_writes_properties_and_script(
        self, mock_plugin, mock_creds, mock_bootstrap, mock_cli, tmp_path
    ):
        conf = self._make_conf(tmp_path)
        out_dir = str(tmp_path / "out")
        rc = main(["--config", conf, "--out-dir", out_dir])
        assert rc == 0
        assert os.path.exists(os.path.join(out_dir, "connect-worker.properties"))
        assert os.path.exists(os.path.join(out_dir, "start-worker.sh"))

    @patch("sn_confluent.worker.main.check_confluent_cli")
    @patch("sn_confluent.worker.main.get_cc_bootstrap", return_value="host:9092")
    @patch("sn_confluent.worker.main.resolve_api_credentials", return_value=("K", "S"))
    @patch("sn_confluent.worker.main.ensure_plugin")
    def test_properties_file_mode_600(
        self, mock_plugin, mock_creds, mock_bootstrap, mock_cli, tmp_path
    ):
        conf = self._make_conf(tmp_path)
        out_dir = str(tmp_path / "out")
        main(["--config", conf, "--out-dir", out_dir])
        props = os.path.join(out_dir, "connect-worker.properties")
        mode = os.stat(props).st_mode & 0o777
        # On Windows the chmod may be a no-op; skip the assertion there
        if os.name != "nt":
            assert mode == 0o600

    @patch("sn_confluent.worker.main.check_confluent_cli")
    @patch("sn_confluent.worker.main.get_cc_bootstrap", return_value="host:9092")
    @patch("sn_confluent.worker.main.resolve_api_credentials", return_value=("K", "S"))
    @patch("sn_confluent.worker.main.ensure_plugin")
    def test_next_steps_printed(
        self, mock_plugin, mock_creds, mock_bootstrap, mock_cli, tmp_path, capsys
    ):
        conf = self._make_conf(tmp_path)
        out_dir = str(tmp_path / "out")
        main(["--config", conf, "--out-dir", out_dir])
        out = capsys.readouterr().out
        assert "sn-confluent replicate" in out
