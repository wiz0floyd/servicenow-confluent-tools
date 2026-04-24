import os
import sys
import tempfile
import textwrap
import pytest
from unittest.mock import patch, MagicMock


VALID_CONFIG = textwrap.dedent("""\
    [confluent]
    environment_id   = env-abc123
    cluster_id       = lkc-abc123
    link_name        = test-link
    source_bootstrap = broker.example.com:9093
""")


def write_config(tmp_path, content=VALID_CONFIG):
    p = tmp_path / "link.conf"
    p.write_text(content)
    return str(p)


# ---------------------------------------------------------------------------
# sn_bootstrap
# ---------------------------------------------------------------------------

def test_sn_bootstrap_4100():
    from create_link import sn_bootstrap
    result = sn_bootstrap("hermes1.service-now.com", 4100)
    assert result == "hermes1.service-now.com:4100,hermes1.service-now.com:4101,hermes1.service-now.com:4102,hermes1.service-now.com:4103"


def test_sn_bootstrap_4200():
    from create_link import sn_bootstrap
    result = sn_bootstrap("hermes1.service-now.com", 4200)
    assert result == "hermes1.service-now.com:4200,hermes1.service-now.com:4201,hermes1.service-now.com:4202,hermes1.service-now.com:4203"


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

def test_load_config_returns_dict(tmp_path):
    from create_link import load_config
    path = write_config(tmp_path)
    cfg = load_config(path)
    assert cfg["environment_id"] == "env-abc123"
    assert cfg["cluster_id"] == "lkc-abc123"
    assert cfg["link_name"] == "test-link"
    assert cfg["source_bootstrap"] == "broker.example.com:9093"


def test_load_config_exits_if_file_missing():
    from create_link import load_config
    with pytest.raises(SystemExit) as exc:
        load_config("/nonexistent/link.conf")
    assert exc.value.code == 1


def test_load_config_exits_if_key_missing(tmp_path):
    from create_link import load_config
    bad = textwrap.dedent("""\
        [confluent]
        environment_id = env-abc123
        cluster_id     = lkc-abc123
    """)
    path = write_config(tmp_path, bad)
    with pytest.raises(SystemExit) as exc:
        load_config(path)
    assert exc.value.code == 1


def test_load_config_accepts_source_host(tmp_path):
    from create_link import load_config
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id = env-abc123
        cluster_id     = lkc-abc123
        link_name      = test-link
        source_host    = hermes1.service-now.com
    """)
    path = write_config(tmp_path, cfg_text)
    cfg = load_config(path)
    assert cfg["source_host"] == "hermes1.service-now.com"


def test_load_config_exits_if_neither_source_key(tmp_path):
    from create_link import load_config
    bad = textwrap.dedent("""\
        [confluent]
        environment_id = env-abc123
        cluster_id     = lkc-abc123
        link_name      = test-link
    """)
    path = write_config(tmp_path, bad)
    with pytest.raises(SystemExit) as exc:
        load_config(path)
    assert exc.value.code == 1


# ---------------------------------------------------------------------------
# check_confluent_cli
# ---------------------------------------------------------------------------

def test_check_confluent_cli_passes_when_found():
    from create_link import check_confluent_cli
    with patch("shutil.which", return_value="/usr/local/bin/confluent"):
        check_confluent_cli()  # Should not raise


def test_check_confluent_cli_exits_when_missing(capsys):
    from create_link import check_confluent_cli
    with patch("shutil.which", return_value=None):
        with pytest.raises(SystemExit) as exc:
            check_confluent_cli()
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "winget" in captured.out or "brew" in captured.out


# ---------------------------------------------------------------------------
# check_auth
# ---------------------------------------------------------------------------

def test_check_auth_passes_when_cluster_describe_succeeds():
    from create_link import check_auth
    mock_result = MagicMock(returncode=0)
    with patch("subprocess.run", return_value=mock_result):
        check_auth("env-abc123", "lkc-abc123")  # Should not raise


def test_check_auth_exits_when_cluster_describe_fails(capsys):
    from create_link import check_auth
    mock_result = MagicMock(returncode=1, stderr="Unauthorized")
    with patch("subprocess.run", return_value=mock_result):
        with pytest.raises(SystemExit) as exc:
            check_auth("env-abc123", "lkc-abc123")
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "confluent login" in captured.err


# ---------------------------------------------------------------------------
# load_pem_files
# ---------------------------------------------------------------------------

def test_load_pem_files_returns_bytes(tmp_path):
    from create_link import load_pem_files
    (tmp_path / "ca.pem").write_bytes(b"CA")
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")
    ca, cert, key = load_pem_files(str(tmp_path))
    assert ca == b"CA"
    assert cert == b"CERT"
    assert key == b"KEY"


def test_load_pem_files_exits_if_ca_missing(tmp_path, capsys):
    from create_link import load_pem_files
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")
    with pytest.raises(SystemExit) as exc:
        load_pem_files(str(tmp_path))
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "extract_pem.py" in captured.err


# ---------------------------------------------------------------------------
# build_ssl_properties
# ---------------------------------------------------------------------------

def test_build_ssl_properties_contains_required_keys():
    from create_link import build_ssl_properties
    result = build_ssl_properties(b"CA-PEM", b"CERT-PEM", b"KEY-PEM")
    assert "ssl.truststore.type=PEM" in result
    assert "ssl.keystore.type=PEM" in result
    assert "ssl.truststore.certificates=" in result
    assert "ssl.keystore.certificate.chain=" in result
    assert "ssl.keystore.key=" in result


def test_build_ssl_properties_escapes_newlines():
    from create_link import build_ssl_properties
    CERT_KEYS = ("ssl.truststore.certificates", "ssl.keystore.certificate.chain", "ssl.keystore.key")
    result = build_ssl_properties(b"LINE1\nLINE2", b"L1\nL2", b"K1\nK2")
    lines = result.splitlines()
    cert_lines = [l for l in lines if any(l.startswith(k) for k in CERT_KEYS)]
    assert len(cert_lines) == 3
    for line in cert_lines:
        val = line.split("=", 1)[1]
        assert "\n" not in val
        assert "\\n" in val


def test_build_ssl_properties_literal_newlines():
    from create_link import build_ssl_properties
    result = build_ssl_properties(b"LINE1\nLINE2", b"L1\nL2", b"K1\nK2", literal_newlines=True)
    assert "\\n" not in result
    assert "LINE1\nLINE2" in result


def test_build_ssl_properties_includes_key_password():
    from create_link import build_ssl_properties
    result = build_ssl_properties(b"CA", b"CERT", b"KEY", key_password="secretpw")
    assert "ssl.key.password=secretpw" in result


def test_build_ssl_properties_omits_key_password_when_not_set():
    from create_link import build_ssl_properties
    result = build_ssl_properties(b"CA", b"CERT", b"KEY")
    assert "ssl.key.password" not in result




# ---------------------------------------------------------------------------
# build_link_command
# ---------------------------------------------------------------------------

def test_build_link_command_returns_correct_args():
    from create_link import build_link_command
    cfg = {
        "link_name": "my-link",
        "environment_id": "env-123",
        "cluster_id": "lkc-123",
        "source_bootstrap": "broker:9093",
    }
    cmd = build_link_command(cfg, "/tmp/ssl.properties")
    assert cmd[0] == "confluent"
    assert "link" in cmd
    assert "create" in cmd
    assert "my-link" in cmd
    assert "--environment" in cmd
    assert "env-123" in cmd
    assert "--cluster" in cmd
    assert "lkc-123" in cmd
    assert "--source-bootstrap-server" in cmd
    assert "broker:9093" in cmd
    assert "--config-file" in cmd
    assert "/tmp/ssl.properties" in cmd


# ---------------------------------------------------------------------------
# dry-run and execution
# ---------------------------------------------------------------------------

def test_dry_run_prints_command_without_running(capsys):
    from create_link import run_link_create
    cmd = ["confluent", "kafka", "link", "create", "test-link"]
    with patch("subprocess.run") as mock_run:
        run_link_create(cmd, dry_run=True)
        mock_run.assert_not_called()
    captured = capsys.readouterr()
    assert "confluent" in captured.out
    assert "test-link" in captured.out


def test_run_link_create_calls_subprocess():
    from create_link import run_link_create
    cmd = ["confluent", "kafka", "link", "create", "test-link"]
    mock_result = MagicMock(returncode=0, stdout="Created link test-link")
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        run_link_create(cmd, dry_run=False)
        mock_run.assert_called_once_with(cmd, capture_output=True, text=True)


def test_run_link_create_exits_on_cli_failure(capsys):
    from create_link import run_link_create
    cmd = ["confluent", "kafka", "link", "create", "test-link"]
    mock_result = MagicMock(returncode=1, stderr="Error: cluster not found")
    with patch("subprocess.run", return_value=mock_result):
        with pytest.raises(SystemExit) as exc:
            run_link_create(cmd, dry_run=False)
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "cluster not found" in captured.err


# ---------------------------------------------------------------------------
# copy_security_config
# ---------------------------------------------------------------------------

def test_copy_security_config_uses_pyperclip(capsys):
    from create_link import copy_security_config
    mock_pyperclip = MagicMock()
    with patch.dict("sys.modules", {"pyperclip": mock_pyperclip}):
        copy_security_config("ssl.truststore.type=PEM\n")
    mock_pyperclip.copy.assert_called_once_with("ssl.truststore.type=PEM\n")
    assert "clipboard" in capsys.readouterr().out


def test_copy_security_config_fallback_windows(capsys):
    from create_link import copy_security_config
    import builtins
    real_import = builtins.__import__

    def no_pyperclip(name, *args, **kwargs):
        if name == "pyperclip":
            raise ImportError
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=no_pyperclip):
        with patch("sys.platform", "win32"):
            with patch("subprocess.run") as mock_run:
                copy_security_config("ssl.truststore.type=PEM\n")
                mock_run.assert_called_once()
                assert mock_run.call_args[0][0] == "clip"
    assert "clipboard" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# wait_for_link_active
# ---------------------------------------------------------------------------

CFG = {
    "link_name": "test-link",
    "environment_id": "env-abc123",
    "cluster_id": "lkc-abc123",
}


def _describe_result(state: str):
    import json
    return MagicMock(returncode=0, stdout=json.dumps({"state": state}))


def test_wait_for_link_active_returns_on_active(capsys):
    from create_link import wait_for_link_active
    with patch("subprocess.run", return_value=_describe_result("ACTIVE")):
        wait_for_link_active(CFG, timeout=10)
    assert "ACTIVE" in capsys.readouterr().out


def test_wait_for_link_active_exits_on_failed(capsys):
    from create_link import wait_for_link_active
    with patch("subprocess.run", return_value=_describe_result("FAILED")):
        with pytest.raises(SystemExit) as exc:
            wait_for_link_active(CFG, timeout=10)
    assert exc.value.code == 1
    assert "FAILED" in capsys.readouterr().err


def test_wait_for_link_active_retries_then_succeeds(capsys):
    from create_link import wait_for_link_active
    responses = [
        _describe_result("PENDING"),
        _describe_result("PENDING"),
        _describe_result("ACTIVE"),
    ]
    with patch("subprocess.run", side_effect=responses):
        with patch("time.sleep"):
            wait_for_link_active(CFG, timeout=60)
    assert "ACTIVE" in capsys.readouterr().out


def test_wait_for_link_active_exits_on_timeout():
    from create_link import wait_for_link_active
    with patch("subprocess.run", return_value=_describe_result("PENDING")):
        with patch("time.monotonic", side_effect=[0, 0, 999]):
            with patch("time.sleep"):
                with pytest.raises(SystemExit) as exc:
                    wait_for_link_active(CFG, timeout=10)
    assert exc.value.code == 1


def test_wait_for_link_active_handles_describe_failure(capsys):
    from create_link import wait_for_link_active
    fail = MagicMock(returncode=1, stdout="", stderr="connection refused")
    responses = [fail, fail, _describe_result("ACTIVE")]
    with patch("subprocess.run", side_effect=responses):
        with patch("time.sleep"):
            wait_for_link_active(CFG, timeout=60)
    assert "ACTIVE" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# build_link_properties
# ---------------------------------------------------------------------------

def test_dual_link_properties_include_4100_prefix():
    from create_link import build_link_properties
    props = build_link_properties(4100)
    assert "cluster.link.prefix=4100." in props

def test_dual_link_properties_include_4200_prefix():
    from create_link import build_link_properties
    props = build_link_properties(4200)
    assert "cluster.link.prefix=4200." in props
