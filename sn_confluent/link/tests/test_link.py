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
    from sn_confluent.link.main import sn_bootstrap
    result = sn_bootstrap("kafka.example.com", 4100)
    assert result == "kafka.example.com:4100,kafka.example.com:4101,kafka.example.com:4102,kafka.example.com:4103"


def test_sn_bootstrap_4200():
    from sn_confluent.link.main import sn_bootstrap
    result = sn_bootstrap("kafka.example.com", 4200)
    assert result == "kafka.example.com:4200,kafka.example.com:4201,kafka.example.com:4202,kafka.example.com:4203"


def test_sn_bootstrap_custom_brokers_per_cluster():
    from sn_confluent.link.main import sn_bootstrap
    result = sn_bootstrap("kafka.example.com", 5000, brokers_per_cluster=2)
    assert result == "kafka.example.com:5000,kafka.example.com:5001"


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

def test_load_config_returns_dict(tmp_path):
    from sn_confluent.link.main import load_config
    path = write_config(tmp_path)
    cfg = load_config(path)
    assert cfg["environment_id"] == "env-abc123"
    assert cfg["cluster_id"] == "lkc-abc123"
    assert cfg["link_name"] == "test-link"
    assert cfg["source_bootstrap"] == "broker.example.com:9093"


def test_load_config_exits_if_file_missing():
    from sn_confluent.link.main import load_config
    with pytest.raises(SystemExit) as exc:
        load_config("/nonexistent/link.conf")
    assert exc.value.code == 1


def test_load_config_exits_if_key_missing(tmp_path):
    from sn_confluent.link.main import load_config
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
    from sn_confluent.link.main import load_config
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id = env-abc123
        cluster_id     = lkc-abc123
        link_name      = test-link
        source_host    = kafka.example.com
    """)
    path = write_config(tmp_path, cfg_text)
    cfg = load_config(path)
    assert cfg["source_host"] == "kafka.example.com"


def test_load_config_defaults_source_clusters_and_brokers(tmp_path):
    from sn_confluent.link.main import load_config, SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER
    path = write_config(tmp_path)
    cfg = load_config(path)
    assert cfg["source_clusters"] == SN_SOURCE_CLUSTERS
    assert cfg["brokers_per_cluster"] == SN_BROKERS_PER_CLUSTER


def test_load_config_reads_custom_source_clusters(tmp_path):
    from sn_confluent.link.main import load_config
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id   = env-abc123
        cluster_id       = lkc-abc123
        link_name        = test-link
        source_bootstrap = broker.example.com:9093
        source_clusters  = 5000, 5100
    """)
    path = write_config(tmp_path, cfg_text)
    cfg = load_config(path)
    assert cfg["source_clusters"] == [5000, 5100]


def test_load_config_reads_custom_brokers_per_cluster(tmp_path):
    from sn_confluent.link.main import load_config
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id      = env-abc123
        cluster_id          = lkc-abc123
        link_name           = test-link
        source_bootstrap    = broker.example.com:9093
        brokers_per_cluster = 2
    """)
    path = write_config(tmp_path, cfg_text)
    cfg = load_config(path)
    assert cfg["brokers_per_cluster"] == 2


def test_load_config_exits_on_non_integer_source_clusters(tmp_path, capsys):
    from sn_confluent.link.main import load_config
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id   = env-abc123
        cluster_id       = lkc-abc123
        link_name        = test-link
        source_bootstrap = broker.example.com:9093
        source_clusters  = 4100, abc
    """)
    path = write_config(tmp_path, cfg_text)
    with pytest.raises(SystemExit) as exc_info:
        load_config(path)
    assert exc_info.value.code == 1
    assert "source_clusters" in capsys.readouterr().err


def test_load_config_exits_on_non_integer_brokers_per_cluster(tmp_path, capsys):
    from sn_confluent.link.main import load_config
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id      = env-abc123
        cluster_id          = lkc-abc123
        link_name           = test-link
        source_bootstrap    = broker.example.com:9093
        brokers_per_cluster = two
    """)
    path = write_config(tmp_path, cfg_text)
    with pytest.raises(SystemExit) as exc_info:
        load_config(path)
    assert exc_info.value.code == 1
    assert "brokers_per_cluster" in capsys.readouterr().err


def test_load_config_exits_if_neither_source_key(tmp_path):
    from sn_confluent.link.main import load_config
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
    from sn_confluent.link.main import check_confluent_cli
    with patch("shutil.which", return_value="/usr/local/bin/confluent"):
        check_confluent_cli()  # Should not raise


def test_check_confluent_cli_exits_when_missing(capsys):
    from sn_confluent.link.main import check_confluent_cli
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
    from sn_confluent.link.main import check_auth
    mock_result = MagicMock(returncode=0)
    with patch("subprocess.run", return_value=mock_result):
        check_auth("env-abc123", "lkc-abc123")  # Should not raise


def test_check_auth_exits_when_cluster_describe_fails(capsys):
    from sn_confluent.link.main import check_auth
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
    from sn_confluent.link.main import load_pem_files
    (tmp_path / "ca.pem").write_bytes(b"CA")
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")
    ca, cert, key = load_pem_files(str(tmp_path))
    assert ca == b"CA"
    assert cert == b"CERT"
    assert key == b"KEY"


def test_load_pem_files_exits_if_ca_missing(tmp_path, capsys):
    from sn_confluent.link.main import load_pem_files
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")
    with pytest.raises(SystemExit) as exc:
        load_pem_files(str(tmp_path))
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "sn-confluent extract" in captured.err


# ---------------------------------------------------------------------------
# build_ssl_properties
# ---------------------------------------------------------------------------

def test_build_ssl_properties_contains_required_keys():
    from sn_confluent.link.main import build_ssl_properties
    result = build_ssl_properties(b"CA-PEM", b"CERT-PEM", b"KEY-PEM")
    assert "ssl.truststore.type=PEM" in result
    assert "ssl.keystore.type=PEM" in result
    assert "ssl.truststore.certificates=" in result
    assert "ssl.keystore.certificate.chain=" in result
    assert "ssl.keystore.key=" in result


def test_build_ssl_properties_escapes_newlines():
    from sn_confluent.link.main import build_ssl_properties
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
    from sn_confluent.link.main import build_ssl_properties
    result = build_ssl_properties(b"LINE1\nLINE2", b"L1\nL2", b"K1\nK2", literal_newlines=True)
    assert "\\n" not in result
    assert "LINE1\nLINE2" in result


def test_build_ssl_properties_includes_key_password():
    from sn_confluent.link.main import build_ssl_properties
    result = build_ssl_properties(b"CA", b"CERT", b"KEY", key_password="secretpw")
    assert "ssl.key.password=secretpw" in result


def test_build_ssl_properties_omits_key_password_when_not_set():
    from sn_confluent.link.main import build_ssl_properties
    result = build_ssl_properties(b"CA", b"CERT", b"KEY")
    assert "ssl.key.password" not in result




# ---------------------------------------------------------------------------
# build_link_command
# ---------------------------------------------------------------------------

def test_build_link_command_returns_correct_args():
    from sn_confluent.link.main import build_link_command
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
    from sn_confluent.link.main import run_link_create
    cmd = ["confluent", "kafka", "link", "create", "test-link"]
    with patch("subprocess.run") as mock_run:
        run_link_create(cmd, dry_run=True)
        mock_run.assert_not_called()
    captured = capsys.readouterr()
    assert "confluent" in captured.out
    assert "test-link" in captured.out


def test_run_link_create_calls_subprocess():
    from sn_confluent.link.main import run_link_create
    cmd = ["confluent", "kafka", "link", "create", "test-link"]
    mock_result = MagicMock(returncode=0, stdout="Created link test-link")
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        run_link_create(cmd, dry_run=False)
        mock_run.assert_called_once_with(cmd, capture_output=True, text=True)


def test_run_link_create_exits_on_cli_failure(capsys):
    from sn_confluent.link.main import run_link_create
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
    from sn_confluent.link.main import copy_security_config
    mock_pyperclip = MagicMock()
    with patch.dict("sys.modules", {"pyperclip": mock_pyperclip}):
        copy_security_config("ssl.truststore.type=PEM\n")
    mock_pyperclip.copy.assert_called_once_with("ssl.truststore.type=PEM\n")
    assert "clipboard" in capsys.readouterr().out


def test_copy_security_config_fallback_windows(capsys):
    from sn_confluent.link.main import copy_security_config
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
    from sn_confluent.link.main import wait_for_link_active
    with patch("subprocess.run", return_value=_describe_result("ACTIVE")):
        wait_for_link_active(CFG, timeout=10)
    assert "ACTIVE" in capsys.readouterr().out


def test_wait_for_link_active_exits_on_failed(capsys):
    from sn_confluent.link.main import wait_for_link_active
    with patch("subprocess.run", return_value=_describe_result("FAILED")):
        with pytest.raises(SystemExit) as exc:
            wait_for_link_active(CFG, timeout=10)
    assert exc.value.code == 1
    assert "FAILED" in capsys.readouterr().err


def test_wait_for_link_active_retries_then_succeeds(capsys):
    from sn_confluent.link.main import wait_for_link_active
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
    from sn_confluent.link.main import wait_for_link_active
    with patch("subprocess.run", return_value=_describe_result("PENDING")):
        with patch("time.monotonic", side_effect=[0, 0, 999]):
            with patch("time.sleep"):
                with pytest.raises(SystemExit) as exc:
                    wait_for_link_active(CFG, timeout=10)
    assert exc.value.code == 1


def test_wait_for_link_active_handles_describe_failure(capsys):
    from sn_confluent.link.main import wait_for_link_active
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
    from sn_confluent.link.main import build_link_properties
    props = build_link_properties(4100)
    assert "cluster.link.prefix=4100." in props

def test_dual_link_properties_include_4200_prefix():
    from sn_confluent.link.main import build_link_properties
    props = build_link_properties(4200)
    assert "cluster.link.prefix=4200." in props


# ---------------------------------------------------------------------------
# single-cluster vs dual-cluster link expansion (regression for issue #14)
# ---------------------------------------------------------------------------

def test_single_cluster_mode_does_not_crash_or_add_prefix(tmp_path, capsys):
    """source_bootstrap mode must not try to extract a port from link_name."""
    from sn_confluent.link.main import main
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id   = env-abc123
        cluster_id       = lkc-abc123
        link_name        = my-link
        source_bootstrap = broker.example.com:9093
    """)
    (tmp_path / "link.conf").write_text(cfg_text)
    (tmp_path / "ca.pem").write_bytes(b"CA")
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")

    mock_run = MagicMock(returncode=0, stdout="", stderr="")
    with patch("sys.argv", [
        "create_link.py",
        "--config", str(tmp_path / "link.conf"),
        "--pem-dir", str(tmp_path),
        "--dry-run",
    ]):
        with patch("sn_confluent.link.main._resolve_key_password", return_value=None):
            with patch("sn_confluent.link.main.check_confluent_cli"):
                with patch("sn_confluent.link.main.check_auth"):
                    with patch("subprocess.run", return_value=mock_run):
                        main()  # must not raise ValueError / IndexError

    out = capsys.readouterr().out
    assert "cluster.link.prefix" not in out


def test_dual_cluster_mode_adds_port_prefix(tmp_path, capsys):
    """source_host mode must add cluster.link.prefix for each port."""
    from sn_confluent.link.main import main
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id = env-abc123
        cluster_id     = lkc-abc123
        link_name      = my-link
        source_host    = kafka.example.com
        source_clusters = 4100, 4200
    """)
    (tmp_path / "link.conf").write_text(cfg_text)
    (tmp_path / "ca.pem").write_bytes(b"CA")
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")

    with patch("sys.argv", [
        "create_link.py",
        "--config", str(tmp_path / "link.conf"),
        "--pem-dir", str(tmp_path),
        "--dry-run",
    ]):
        with patch("sn_confluent.link.main._resolve_key_password", return_value=None):
            with patch("sn_confluent.link.main.check_confluent_cli"):
                with patch("sn_confluent.link.main.check_auth"):
                    main()  # must not raise

    out = capsys.readouterr().out
    assert "4100" in out
    assert "4200" in out


# ---------------------------------------------------------------------------
# _resolve_key_password
# ---------------------------------------------------------------------------

def test_resolve_key_password_uses_env_var(monkeypatch):
    from sn_confluent.link.main import _resolve_key_password
    monkeypatch.setenv("KEY_PASSWORD", "env-secret")
    assert _resolve_key_password() == "env-secret"


def test_resolve_key_password_env_var_empty_returns_none(monkeypatch):
    from sn_confluent.link.main import _resolve_key_password
    monkeypatch.setenv("KEY_PASSWORD", "")
    assert _resolve_key_password() is None


def test_resolve_key_password_prompts_when_env_unset(monkeypatch):
    from sn_confluent.link.main import _resolve_key_password
    monkeypatch.delenv("KEY_PASSWORD", raising=False)
    monkeypatch.setattr("getpass.getpass", lambda _prompt: "typed-secret")
    assert _resolve_key_password() == "typed-secret"


def test_resolve_key_password_prompt_empty_returns_none(monkeypatch):
    from sn_confluent.link.main import _resolve_key_password
    monkeypatch.delenv("KEY_PASSWORD", raising=False)
    monkeypatch.setattr("getpass.getpass", lambda _prompt: "")
    assert _resolve_key_password() is None


def test_resolve_key_password_env_var_skips_prompt(monkeypatch):
    from sn_confluent.link.main import _resolve_key_password
    monkeypatch.setenv("KEY_PASSWORD", "env-secret")
    monkeypatch.setattr("getpass.getpass", lambda _prompt: (_ for _ in ()).throw(AssertionError("getpass must not be called when env var is set")))
    assert _resolve_key_password() == "env-secret"


# ---------------------------------------------------------------------------
# warn_no_key_password
# ---------------------------------------------------------------------------

def test_warn_no_key_password_prints_to_stderr(capsys):
    from sn_confluent.link.main import warn_no_key_password
    warn_no_key_password()
    err = capsys.readouterr().err
    assert "SECURITY WARNING" in err
    assert "SHALL" in err
    assert "production" in err


def test_main_warns_when_no_key_password(tmp_path, capsys):
    from sn_confluent.link.main import main
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id   = env-abc123
        cluster_id       = lkc-abc123
        link_name        = my-link
        source_bootstrap = broker.example.com:9093
    """)
    (tmp_path / "link.conf").write_text(cfg_text)
    (tmp_path / "ca.pem").write_bytes(b"CA")
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")

    with patch("sys.argv", [
        "create_link.py",
        "--config", str(tmp_path / "link.conf"),
        "--pem-dir", str(tmp_path),
        "--dry-run",
    ]):
        with patch("sn_confluent.link.main._resolve_key_password", return_value=None):
            with patch("sn_confluent.link.main.check_confluent_cli"):
                with patch("sn_confluent.link.main.check_auth"):
                    main()

    err = capsys.readouterr().err
    assert "SECURITY WARNING" in err
    assert "SHALL" in err


def test_main_no_warn_when_key_password_set(tmp_path, capsys):
    from sn_confluent.link.main import main
    cfg_text = textwrap.dedent("""\
        [confluent]
        environment_id   = env-abc123
        cluster_id       = lkc-abc123
        link_name        = my-link
        source_bootstrap = broker.example.com:9093
    """)
    (tmp_path / "link.conf").write_text(cfg_text)
    (tmp_path / "ca.pem").write_bytes(b"CA")
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")

    with patch("sys.argv", [
        "create_link.py",
        "--config", str(tmp_path / "link.conf"),
        "--pem-dir", str(tmp_path),
        "--dry-run",
    ]):
        with patch("sn_confluent.link.main._resolve_key_password", return_value="s3cr3t"):
            with patch("sn_confluent.link.main.check_confluent_cli"):
                with patch("sn_confluent.link.main.check_auth"):
                    main()

    err = capsys.readouterr().err
    assert "SECURITY WARNING" not in err


# ---------------------------------------------------------------------------
# --profile flag
# ---------------------------------------------------------------------------

PROFILE_CONFIG = textwrap.dedent("""\
    [DEFAULT]
    environment_id   = env-prod
    cluster_id       = lkc-prod
    link_name        = sn-prod
    source_bootstrap = broker-prod.example.com:9093

    [dev]
    environment_id   = env-dev
    cluster_id       = lkc-dev
    link_name        = sn-dev
    source_bootstrap = broker-dev.example.com:9093
""")


def test_load_config_profile_reads_named_section(tmp_path):
    from sn_confluent.link.main import load_config
    path = write_config(tmp_path, PROFILE_CONFIG)
    cfg = load_config(path, profile="dev")
    assert cfg["environment_id"] == "env-dev"
    assert cfg["cluster_id"] == "lkc-dev"


def test_load_config_profile_inherits_default_keys(tmp_path):
    from sn_confluent.link.main import load_config
    partial_profile = textwrap.dedent("""\
        [DEFAULT]
        environment_id   = env-prod
        cluster_id       = lkc-prod
        link_name        = sn-prod
        source_bootstrap = broker-prod.example.com:9093

        [dev]
        environment_id   = env-dev
    """)
    path = write_config(tmp_path, partial_profile)
    cfg = load_config(path, profile="dev")
    assert cfg["environment_id"] == "env-dev"
    assert cfg["cluster_id"] == "lkc-prod"   # inherited from DEFAULT
    assert cfg["link_name"] == "sn-prod"      # inherited from DEFAULT


def test_load_config_profile_not_found_exits(tmp_path, capsys):
    from sn_confluent.link.main import load_config
    path = write_config(tmp_path, PROFILE_CONFIG)
    with pytest.raises(SystemExit) as exc:
        load_config(path, profile="staging")
    assert exc.value.code == 1
    assert "staging" in capsys.readouterr().err


def test_main_profile_flag_passed_through(tmp_path, capsys):
    from sn_confluent.link.main import main
    (tmp_path / "link.conf").write_text(PROFILE_CONFIG)
    (tmp_path / "ca.pem").write_bytes(b"CA")
    (tmp_path / "client-cert.pem").write_bytes(b"CERT")
    (tmp_path / "client-key.pem").write_bytes(b"KEY")

    with patch("sys.argv", [
        "create_link.py",
        "--config", str(tmp_path / "link.conf"),
        "--profile", "dev",
        "--pem-dir", str(tmp_path),
        "--dry-run",
    ]):
        with patch("sn_confluent.link.main._resolve_key_password", return_value="s3cr3t"):
            with patch("sn_confluent.link.main.check_confluent_cli"):
                with patch("sn_confluent.link.main.check_auth"):
                    rc = main()
    assert rc == 0
    out = capsys.readouterr().out
    assert "sn-dev" in out
