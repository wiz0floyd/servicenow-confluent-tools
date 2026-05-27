"""Tests for sn_confluent.deploy.main."""

import base64
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import textwrap

from sn_confluent.deploy import main as dm
from sn_confluent.core.hermes import HermesClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_CONF = """\
[confluent]
environment_id = env-abc
cluster_id = lkc-123
connector_name = hermes-sink-test
topics = my-topic
instance_name = myinstance
hermes_topic = snc.myinstance.sn_streamconnect.my-topic
keystore_b64 = AAAA
keystore_password = secret1
truststore_b64 = BBBB
truststore_password = secret2
kafka_api_key = MYKEY
kafka_api_secret = MYSECRET
plugin_id = ccp-abc123
"""


def _write_conf(tmp_dir: str, text: str = MINIMAL_CONF) -> str:
    path = os.path.join(tmp_dir, "deploy.conf")
    with open(path, "w") as fh:
        fh.write(text)
    return path


@pytest.fixture
def tmp(tmp_path):
    yield str(tmp_path)


# ---------------------------------------------------------------------------
# find_plugin_file
# ---------------------------------------------------------------------------

def test_find_plugin_file_explicit_exists(tmp):
    zip_path = os.path.join(tmp, "connector.zip")
    open(zip_path, "w").close()
    assert dm.find_plugin_file(zip_path) == zip_path


def test_find_plugin_file_explicit_missing(tmp):
    assert dm.find_plugin_file("/does/not/exist.zip") is None


def test_find_plugin_file_none_no_match():
    with patch("sn_confluent.deploy.main._glob.glob", return_value=[]):
        assert dm.find_plugin_file(None) is None


def test_find_plugin_file_none_auto_detects(tmp):
    zip_path = os.path.join(tmp, "servicenow-hermes-kafka-connector-0.1.0.zip")
    open(zip_path, "w").close()
    with patch("sn_confluent.deploy.main._glob.glob", return_value=[zip_path]):
        assert dm.find_plugin_file(None) == zip_path


# ---------------------------------------------------------------------------
# encode_p12_file
# ---------------------------------------------------------------------------

def test_encode_p12_file_roundtrip(tmp):
    raw = b"\x00\x01\x02\x03binary"
    p12_path = os.path.join(tmp, "test.p12")
    with open(p12_path, "wb") as fh:
        fh.write(raw)
    result = dm.encode_p12_file(p12_path)
    assert base64.b64decode(result) == raw


def test_encode_p12_file_missing_exits(tmp):
    with pytest.raises(SystemExit):
        dm.encode_p12_file(os.path.join(tmp, "nope.p12"))


# ---------------------------------------------------------------------------
# resolve_keystore
# ---------------------------------------------------------------------------

def test_resolve_keystore_from_b64():
    cfg = {"keystore_b64": "ENCODED==", "keystore_path": ""}
    assert dm.resolve_keystore(cfg, "keystore_b64", "keystore_path") == "ENCODED=="


def test_resolve_keystore_from_path(tmp):
    raw = b"pkcs12data"
    p12 = os.path.join(tmp, "ks.p12")
    with open(p12, "wb") as fh:
        fh.write(raw)
    cfg = {"keystore_b64": "", "keystore_path": p12}
    result = dm.resolve_keystore(cfg, "keystore_b64", "keystore_path")
    assert base64.b64decode(result) == raw


def test_resolve_keystore_empty():
    cfg = {}
    assert dm.resolve_keystore(cfg, "keystore_b64", "keystore_path") == ""


# ---------------------------------------------------------------------------
# build_connector_config
# ---------------------------------------------------------------------------

def test_build_connector_config_required_keys():
    cfg = dm.build_sink_config(
        plugin_id="ccp-xyz",
        connector_name="hermes-sink",
        topics="topic-a",
        api_key="KEY",
        api_secret="SECRET",
        instance_name="myinstance",
        hermes_topic="snc.myinstance.sn_streamconnect.t",
        keystore_b64="KS==",
        keystore_password="kspw",
        truststore_b64="TS==",
        truststore_password="tspw",
    )
    assert cfg["connector.class"] == dm.SINK_CONNECTOR_CLASS
    assert cfg["confluent.custom.plugin.id"] == "ccp-xyz"
    assert cfg["confluent.connector.type"] == "CUSTOM"
    assert cfg["kafka.auth.mode"] == "KAFKA_API_KEY"
    assert cfg["hermes.instance.name"] == "myinstance"
    assert cfg["hermes.ssl.keystore.b64"] == "KS=="
    assert cfg["hermes.ssl.truststore.b64"] == "TS=="
    assert cfg["tasks.max"] == "1"


def test_build_connector_config_tasks_max():
    cfg = dm.build_sink_config(
        "ccp-1", "n", "t", "k", "s", "i", "h", "ks", "kp", "ts", "tp", tasks_max=4
    )
    assert cfg["tasks.max"] == "4"


def test_build_sink_config_endpoints_format():
    cfg = dm.build_sink_config(
        "ccp-1", "n", "t", "k", "s", "myinstance", "h", "ks", "kp", "ts", "tp"
    )
    assert cfg["confluent.custom.connection.endpoints"] == (
        "myinstance.service-now.com:4000,4001,4002,4003,4004,4005,4006,4007"
    )


def test_build_sink_config_endpoints_fqdn():
    cfg = dm.build_sink_config(
        "ccp-1", "n", "t", "k", "s", "myinstance.service-now.com", "h", "ks", "kp", "ts", "tp"
    )
    ep = cfg["confluent.custom.connection.endpoints"]
    assert ep.startswith("myinstance.service-now.com:")
    assert "service-now.com.service-now.com" not in ep


def test_build_source_config_endpoints_format():
    from sn_confluent.core.pem import SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER
    cfg = dm.build_source_config(
        "ccp-1", "n", "dest", "k", "s", "myinstance", "src", "ks", "kp", "ts", "tp"
    )
    ep = cfg["confluent.custom.connection.endpoints"]
    assert ep.startswith("myinstance.service-now.com:")
    expected_ports = ",".join(
        str(base + i) for base in SN_SOURCE_CLUSTERS for i in range(SN_BROKERS_PER_CLUSTER)
    )
    assert ep == "myinstance.service-now.com:" + expected_ports


def test_build_source_config_endpoints_override():
    cfg = dm.build_source_config(
        "ccp-1", "n", "dest", "k", "s", "myinstance", "src", "ks", "kp", "ts", "tp",
        endpoints="custom.host:9000,9001",
    )
    assert cfg["confluent.custom.connection.endpoints"] == "custom.host:9000,9001"


# ---------------------------------------------------------------------------
# validate_keystores
# ---------------------------------------------------------------------------

def test_validate_keystores_success(capsys):
    p12_b64 = _make_pkcs12_b64()
    result = dm.validate_keystores(p12_b64, "password", p12_b64, "password")
    assert result is True
    out = capsys.readouterr().out
    assert "expires" in out


def test_validate_keystores_wrong_password(capsys):
    p12_b64 = _make_pkcs12_b64()
    result = dm.validate_keystores(p12_b64, "wrongpw", p12_b64, "wrongpw")
    assert result is False
    err = capsys.readouterr().err
    assert "wrong password" in err


def test_validate_keystores_bad_b64(capsys):
    result = dm.validate_keystores("notbase64!!!", "pw", "notbase64!!!", "pw")
    assert result is False


# ---------------------------------------------------------------------------
# list_cc_topics
# ---------------------------------------------------------------------------

def test_list_cc_topics_success():
    payload = json.dumps([{"name": "topic-a"}, {"name": "topic-b"}])
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        result = dm.list_cc_topics("env-1", "lkc-1")
    assert result == ["topic-a", "topic-b"]


def test_list_cc_topics_cli_failure_returns_none():
    with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="bad")):
        assert dm.list_cc_topics("env-1", "lkc-1") is None


def test_list_cc_topics_bad_json_returns_none():
    with patch("subprocess.run", return_value=_make_proc(stdout="not-json")):
        assert dm.list_cc_topics("env-1", "lkc-1") is None


# ---------------------------------------------------------------------------
# HermesClient.list_topics
# ---------------------------------------------------------------------------

def _make_pkcs12_b64():
    """Generate a minimal self-signed PKCS12 for testing."""
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    import datetime

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u"test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    p12_bytes = pkcs12.serialize_key_and_certificates(
        b"test", key, cert, None,
        serialization.BestAvailableEncryption(b"password"),
    )
    return base64.b64encode(p12_bytes).decode("ascii")


def test_hermes_client_list_topics_success():
    p12_b64 = _make_pkcs12_b64()
    ca_pem, cert_pem, key_pem = dm._extract_pem_from_p12(p12_b64, "password", p12_b64, "password")
    mock_admin = MagicMock()
    mock_admin.list_topics.return_value = ["snc.inst.sn_streamconnect.t1", "__consumer_offsets"]
    with patch("kafka.admin.KafkaAdminClient", return_value=mock_admin):
        result = HermesClient("inst", ca_pem, cert_pem, key_pem).list_topics()
    assert result == ["snc.inst.sn_streamconnect.t1"]
    assert "__consumer_offsets" not in result


def test_hermes_client_list_topics_connection_error_returns_none():
    p12_b64 = _make_pkcs12_b64()
    ca_pem, cert_pem, key_pem = dm._extract_pem_from_p12(p12_b64, "password", p12_b64, "password")
    with patch("kafka.admin.KafkaAdminClient", side_effect=Exception("timeout")):
        result = HermesClient("inst", ca_pem, cert_pem, key_pem).list_topics()
    assert result is None


# ---------------------------------------------------------------------------
# _resolve_hermes_pem — pem_dir path
# ---------------------------------------------------------------------------

def test_resolve_hermes_pem_from_pem_dir(tmp):
    p12_b64 = _make_pkcs12_b64()
    ca_pem, cert_pem, key_pem = dm._extract_pem_from_p12(p12_b64, "password", p12_b64, "password")
    # Write PEM files to tmp dir
    for name, data in [("ca.pem", ca_pem), ("client-cert.pem", cert_pem), ("client-key.pem", key_pem)]:
        with open(os.path.join(tmp, name), "wb") as fh:
            fh.write(data)
    result = dm._resolve_hermes_pem({}, tmp)
    assert result is not None
    assert b"BEGIN CERTIFICATE" in result[0]


def test_resolve_hermes_pem_pem_dir_missing_files_returns_none(tmp):
    result = dm._resolve_hermes_pem({}, tmp)
    assert result is None


def test_resolve_hermes_pem_from_p12(tmp):
    p12_b64 = _make_pkcs12_b64()
    cfg = {
        "keystore_b64": p12_b64, "keystore_password": "password",
        "truststore_b64": p12_b64, "truststore_password": "password",
    }
    result = dm._resolve_hermes_pem(cfg, None)
    assert result is not None
    assert b"BEGIN CERTIFICATE" in result[0]


def test_resolve_hermes_pem_no_source_returns_none():
    assert dm._resolve_hermes_pem({}, None) is None


# ---------------------------------------------------------------------------
# _extract_pem_from_p12
# ---------------------------------------------------------------------------

def test_extract_pem_from_p12_roundtrip():
    p12_b64 = _make_pkcs12_b64()
    ca_pem, cert_pem, key_pem = dm._extract_pem_from_p12(p12_b64, "password", p12_b64, "password")
    assert b"BEGIN CERTIFICATE" in ca_pem
    assert b"BEGIN CERTIFICATE" in cert_pem
    assert b"BEGIN PRIVATE KEY" in key_pem


# ---------------------------------------------------------------------------
# upload_plugin
# ---------------------------------------------------------------------------

def _make_proc(returncode=0, stdout="", stderr=""):
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    m.stderr = stderr
    return m


def test_upload_plugin_success():
    payload = json.dumps({"id": "ccp-newplugin", "name": "test-plugin"})
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        pid = dm.upload_plugin("/fake/connector.zip", "test-plugin", "aws", "sink", dm.SINK_CONNECTOR_CLASS)
    assert pid == "ccp-newplugin"


def test_upload_plugin_failure_exits():
    with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="auth error")):
        with pytest.raises(SystemExit):
            dm.upload_plugin("/fake/connector.zip", "test-plugin", "aws", "sink", dm.SINK_CONNECTOR_CLASS)


def test_upload_plugin_bad_json_exits():
    with patch("subprocess.run", return_value=_make_proc(stdout="not-json")):
        with pytest.raises(SystemExit):
            dm.upload_plugin("/fake/connector.zip", "test-plugin", "aws", "sink", dm.SINK_CONNECTOR_CLASS)


def test_upload_plugin_missing_id_exits():
    with patch("subprocess.run", return_value=_make_proc(stdout=json.dumps({"name": "p"}))):
        with pytest.raises(SystemExit):
            dm.upload_plugin("/fake/connector.zip", "test-plugin", "aws", "sink", dm.SINK_CONNECTOR_CLASS)


# ---------------------------------------------------------------------------
# create_connector
# ---------------------------------------------------------------------------

def test_create_connector_returns_id():
    payload = json.dumps({"id": "lcc-abc", "name": "hermes-sink-test"})
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        conn_id = dm.create_connector("env-1", "lkc-1", {"name": "hermes-sink-test"})
    assert conn_id == "lcc-abc"


def test_create_connector_fallback_to_name_on_bad_json():
    with patch("subprocess.run", return_value=_make_proc(stdout="not-json")):
        conn_id = dm.create_connector("env-1", "lkc-1", {"name": "my-connector"})
    assert conn_id == "my-connector"


def test_create_connector_cli_failure_exits():
    with patch("subprocess.run", return_value=_make_proc(returncode=1, stderr="bad")):
        with pytest.raises(SystemExit):
            dm.create_connector("env-1", "lkc-1", {"name": "x"})


# ---------------------------------------------------------------------------
# poll_connector
# ---------------------------------------------------------------------------

def test_poll_connector_running():
    payload = json.dumps({"status": {"state": "RUNNING"}})
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        with patch("time.sleep"):
            dm.poll_connector("env-1", "lkc-1", "lcc-1", timeout=10)


def test_poll_connector_failed_exits():
    payload = json.dumps({"status": {"state": "FAILED"}})
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        with patch("time.sleep"):
            with pytest.raises(SystemExit):
                dm.poll_connector("env-1", "lkc-1", "lcc-1", timeout=10)


def test_poll_connector_timeout_exits():
    payload = json.dumps({"status": {"state": "PROVISIONING"}})
    with patch("subprocess.run", return_value=_make_proc(stdout=payload)):
        with patch("time.sleep"):
            with patch("time.monotonic", side_effect=[0, 0, 999]):
                with pytest.raises(SystemExit):
                    dm.poll_connector("env-1", "lkc-1", "lcc-1", timeout=1, interval=0)


# ---------------------------------------------------------------------------
# main — dry-run end-to-end
# ---------------------------------------------------------------------------

def test_main_dry_run(tmp, capsys):
    conf = _write_conf(tmp)
    with patch("sn_confluent.deploy.main.ensure_authenticated"):
        with patch("sn_confluent.deploy.main._glob.glob", return_value=[]):
            with patch("sn_confluent.deploy.main.validate_keystores", return_value=True):
                rc = dm.main(["sink", "--config", conf, "--dry-run", "--no-wizard"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "dry-run" in out
    assert "hermes-sink-test" in out
    assert "MYKEY" in out          # api key is not masked
    assert "MYSECRET" not in out   # api secret is masked


def test_main_missing_plugin_no_wizard_exits(tmp):
    conf = _write_conf(tmp, MINIMAL_CONF.replace("plugin_id = ccp-abc123", "plugin_id ="))
    with patch("sn_confluent.deploy.main.ensure_authenticated"):
        with patch("sn_confluent.deploy.main._glob.glob", return_value=[]):
            with pytest.raises(SystemExit):
                dm.main(["sink", "--config", conf, "--no-wizard"])


def test_main_missing_topics_no_wizard_exits(tmp):
    conf_text = MINIMAL_CONF.replace("topics = my-topic", "topics =")
    conf = _write_conf(tmp, conf_text)
    with patch("sn_confluent.deploy.main.ensure_authenticated"):
        with pytest.raises(SystemExit):
            dm.main(["sink", "--config", conf, "--no-wizard"])


# ---------------------------------------------------------------------------
# resolve_api_credentials — auto-generation
# ---------------------------------------------------------------------------

def test_resolve_api_credentials_from_env(monkeypatch):
    monkeypatch.setenv("CC_API_KEY", "ENVKEY")
    monkeypatch.setenv("CC_API_SECRET", "ENVSECRET")
    key, secret = dm.resolve_api_credentials({}, "env-1", "lkc-1")
    assert key == "ENVKEY"
    assert secret == "ENVSECRET"


def test_resolve_api_credentials_from_config(monkeypatch):
    monkeypatch.delenv("CC_API_KEY", raising=False)
    monkeypatch.delenv("CC_API_SECRET", raising=False)
    cfg = {"kafka_api_key": "CFGKEY", "kafka_api_secret": "CFGSECRET"}
    key, secret = dm.resolve_api_credentials(cfg, "env-1", "lkc-1")
    assert key == "CFGKEY"
    assert secret == "CFGSECRET"


def test_resolve_api_credentials_auto_generates(monkeypatch, capsys):
    monkeypatch.delenv("CC_API_KEY", raising=False)
    monkeypatch.delenv("CC_API_SECRET", raising=False)
    with patch("sn_confluent.api_key.main.create_kafka_api_key", return_value=("NEWKEY", "NEWSECRET")) as mock_create:
        key, secret = dm.resolve_api_credentials({}, "env-1", "lkc-1")
    assert key == "NEWKEY"
    assert secret == "NEWSECRET"
    mock_create.assert_called_once_with(
        environment_id="env-1",
        cluster_id="lkc-1",
        description="sn-confluent deploy (auto-generated)",
    )
    out = capsys.readouterr().out
    assert "NEWKEY" in out


def test_resolve_api_credentials_no_cluster_id_prompts(monkeypatch):
    monkeypatch.delenv("CC_API_KEY", raising=False)
    monkeypatch.delenv("CC_API_SECRET", raising=False)
    with patch("builtins.input", return_value="PROMPTKEY"):
        with patch("getpass.getpass", return_value="PROMPTSECRET"):
            key, secret = dm.resolve_api_credentials({})
    assert key == "PROMPTKEY"
    assert secret == "PROMPTSECRET"


# ---------------------------------------------------------------------------
# --profile flag
# ---------------------------------------------------------------------------

DEPLOY_PROFILE_CONF = textwrap.dedent("""\
    [DEFAULT]
    environment_id = env-prod
    cluster_id = lkc-prod
    connector_name = hermes-sink-prod
    topics = prod-topic
    instance_name = myinstance
    hermes_topic = snc.myinstance.sn_streamconnect.prod
    keystore_b64 = AAAA
    keystore_password = pw1
    truststore_b64 = BBBB
    truststore_password = pw2
    kafka_api_key = PRODKEY
    kafka_api_secret = PRODSECRET
    plugin_id = ccp-prod

    [dev]
    environment_id = env-dev
    cluster_id = lkc-dev
    connector_name = hermes-sink-dev
    plugin_id = ccp-dev
""")


def test_load_config_profile_reads_dev_section(tmp_path):
    path = _write_conf(str(tmp_path), DEPLOY_PROFILE_CONF)
    cfg = dm.load_config(path, profile="dev")
    assert cfg["environment_id"] == "env-dev"
    assert cfg["cluster_id"] == "lkc-dev"
    assert cfg["plugin_id"] == "ccp-dev"


def test_load_config_profile_inherits_default_keys(tmp_path):
    path = _write_conf(str(tmp_path), DEPLOY_PROFILE_CONF)
    cfg = dm.load_config(path, profile="dev")
    assert cfg["connector_name"] == "hermes-sink-dev"   # overridden in [dev]
    assert cfg["topics"] == "prod-topic"                # inherited from DEFAULT


def test_load_config_profile_not_found_exits(tmp_path, capsys):
    path = _write_conf(str(tmp_path), DEPLOY_PROFILE_CONF)
    with pytest.raises(SystemExit) as exc:
        dm.load_config(path, profile="staging")
    assert exc.value.code == 1
    assert "staging" in capsys.readouterr().err


def test_sink_main_profile_flag_loads_correct_section(tmp_path, capsys):
    path = _write_conf(str(tmp_path), DEPLOY_PROFILE_CONF)
    with patch("sn_confluent.deploy.main.ensure_authenticated"):
        with patch("sn_confluent.deploy.main.validate_keystores", return_value=True):
            with patch("sn_confluent.deploy.main.resolve_api_credentials", return_value=("K", "S")):
                rc = dm._sink_main([
                    "--config", path,
                    "--profile", "dev",
                    "--dry-run",
                ])
    assert rc == 0
    out = capsys.readouterr().out
    # env-dev and lkc-dev come from the --environment/--cluster flags in the dry-run command
    assert "env-dev" in out
    assert "lkc-dev" in out
    # connector_name appears in the dry-run JSON body
    assert "hermes-sink-dev" in out

def test_source_main_profile_flag_loads_correct_section(tmp_path, capsys):
    path = _write_conf(str(tmp_path), DEPLOY_PROFILE_CONF)
    with patch("sn_confluent.deploy.main.ensure_authenticated"):
        with patch("sn_confluent.deploy.main.validate_keystores", return_value=True):
            with patch("sn_confluent.deploy.main.resolve_api_credentials", return_value=("K", "S")):
                rc = dm._source_main([
                    "--config", path,
                    "--profile", "dev",
                    "--dry-run",
                ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "env-dev" in out
    assert "lkc-dev" in out
    assert "hermes-sink-dev" in out
