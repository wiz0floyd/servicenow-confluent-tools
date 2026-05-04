"""Tests for setup_replicator.py"""

import json
import os
import sys
import tempfile
import time
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

# Ensure the parent package is importable.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import setup_replicator as sr

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FAKE_CA = b"-----BEGIN CERTIFICATE-----\nFAKECA\n-----END CERTIFICATE-----"
FAKE_CERT = b"-----BEGIN CERTIFICATE-----\nFAKECERT\n-----END CERTIFICATE-----"
FAKE_KEY = b"-----BEGIN RSA PRIVATE KEY-----\nFAKEKEY\n-----END RSA PRIVATE KEY-----"
FAKE_KEY_ENC = b"-----BEGIN ENCRYPTED PRIVATE KEY-----\nFAKEKEY\n-----END ENCRYPTED PRIVATE KEY-----"

MINIMAL_CONF = """\
[confluent]
environment_id = env-abc
cluster_id = lkc-123
connector_name = my-repl
source_host = broker.example.com
pem_dir = /tmp/pems
"""


def _write_conf(tmp_path, text):
    """Write config text to a temp file and return its path."""
    path = os.path.join(tmp_path, "link.conf")
    with open(path, "w") as f:
        f.write(text)
    return path


# =========================================================================
# TestBuildConnectorConfig
# =========================================================================


class TestBuildConnectorConfig(unittest.TestCase):
    """Covers build_cc_to_sn_config and build_sn_to_cc_configs."""

    def test_cc_to_sn_has_producer_override_keys(self):
        cfg = sr.build_cc_to_sn_config(
            "repl", "host", 4000, 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, None,
            "grp", ".*", "${topic}",
        )
        self.assertIn("producer.override.bootstrap.servers", cfg)
        self.assertIn("producer.override.security.protocol", cfg)
        # Must NOT have src.kafka.* keys
        src_keys = [k for k in cfg if k.startswith("src.kafka.")]
        self.assertEqual(src_keys, [])

    def test_sn_to_cc_has_src_kafka_keys(self):
        configs = sr.build_sn_to_cc_configs(
            "repl", "host", [4100, 4200], 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, None,
            "grp", ".*", "${topic}",
        )
        self.assertTrue(len(configs) >= 1)
        cfg = configs[0]
        self.assertIn("src.kafka.bootstrap.servers", cfg)
        self.assertIn("src.kafka.security.protocol", cfg)
        # Must NOT have producer.override.* keys
        po_keys = [k for k in cfg if k.startswith("producer.override.")]
        self.assertEqual(po_keys, [])

    def test_cc_to_sn_key_password_present(self):
        cfg = sr.build_cc_to_sn_config(
            "repl", "host", 4000, 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, "s3cret",
            "grp", ".*", "${topic}",
        )
        self.assertEqual(cfg["producer.override.ssl.key.password"], "s3cret")

    def test_cc_to_sn_key_password_absent(self):
        cfg = sr.build_cc_to_sn_config(
            "repl", "host", 4000, 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, None,
            "grp", ".*", "${topic}",
        )
        self.assertNotIn("producer.override.ssl.key.password", cfg)

    def test_sn_to_cc_key_password_present(self):
        configs = sr.build_sn_to_cc_configs(
            "repl", "host", [4100], 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, "s3cret",
            "grp", ".*", "${topic}",
        )
        self.assertEqual(configs[0]["src.kafka.ssl.key.password"], "s3cret")

    def test_sn_to_cc_key_password_absent(self):
        configs = sr.build_sn_to_cc_configs(
            "repl", "host", [4100], 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, None,
            "grp", ".*", "${topic}",
        )
        self.assertNotIn("src.kafka.ssl.key.password", configs[0])

    def test_topic_regex_all(self):
        regex = sr.build_topic_regex(None, use_all=True)
        self.assertIn("__", regex)  # negative lookahead for __
        # Verify it is the catch-all pattern
        self.assertEqual(regex, "(?!__)(?!_confluent).+")

    def test_topic_regex_selected(self):
        regex = sr.build_topic_regex(["t1", "t2"], use_all=False)
        self.assertEqual(regex, "^(t1|t2)$")

    def test_topic_regex_dot_escaped(self):
        regex = sr.build_topic_regex(["my.topic"], use_all=False)
        self.assertIn("my\\.topic", regex)

    def test_cc_to_sn_connector_name_plain(self):
        cfg = sr.build_cc_to_sn_config(
            "my-repl", "host", 4000, 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, None,
            "grp", ".*", "${topic}",
        )
        self.assertEqual(cfg["name"], "my-repl")

    def test_sn_to_cc_connector_name_appends_port(self):
        configs = sr.build_sn_to_cc_configs(
            "my-repl", "host", [4100, 4200], 4,
            FAKE_CA, FAKE_CERT, FAKE_KEY, None,
            "grp", ".*", "${topic}",
        )
        self.assertEqual(configs[0]["name"], "my-repl-4100")
        self.assertEqual(configs[1]["name"], "my-repl-4200")


# =========================================================================
# TestLoadConfig
# =========================================================================


class TestLoadConfig(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_direction_defaults_cc_to_sn(self):
        path = _write_conf(self.tmp, MINIMAL_CONF)
        cfg = sr.load_config(path)
        # direction key absent -> caller defaults to cc-to-sn;
        # load_config itself doesn't inject direction, but .get returns None
        self.assertNotIn("direction", cfg)

    def test_group_id_default(self):
        # group_id is resolved in main(), not load_config, so verify
        # that load_config doesn't inject one — main uses the fallback.
        path = _write_conf(self.tmp, MINIMAL_CONF)
        cfg = sr.load_config(path)
        self.assertNotIn("group_id", cfg)

    def test_connect_url_default(self):
        path = _write_conf(self.tmp, MINIMAL_CONF)
        cfg = sr.load_config(path)
        # connect_url defaults in main() via .get
        self.assertNotIn("connect_url", cfg)

    def test_missing_required_key_exits(self):
        bad_conf = "[confluent]\nenvironment_id = env-abc\n"
        path = _write_conf(self.tmp, bad_conf)
        with self.assertRaises(SystemExit) as ctx:
            sr.load_config(path)
        self.assertEqual(ctx.exception.code, 1)

    def test_missing_file_exits(self):
        with self.assertRaises(SystemExit):
            sr.load_config(os.path.join(self.tmp, "nonexistent.conf"))

    def test_topics_override(self):
        conf = MINIMAL_CONF + "topics = alpha,beta\n"
        path = _write_conf(self.tmp, conf)
        cfg = sr.load_config(path)
        self.assertEqual(cfg.get("topics"), "alpha,beta")

    def test_pem_dir_override(self):
        conf = MINIMAL_CONF.replace("pem_dir = /tmp/pems", "pem_dir = /other/pems")
        path = _write_conf(self.tmp, conf)
        cfg = sr.load_config(path)
        self.assertEqual(cfg["pem_dir"], "/other/pems")

    def test_source_clusters_parses_csv(self):
        conf = MINIMAL_CONF + "source_clusters = 5100,5200,5300\n"
        path = _write_conf(self.tmp, conf)
        cfg = sr.load_config(path)
        self.assertEqual(cfg["source_clusters"], [5100, 5200, 5300])

    def test_source_clusters_default(self):
        path = _write_conf(self.tmp, MINIMAL_CONF)
        cfg = sr.load_config(path)
        self.assertEqual(cfg["source_clusters"], list(sr.SN_SOURCE_CLUSTERS))


# =========================================================================
# TestListTopics
# =========================================================================


class TestListTopics(unittest.TestCase):

    @patch("setup_replicator.subprocess.run")
    def test_list_cc_topics(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=json.dumps([
                {"name": "zeta"},
                {"name": "alpha"},
                {"name": "beta"},
            ]),
        )
        result = sr.list_cc_topics("env-1", "lkc-1")
        self.assertEqual(result, ["alpha", "beta", "zeta"])
        mock_run.assert_called_once()

    @patch("setup_replicator.subprocess.run")
    def test_list_cc_topics_failure_exits(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="oops")
        with self.assertRaises(SystemExit):
            sr.list_cc_topics("env-1", "lkc-1")

    @patch("setup_replicator.KafkaAdminClient", create=True)
    def test_list_sn_topics(self, _unused):
        """Mock KafkaAdminClient at the import location inside list_sn_topics."""
        mock_admin_instance = MagicMock()
        mock_admin_instance.list_topics.return_value = [
            "topicB", "__consumer_offsets", "topicA", "_confluent-metrics",
        ]
        mock_admin_cls = MagicMock(return_value=mock_admin_instance)

        with patch.dict("sys.modules", {"kafka": MagicMock(), "kafka.admin": MagicMock(KafkaAdminClient=mock_admin_cls)}):
            result = sr.list_sn_topics(
                "host", [4100], 4,
                FAKE_CA, FAKE_CERT, FAKE_KEY, None,
            )
        self.assertEqual(result, ["topicA", "topicB"])


# =========================================================================
# TestCreateConnector
# =========================================================================


class TestCreateConnector(unittest.TestCase):

    @patch("setup_replicator.urllib.request.urlopen")
    def test_create_connector_success(self, mock_urlopen):
        config = {"name": "repl-1", "connector.class": "Replicator"}
        resp_body = json.dumps({"name": "repl-1"}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = resp_body
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        name = sr.create_connector("http://localhost:8083", config)
        self.assertEqual(name, "repl-1")

        # Verify POST body
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        body = json.loads(req.data.decode())
        self.assertEqual(body["name"], "repl-1")
        self.assertIn("connector.class", body["config"])

    @patch("setup_replicator.urllib.request.urlopen")
    def test_create_connector_http_error_exits(self, mock_urlopen):
        import urllib.error
        error = urllib.error.HTTPError(
            "http://localhost:8083/connectors", 409,
            "Conflict", {}, BytesIO(b"already exists"),
        )
        mock_urlopen.side_effect = error

        with self.assertRaises(SystemExit) as ctx:
            sr.create_connector("http://localhost:8083", {"name": "x"})
        self.assertEqual(ctx.exception.code, 1)


# =========================================================================
# TestPollConnector
# =========================================================================


class TestPollConnector(unittest.TestCase):

    def _mock_urlopen_responses(self, states):
        """Return a side_effect callable that yields status JSON for each state."""
        responses = []
        for state, tasks in states:
            body = json.dumps({
                "connector": {"state": state},
                "tasks": tasks,
            }).encode()
            mock_resp = MagicMock()
            mock_resp.read.return_value = body
            mock_resp.__enter__ = MagicMock(return_value=mock_resp)
            mock_resp.__exit__ = MagicMock(return_value=False)
            responses.append(mock_resp)
        return responses

    @patch("setup_replicator.time.sleep", return_value=None)
    @patch("setup_replicator.urllib.request.urlopen")
    def test_poll_provisioning_then_running(self, mock_urlopen, _sleep):
        mock_urlopen.side_effect = self._mock_urlopen_responses([
            ("PROVISIONING", []),
            ("RUNNING", []),
        ])
        # Should return without error
        sr.poll_connector("http://localhost:8083", "repl-1", timeout=60, interval=1)

    @patch("setup_replicator.time.sleep", return_value=None)
    @patch("setup_replicator.urllib.request.urlopen")
    def test_poll_failed_exits(self, mock_urlopen, _sleep):
        mock_urlopen.side_effect = self._mock_urlopen_responses([
            ("FAILED", [{"id": 0, "trace": "NullPointerException"}]),
        ])
        with self.assertRaises(SystemExit) as ctx:
            sr.poll_connector("http://localhost:8083", "repl-1", timeout=60, interval=1)
        self.assertEqual(ctx.exception.code, 1)

    @patch("setup_replicator.time.sleep", return_value=None)
    @patch("setup_replicator.time.monotonic")
    @patch("setup_replicator.urllib.request.urlopen")
    def test_poll_timeout_exits(self, mock_urlopen, mock_monotonic, _sleep):
        # Simulate time passing beyond the deadline
        mock_monotonic.side_effect = [0, 0, 200]  # start, first check, second check past deadline
        mock_urlopen.side_effect = self._mock_urlopen_responses([
            ("PROVISIONING", []),
        ])
        with self.assertRaises(SystemExit) as ctx:
            sr.poll_connector("http://localhost:8083", "repl-1", timeout=60, interval=1)
        self.assertEqual(ctx.exception.code, 1)


# =========================================================================
# TestCLIFlags
# =========================================================================


class TestCLIFlags(unittest.TestCase):
    """Test CLI argument combinations via main() with mocked dependencies."""

    def _make_pem_dir(self, tmp):
        pem_dir = os.path.join(tmp, "pems")
        os.makedirs(pem_dir, exist_ok=True)
        for name, data in [("ca.pem", FAKE_CA), ("client-cert.pem", FAKE_CERT), ("client-key.pem", FAKE_KEY)]:
            with open(os.path.join(pem_dir, name), "wb") as f:
                f.write(data)
        return pem_dir

    def _write_full_conf(self, tmp, pem_dir, extra=""):
        conf = MINIMAL_CONF.replace("/tmp/pems", pem_dir) + extra
        return _write_conf(tmp, conf)

    @patch("setup_replicator.urllib.request.urlopen")
    @patch("setup_replicator.check_confluent_cli")
    def test_dry_run_all_no_wizard(self, mock_cli_check, mock_urlopen):
        """--dry-run --all --no-wizard should exit 0 with no REST call."""
        with tempfile.TemporaryDirectory() as tmp:
            pem_dir = self._make_pem_dir(tmp)
            conf_path = self._write_full_conf(tmp, pem_dir)
            with patch("sys.argv", [
                "setup_replicator.py",
                "--config", conf_path,
                "--dry-run", "--all", "--no-wizard",
            ]):
                sr.main()
        mock_urlopen.assert_not_called()

    @patch("setup_replicator.urllib.request.urlopen")
    @patch("setup_replicator.check_confluent_cli")
    def test_direction_sn_to_cc_all_produces_two_configs(self, mock_cli_check, mock_urlopen):
        """--direction sn-to-cc --all --no-wizard --dry-run produces two configs (default clusters)."""
        with tempfile.TemporaryDirectory() as tmp:
            pem_dir = self._make_pem_dir(tmp)
            conf_path = self._write_full_conf(tmp, pem_dir)
            with patch("sys.argv", [
                "setup_replicator.py",
                "--config", conf_path,
                "--direction", "sn-to-cc",
                "--dry-run", "--all", "--no-wizard",
            ]):
                # Capture stdout to verify two connector blocks
                import io
                captured = io.StringIO()
                with patch("sys.stdout", captured):
                    sr.main()
                output = captured.getvalue()
        # Two connectors: one per default source cluster (4100, 4200)
        self.assertEqual(output.count("--- Connector:"), 2)
        mock_urlopen.assert_not_called()

    @patch("setup_replicator.urllib.request.urlopen")
    @patch("setup_replicator.check_confluent_cli")
    def test_topics_flag_produces_correct_regex(self, mock_cli_check, mock_urlopen):
        """--topics t1,t2 --no-wizard --dry-run should produce ^(t1|t2)$ regex."""
        with tempfile.TemporaryDirectory() as tmp:
            pem_dir = self._make_pem_dir(tmp)
            conf_path = self._write_full_conf(tmp, pem_dir)
            import io
            captured = io.StringIO()
            with patch("sys.argv", [
                "setup_replicator.py",
                "--config", conf_path,
                "--topics", "t1,t2",
                "--dry-run", "--no-wizard",
            ]):
                with patch("sys.stdout", captured):
                    sr.main()
            output = captured.getvalue()
        self.assertIn("^(t1|t2)$", output)


if __name__ == "__main__":
    unittest.main()
