import json
import textwrap
import pytest
from unittest.mock import patch, MagicMock

VALID_CONFIG = textwrap.dedent("""\
    [confluent]
    environment_id = env-abc123
    cluster_id     = lkc-abc123
    link_name      = servicenow-link
    source_host    = kafka.example.com
    instance_name  = snc.myinstance
""")


def write_config(tmp_path, content=VALID_CONFIG):
    p = tmp_path / "link.conf"
    p.write_text(content)
    return str(p)


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

def test_load_config_returns_dict_with_link_names(tmp_path):
    from sn_confluent.mirror.main import load_config
    cfg = load_config(write_config(tmp_path))
    assert cfg["environment_id"] == "env-abc123"
    assert cfg["link_name_4100"] == "servicenow-link-4100"
    assert cfg["link_name_4200"] == "servicenow-link-4200"


def test_load_config_defaults_source_clusters_and_brokers(tmp_path):
    from sn_confluent.mirror.main import load_config, SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER
    cfg = load_config(write_config(tmp_path))
    assert cfg["source_clusters"] == SN_SOURCE_CLUSTERS
    assert cfg["brokers_per_cluster"] == SN_BROKERS_PER_CLUSTER


def test_load_config_reads_custom_source_clusters(tmp_path):
    from sn_confluent.mirror.main import load_config
    custom = textwrap.dedent("""\
        [confluent]
        environment_id  = env-abc123
        cluster_id      = lkc-abc123
        link_name       = servicenow-link
        source_host     = kafka.example.com
        instance_name   = snc.myinstance
        source_clusters = 5000, 5100
    """)
    cfg = load_config(write_config(tmp_path, custom))
    assert cfg["source_clusters"] == [5000, 5100]
    assert cfg["link_name_5000"] == "servicenow-link-5000"
    assert cfg["link_name_5100"] == "servicenow-link-5100"


def test_load_config_reads_custom_brokers_per_cluster(tmp_path):
    from sn_confluent.mirror.main import load_config
    custom = textwrap.dedent("""\
        [confluent]
        environment_id      = env-abc123
        cluster_id          = lkc-abc123
        link_name           = servicenow-link
        source_host         = kafka.example.com
        instance_name       = snc.myinstance
        brokers_per_cluster = 2
    """)
    cfg = load_config(write_config(tmp_path, custom))
    assert cfg["brokers_per_cluster"] == 2


def test_load_config_exits_if_missing():
    from sn_confluent.mirror.main import load_config
    with pytest.raises(SystemExit) as exc:
        load_config("/nonexistent/link.conf")
    assert exc.value.code == 1


def test_load_config_exits_if_no_source_host(tmp_path):
    from sn_confluent.mirror.main import load_config
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


def test_load_config_exits_if_no_instance_name(tmp_path):
    from sn_confluent.mirror.main import load_config
    bad = textwrap.dedent("""\
        [confluent]
        environment_id = env-abc123
        cluster_id     = lkc-abc123
        link_name      = servicenow-link
        source_host    = kafka.example.com
    """)
    with pytest.raises(SystemExit) as exc:
        load_config(write_config(tmp_path, bad))
    assert exc.value.code == 1


def test_load_config_exposes_instance_name(tmp_path):
    from sn_confluent.mirror.main import load_config
    cfg = load_config(write_config(tmp_path))
    assert cfg["instance_name"] == "snc.myinstance"


# ---------------------------------------------------------------------------
# list_source_topics
# ---------------------------------------------------------------------------

def test_list_source_topics_returns_sorted_list():
    from sn_confluent.mirror.main import list_source_topics
    mock_consumer = MagicMock()
    mock_consumer.topics.return_value = {"zebra", "alpha", "__consumer_offsets"}
    with patch("sn_confluent.mirror.main.KafkaConsumer", return_value=mock_consumer):
        topics = list_source_topics("kafka.example.com", 4100, "ca", "cert", "key")
    assert topics == ["alpha", "zebra"]
    assert "__consumer_offsets" not in topics


def test_list_source_topics_applies_filter():
    from sn_confluent.mirror.main import list_source_topics
    mock_consumer = MagicMock()
    mock_consumer.topics.return_value = {"sn_foo", "sn_bar", "other"}
    with patch("sn_confluent.mirror.main.KafkaConsumer", return_value=mock_consumer):
        topics = list_source_topics(
            "kafka.example.com", 4100, "ca", "cert", "key", filter_prefix="sn_"
        )
    assert topics == ["sn_bar", "sn_foo"]
    assert "other" not in topics


def test_list_source_topics_exits_on_connection_error(capsys):
    from sn_confluent.mirror.main import list_source_topics
    mock_consumer = MagicMock()
    mock_consumer.topics.side_effect = Exception("Connection refused")
    with patch("sn_confluent.mirror.main.KafkaConsumer", return_value=mock_consumer):
        with pytest.raises(SystemExit) as exc:
            list_source_topics("kafka.example.com", 4100, "ca", "cert", "key")
    assert exc.value.code == 1
    assert "Connection refused" in capsys.readouterr().err


def test_list_source_topics_closes_consumer_on_topics_error():
    from sn_confluent.mirror.main import list_source_topics
    mock_consumer = MagicMock()
    mock_consumer.topics.side_effect = Exception("Connection refused")
    with patch("sn_confluent.mirror.main.KafkaConsumer", return_value=mock_consumer):
        with pytest.raises(SystemExit):
            list_source_topics("kafka.example.com", 4100, "ca", "cert", "key")
    mock_consumer.close.assert_called_once()


def test_list_source_topics_sets_kafka_timeouts():
    from sn_confluent.mirror.main import (
        KAFKA_CONNECTIONS_MAX_IDLE_MS,
        KAFKA_METADATA_MAX_AGE_MS,
        KAFKA_REQUEST_TIMEOUT_MS,
        list_source_topics,
    )
    mock_consumer = MagicMock()
    mock_consumer.topics.return_value = {"alpha"}
    with patch("sn_confluent.mirror.main.KafkaConsumer", return_value=mock_consumer) as kafka_consumer:
        list_source_topics("kafka.example.com", 4100, "ca", "cert", "key")
    kafka_consumer.assert_called_once_with(
        bootstrap_servers="kafka.example.com:4100,kafka.example.com:4101,kafka.example.com:4102,kafka.example.com:4103",
        security_protocol="SSL",
        ssl_cafile="ca",
        ssl_certfile="cert",
        ssl_keyfile="key",
        request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
        metadata_max_age_ms=KAFKA_METADATA_MAX_AGE_MS,
        connections_max_idle_ms=KAFKA_CONNECTIONS_MAX_IDLE_MS,
    )


# ---------------------------------------------------------------------------
# get_mirrored_source_topics
# ---------------------------------------------------------------------------

def test_get_mirrored_source_topics_strips_prefix():
    from sn_confluent.mirror.main import get_mirrored_source_topics
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

    with patch("sn_confluent.mirror.main.subprocess.run", side_effect=fake_run):
        result = get_mirrored_source_topics(link_cfg)

    assert result == {"foo", "bar", "baz"}


def test_get_mirrored_source_topics_warns_and_returns_empty_on_cli_failure(capsys):
    from sn_confluent.mirror.main import get_mirrored_source_topics
    link_cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    mock_result = MagicMock(returncode=1, stdout="", stderr="Unauthorized")
    with patch("sn_confluent.mirror.main.subprocess.run", return_value=mock_result):
        result = get_mirrored_source_topics(link_cfg)
    assert result == set()
    err = capsys.readouterr().err
    assert "Warning" in err
    assert "servicenow-link-4100" in err


# ---------------------------------------------------------------------------
# enable_auto_mirror
# ---------------------------------------------------------------------------

def test_enable_auto_mirror_dry_run_prints_commands(capsys):
    from sn_confluent.mirror.main import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("sn_confluent.mirror.main.subprocess.run") as mock_run:
        enable_auto_mirror(cfg, dry_run=True)
        mock_run.assert_not_called()
    out = capsys.readouterr().out
    assert "servicenow-link-4100" in out
    assert "servicenow-link-4200" in out
    # config is written to a temp file; the key appears in the printed file contents
    assert "auto.create.mirror.topics.enable=true" in out


def test_enable_auto_mirror_calls_update_on_both_links():
    from sn_confluent.mirror.main import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    mock_result = MagicMock(returncode=0, stdout="Updated.")
    with patch("sn_confluent.mirror.main.subprocess.run", return_value=mock_result) as mock_run:
        enable_auto_mirror(cfg, dry_run=False)
    assert mock_run.call_count == 2
    calls = [str(c) for c in mock_run.call_args_list]
    assert any("servicenow-link-4100" in c for c in calls)
    assert any("servicenow-link-4200" in c for c in calls)


def test_enable_auto_mirror_exits_on_cli_failure(capsys):
    from sn_confluent.mirror.main import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("sn_confluent.mirror.main.subprocess.run", return_value=MagicMock(returncode=1, stderr="fail")):
        with pytest.raises(SystemExit) as exc:
            enable_auto_mirror(cfg, dry_run=False)
    assert exc.value.code == 1


def test_enable_auto_mirror_with_filters_includes_filter_config(capsys):
    from sn_confluent.mirror.main import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("sn_confluent.mirror.main.subprocess.run") as mock_run:
        enable_auto_mirror(cfg, dry_run=True, include_prefixes=["snc.hermes1"])
        mock_run.assert_not_called()
    # filter key/value appear in the printed config file contents block
    out = capsys.readouterr().out
    assert "auto.create.mirror.topics.filters" in out
    assert "snc.hermes1" in out
    assert "INCLUDE" in out
    assert "PREFIXED" in out


def test_enable_auto_mirror_with_filters_live_run_passes_config_file_and_cleans_up():
    import os
    from sn_confluent.mirror.main import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    config_paths = []

    def capture_config_path(cmd, **kwargs):
        config_idx = cmd.index("--config") + 1
        config_paths.append(cmd[config_idx])
        return MagicMock(returncode=0, stdout="Updated.")

    with patch("sn_confluent.mirror.main.subprocess.run", side_effect=capture_config_path):
        enable_auto_mirror(cfg, dry_run=False, include_prefixes=["snc.hermes1"])

    assert len(config_paths) == 2
    for path in config_paths:
        assert path.endswith(".properties")
        # temp file must be deleted after the call
        assert not os.path.exists(path)


# ---------------------------------------------------------------------------
# build_mirror_filters
# ---------------------------------------------------------------------------

def test_build_mirror_filters_returns_none_when_no_args():
    from sn_confluent.mirror.main import build_mirror_filters
    assert build_mirror_filters() is None


def test_build_mirror_filters_include_prefix():
    from sn_confluent.mirror.main import build_mirror_filters
    result = json.loads(build_mirror_filters(include_prefixes=["snc."]))
    assert result == {"topicFilters": [{"filterType": "INCLUDE", "name": "snc.", "patternType": "PREFIXED"}]}


def test_build_mirror_filters_exclude_prefix():
    from sn_confluent.mirror.main import build_mirror_filters
    result = json.loads(build_mirror_filters(exclude_prefixes=["internal"]))
    assert result == {"topicFilters": [{"filterType": "EXCLUDE", "name": "internal", "patternType": "PREFIXED"}]}


def test_build_mirror_filters_include_topic():
    from sn_confluent.mirror.main import build_mirror_filters
    result = json.loads(build_mirror_filters(include_topics=["my-topic"]))
    assert result == {"topicFilters": [{"filterType": "INCLUDE", "name": "my-topic", "patternType": "LITERAL"}]}


def test_build_mirror_filters_exclude_topic():
    from sn_confluent.mirror.main import build_mirror_filters
    result = json.loads(build_mirror_filters(exclude_topics=["skip-me"]))
    assert result == {"topicFilters": [{"filterType": "EXCLUDE", "name": "skip-me", "patternType": "LITERAL"}]}


def test_build_mirror_filters_multiple_entries():
    from sn_confluent.mirror.main import build_mirror_filters
    result = json.loads(build_mirror_filters(
        include_prefixes=["snc.hermes1"],
        exclude_prefixes=["internal"],
        include_topics=["exact-topic"],
        exclude_topics=["skip-this"],
    ))
    entries = result["topicFilters"]
    assert len(entries) == 4
    types = {(e["filterType"], e["patternType"]) for e in entries}
    assert ("INCLUDE", "PREFIXED") in types
    assert ("EXCLUDE", "PREFIXED") in types
    assert ("INCLUDE", "LITERAL") in types
    assert ("EXCLUDE", "LITERAL") in types


# ---------------------------------------------------------------------------
# create_mirror_topics
# ---------------------------------------------------------------------------

def test_create_mirror_topics_builds_correct_commands():
    from sn_confluent.mirror.main import create_mirror_topics
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    mock_result = MagicMock(returncode=0, stdout="Created.")
    with patch("sn_confluent.mirror.main.subprocess.run", return_value=mock_result) as mock_run:
        failed = create_mirror_topics(cfg, ["foo", "bar"], dry_run=False)
    assert mock_run.call_count == 4
    calls_flat = " ".join(str(c) for c in mock_run.call_args_list)
    assert "4100.foo" in calls_flat
    assert "4200.foo" in calls_flat
    assert "--source-topic" in calls_flat
    assert failed == []


def test_create_mirror_topics_dry_run_no_subprocess(capsys):
    from sn_confluent.mirror.main import create_mirror_topics
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("sn_confluent.mirror.main.subprocess.run") as mock_run:
        create_mirror_topics(cfg, ["foo"], dry_run=True)
        mock_run.assert_not_called()
    out = capsys.readouterr().out
    assert "4100.foo" in out
    assert "4200.foo" in out


def test_create_mirror_topics_continues_after_failure():
    from sn_confluent.mirror.main import create_mirror_topics
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
    with patch("sn_confluent.mirror.main.subprocess.run", side_effect=responses):
        failed = create_mirror_topics(cfg, ["foo", "bar"], dry_run=False)
    assert len(failed) == 1
    assert "foo" in failed[0]


# ---------------------------------------------------------------------------
# --profile flag
# ---------------------------------------------------------------------------

PROFILE_CONFIG = textwrap.dedent("""\
    [DEFAULT]
    environment_id = env-prod
    cluster_id     = lkc-prod
    link_name      = sn-prod-link
    source_host    = hermes1.example.com
    instance_name  = myinstance

    [dev]
    environment_id = env-dev
    cluster_id     = lkc-dev
    link_name      = sn-dev-link
    source_host    = hermes1-dev.example.com
""")


def test_load_config_profile_reads_named_section(tmp_path):
    from sn_confluent.mirror.main import load_config
    path = write_config(tmp_path, PROFILE_CONFIG)
    cfg = load_config(path, profile="dev")
    assert cfg["environment_id"] == "env-dev"
    assert cfg["cluster_id"] == "lkc-dev"


def test_load_config_profile_inherits_instance_name_from_default(tmp_path):
    from sn_confluent.mirror.main import load_config
    path = write_config(tmp_path, PROFILE_CONFIG)
    cfg = load_config(path, profile="dev")
    assert cfg["instance_name"] == "myinstance"   # not in [dev], inherited from DEFAULT


def test_load_config_profile_generates_link_names(tmp_path):
    from sn_confluent.mirror.main import load_config
    path = write_config(tmp_path, PROFILE_CONFIG)
    cfg = load_config(path, profile="dev")
    assert cfg["link_name_4100"] == "sn-dev-link-4100"
    assert cfg["link_name_4200"] == "sn-dev-link-4200"


def test_load_config_profile_not_found_exits(tmp_path, capsys):
    from sn_confluent.mirror.main import load_config
    path = write_config(tmp_path, PROFILE_CONFIG)
    with pytest.raises(SystemExit) as exc:
        load_config(path, profile="missing")
    assert exc.value.code == 1
    assert "missing" in capsys.readouterr().err
