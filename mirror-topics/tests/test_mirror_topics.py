import json
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
    from mirror_topics import load_config
    cfg = load_config(write_config(tmp_path))
    assert cfg["environment_id"] == "env-abc123"
    assert cfg["link_name_4100"] == "servicenow-link-4100"
    assert cfg["link_name_4200"] == "servicenow-link-4200"


def test_load_config_defaults_source_clusters_and_brokers(tmp_path):
    from mirror_topics import load_config, SN_SOURCE_CLUSTERS, SN_BROKERS_PER_CLUSTER
    cfg = load_config(write_config(tmp_path))
    assert cfg["source_clusters"] == SN_SOURCE_CLUSTERS
    assert cfg["brokers_per_cluster"] == SN_BROKERS_PER_CLUSTER


def test_load_config_reads_custom_source_clusters(tmp_path):
    from mirror_topics import load_config
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
    from mirror_topics import load_config
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


def test_load_config_exits_if_no_instance_name(tmp_path):
    from mirror_topics import load_config
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
    from mirror_topics import load_config
    cfg = load_config(write_config(tmp_path))
    assert cfg["instance_name"] == "snc.myinstance"


# ---------------------------------------------------------------------------
# list_source_topics
# ---------------------------------------------------------------------------

def test_list_source_topics_returns_sorted_list():
    from mirror_topics import list_source_topics
    mock_consumer = MagicMock()
    mock_consumer.topics.return_value = {"zebra", "alpha", "__consumer_offsets"}
    with patch("mirror_topics.KafkaConsumer", return_value=mock_consumer):
        topics = list_source_topics("kafka.example.com", 4100, "ca", "cert", "key")
    assert topics == ["alpha", "zebra"]
    assert "__consumer_offsets" not in topics


def test_list_source_topics_applies_filter():
    from mirror_topics import list_source_topics
    mock_consumer = MagicMock()
    mock_consumer.topics.return_value = {"sn_foo", "sn_bar", "other"}
    with patch("mirror_topics.KafkaConsumer", return_value=mock_consumer):
        topics = list_source_topics(
            "kafka.example.com", 4100, "ca", "cert", "key", filter_prefix="sn_"
        )
    assert topics == ["sn_bar", "sn_foo"]
    assert "other" not in topics


def test_list_source_topics_exits_on_connection_error(capsys):
    from mirror_topics import list_source_topics
    mock_consumer = MagicMock()
    mock_consumer.topics.side_effect = Exception("Connection refused")
    with patch("mirror_topics.KafkaConsumer", return_value=mock_consumer):
        with pytest.raises(SystemExit) as exc:
            list_source_topics("kafka.example.com", 4100, "ca", "cert", "key")
    assert exc.value.code == 1
    assert "Connection refused" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# get_mirrored_source_topics
# ---------------------------------------------------------------------------

def test_get_mirrored_source_topics_strips_prefix():
    from mirror_topics import get_mirrored_source_topics
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


# ---------------------------------------------------------------------------
# enable_auto_mirror
# ---------------------------------------------------------------------------

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
    # config is written to a temp file; the key appears in the printed file contents
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


def test_enable_auto_mirror_with_filters_includes_filter_config(capsys):
    from mirror_topics import enable_auto_mirror
    cfg = {
        "link_name_4100": "servicenow-link-4100",
        "link_name_4200": "servicenow-link-4200",
        "environment_id": "env-abc123",
        "cluster_id": "lkc-abc123",
    }
    with patch("subprocess.run") as mock_run:
        enable_auto_mirror(cfg, dry_run=True, include_prefixes=["snc.hermes1"])
        mock_run.assert_not_called()
    # filter key/value appear in the printed config file contents block
    out = capsys.readouterr().out
    assert "auto.create.mirror.topics.filters" in out
    assert "snc.hermes1" in out
    assert "INCLUDE" in out
    assert "PREFIXED" in out


# ---------------------------------------------------------------------------
# build_mirror_filters
# ---------------------------------------------------------------------------

def test_build_mirror_filters_returns_none_when_no_args():
    from mirror_topics import build_mirror_filters
    assert build_mirror_filters() is None


def test_build_mirror_filters_include_prefix():
    from mirror_topics import build_mirror_filters
    result = json.loads(build_mirror_filters(include_prefixes=["snc."]))
    assert result == [{"filterType": "INCLUDE", "name": "snc.", "patternType": "PREFIXED"}]


def test_build_mirror_filters_exclude_prefix():
    from mirror_topics import build_mirror_filters
    result = json.loads(build_mirror_filters(exclude_prefixes=["internal"]))
    assert result == [{"filterType": "EXCLUDE", "name": "internal", "patternType": "PREFIXED"}]


def test_build_mirror_filters_include_topic():
    from mirror_topics import build_mirror_filters
    result = json.loads(build_mirror_filters(include_topics=["my-topic"]))
    assert result == [{"filterType": "INCLUDE", "name": "my-topic", "patternType": "LITERAL"}]


def test_build_mirror_filters_exclude_topic():
    from mirror_topics import build_mirror_filters
    result = json.loads(build_mirror_filters(exclude_topics=["skip-me"]))
    assert result == [{"filterType": "EXCLUDE", "name": "skip-me", "patternType": "LITERAL"}]


def test_build_mirror_filters_multiple_entries():
    from mirror_topics import build_mirror_filters
    result = json.loads(build_mirror_filters(
        include_prefixes=["snc.hermes1"],
        exclude_prefixes=["internal"],
        include_topics=["exact-topic"],
        exclude_topics=["skip-this"],
    ))
    assert len(result) == 4
    types = {(e["filterType"], e["patternType"]) for e in result}
    assert ("INCLUDE", "PREFIXED") in types
    assert ("EXCLUDE", "PREFIXED") in types
    assert ("INCLUDE", "LITERAL") in types
    assert ("EXCLUDE", "LITERAL") in types


# ---------------------------------------------------------------------------
# create_mirror_topics
# ---------------------------------------------------------------------------

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
        failed = create_mirror_topics(cfg, ["foo", "bar"], dry_run=False)
    assert mock_run.call_count == 4
    calls_flat = " ".join(str(c) for c in mock_run.call_args_list)
    assert "4100.foo" in calls_flat
    assert "4200.foo" in calls_flat
    assert "--source-topic" in calls_flat
    assert failed == []


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
