# Mirror Topics — Design Spec

**Date:** 2026-04-23
**Status:** Approved

---

## Context

Cluster links `servicenow-link-4100` and `servicenow-link-4200` connect Confluent Cloud to the two active-active ServiceNow Kafka source clusters. Both links must always be mirroring the same topics — this is non-negotiable. Topics from each DC must be distinguishable on the destination, so each link is created with a `cluster.link.prefix` (`4100.` and `4200.` respectively), making mirrored topic names unambiguous.

This spec covers:
1. A targeted change to `create_link.py` to set `cluster.link.prefix` at link creation time.
2. A new `mirror-topics/mirror_topics.py` tool to select and configure topic mirroring across both links in one operation.

Managing or removing existing mirror topics is out of scope.

---

## Key Finding: cluster.link.prefix

`cluster.link.prefix` must be set at link creation — it is not reconfigurable post-creation. Once set, Confluent Cloud automatically applies the prefix to all mirror topics created under that link, including auto-mirrored ones.

`mirror.topic.rename.format` returns HTTP 404 in Confluent Cloud — not supported.

---

## Change to create_link.py

When building dual-link properties in the `source_host` branch, append `cluster.link.prefix` to the properties written to the temp file for each link:

- `servicenow-link-4100` → `cluster.link.prefix=4100.`
- `servicenow-link-4200` → `cluster.link.prefix=4200.`

No interface change. Existing `link.conf` format unchanged.

---

## Files

```
PEM Tool/
├── cluster-link/
│   ├── create_link.py          # +cluster.link.prefix in dual-link properties
│   └── tests/
├── mirror-topics/              # new
│   ├── mirror_topics.py
│   ├── requirements.txt        # kafka-python, questionary
│   └── tests/
│       └── test_mirror_topics.py
```

---

## CLI Interface

```
python mirror_topics.py [--config PATH] [--pem-dir PATH] [--filter PREFIX] [--all] [--dry-run]

  --config    Path to link.conf (default: ../cluster-link/link.conf)
  --pem-dir   Directory with PEM files (default: ./)
  --filter    Pre-filter topic list by prefix before showing UI (e.g. --filter sn_)
  --all       Skip UI; enable auto-mirror on both links
  --dry-run   Print what would be created/configured without executing
```

`--all --dry-run` is valid and prints the two `confluent kafka link configuration update` commands that would run.

Link names are derived from `link_name` in `link.conf` as `{link_name}-4100` and `{link_name}-4200`, consistent with `create_link.py`.

---

## Runtime Flow

```
1. Load config from link.conf → derive link names ({link_name}-4100, {link_name}-4200)
2. Check confluent CLI in PATH
3. Verify auth: confluent kafka cluster describe
4. Load PEM files from --pem-dir

5. Connect to source brokers (hermes1:4100–4103) via kafka-python AdminClient with mTLS
6. Fetch topic list → apply --filter if provided → sort alphabetically

7. If --all:
   a. If --dry-run: print the two configuration update commands, exit 0
   b. Run: confluent kafka link configuration update {link-4100}
           --config "auto.create.mirror.topics.enable=true"
      Run: confluent kafka link configuration update {link-4200}
           --config "auto.create.mirror.topics.enable=true"
   c. Print confirmation, exit 0

8. Fetch already-mirrored topics from both links:
   confluent kafka mirror list --link {link} --output json
   → for each mirror topic name, strip the link's prefix (e.g. "4100.my-topic" → "my-topic")
   → union results from both links → set of already-mirrored source topic names

9. Present questionary checkbox UI:
   - Already-mirrored topics shown with [mirrored] tag, pre-unchecked, selectable
   - Remaining topics available for selection
   - Type to filter within the UI

10. If nothing selected → print message, exit 0

11. Print confirmation summary:
    "Will create N mirror topics on {link-4100} and {link-4200}"
    List: 4100.{topic}, 4200.{topic} for each selection
    Prompt: proceed? (y/N) — default No

12. For each selected topic, for each link:
    confluent kafka mirror create {prefix}{topic} \
      --link {link_name} --source-topic {topic} \
      --environment {env_id} --cluster {cluster_id}

13. Print results: succeeded / failed per topic
    Exit 1 if any failed, exit 0 if all succeeded
```

---

## Error Handling

| Condition | Behavior |
|---|---|
| `link.conf` missing or incomplete | exit 1, same messages as `create_link.py` |
| `confluent` not in PATH | exit 1, print install instructions |
| Not authenticated | exit 1, "run: confluent login" |
| PEM file missing | exit 1, "run: extract_pem.py first" |
| Source broker unreachable | exit 1, kafka-python error surfaced cleanly |
| No topics match `--filter` | exit 0, "No topics found matching filter '{prefix}'" |
| Nothing selected in UI | exit 0, "No topics selected." |
| Mirror create fails for a topic | print error, continue remaining, exit 1 at end |
| Already-mirrored topic re-selected | CLI error caught, counted as failed, execution continues |

---

## Testing

Tests in `mirror-topics/tests/test_mirror_topics.py`, mocking `subprocess.run` and `kafka-python` AdminClient.

- Config loading and link name derivation from `link_name`
- `--filter` narrows topic list correctly
- Already-mirrored detection strips prefix and builds correct source-name set
- `--all --dry-run` prints correct `configuration update` commands, no subprocess calls
- `--all` live mode calls `configuration update` on both links
- Per-topic: correct `mirror create` command built with `--source-topic` and prefixed dest name
- Mirror create failure on one topic does not abort remaining topics
- Broker unreachable surfaces clean error message
