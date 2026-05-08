# mirror-topics

Mirrors ServiceNow Kafka topics to Confluent Cloud across all source cluster links. Presents a checkbox UI to select topics, skips already-mirrored ones, and optionally enables auto-mirror for all future topics.

## Prerequisites

- Confluent CLI installed and authenticated (`confluent login`)
- PEM files from `extract-pem` or equivalent
- `link.conf` configured via the `link` subcommand

## Setup

From the repo root:

```bash
pip install -e .[dev]
```

## Usage

```bash
python -m sn_confluent.mirror.main [--config PATH] [--pem-dir PATH] [--filter PREFIX] [--all] [--dry-run]
                                   [--include-prefixes PREFIX ...] [--exclude-prefixes PREFIX ...]
                                   [--include-topics TOPIC ...] [--exclude-topics TOPIC ...]
```

Default run — shows a checkbox list of topics filtered by `instance_name` from `link.conf`:

```bash
python -m sn_confluent.mirror.main --pem-dir /tmp/pems
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `../link/link.conf` | Path to config file |
| `--pem-dir PATH` | `./` | Directory containing PEM files |
| `--filter PREFIX` | `instance_name` from config | Pre-filter topics by prefix before showing the UI |
| `--all` | off | Enable `auto.create.mirror.topics.enable=true` on both links and exit (skips UI) |
| `--include-prefixes PREFIX ...` | none | (`--all` only) Auto-mirror topics matching these prefixes |
| `--exclude-prefixes PREFIX ...` | none | (`--all` only) Skip topics matching these prefixes |
| `--include-topics TOPIC ...` | none | (`--all` only) Auto-mirror these exact topic names |
| `--exclude-topics TOPIC ...` | none | (`--all` only) Skip these exact topic names |
| `--dry-run` | off | Print CLI commands without executing |

## link.conf keys used

```ini
[confluent]
environment_id = env-xxxxxx
cluster_id     = lkc-xxxxxx
link_name      = my-cluster-link       # tool appends -<port> per source cluster
source_host    = kafka.example.com     # brokers addressed as <host>:<port+n>
instance_name  = myinstance            # default topic filter prefix

# Optional — defaults shown:
# source_clusters     = 4100, 4200     # one link created per cluster
# brokers_per_cluster = 4
```

`link.conf` is gitignored — your live IDs stay local.

## Tests

```bash
pytest sn_confluent/mirror/tests/
```
