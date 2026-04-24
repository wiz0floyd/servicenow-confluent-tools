# mirror-topics

Mirrors ServiceNow Kafka topics to Confluent Cloud across both DC cluster links (`-4100` and `-4200`). Presents a checkbox UI to select topics, skips already-mirrored ones, and optionally enables auto-mirror for all future topics.

## Prerequisites

- Confluent CLI installed and authenticated (`confluent login`)
- PEM files from [`extract-pem`](../extract-pem/) or equivalent
- `link.conf` configured via [`cluster-link`](../cluster-link/)

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python mirror_topics.py [--config PATH] [--pem-dir PATH] [--filter PREFIX] [--all] [--dry-run]
```

Default run — shows a checkbox list of topics filtered by `instance_name` from `link.conf`:

```bash
python mirror_topics.py --pem-dir /tmp/pems
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `../cluster-link/link.conf` | Path to config file |
| `--pem-dir PATH` | `./` | Directory containing PEM files |
| `--filter PREFIX` | `instance_name` from config | Pre-filter topics by prefix before showing the UI |
| `--all` | off | Enable `auto.create.mirror.topics.enable=true` on both links and exit (skips UI) |
| `--dry-run` | off | Print CLI commands without executing |

## link.conf keys used

```ini
[confluent]
environment_id = env-xxxxxx
cluster_id     = lkc-xxxxxx
link_name      = my-cluster-link     # tool appends -4100 and -4200
source_host    = broker.example.com  # brokers at ports 4100–4103 and 4200–4203
instance_name  = myinstance          # default topic filter prefix
```

`link.conf` is gitignored — your live IDs stay local.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```
