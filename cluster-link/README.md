# cluster-link

Creates a Confluent Cloud Cluster Link from a source Kafka cluster that uses JKS-based mutual TLS. Polls until the link is `ACTIVE`.

## Prerequisites

- Confluent CLI installed and authenticated (`confluent login`)
- PEM files from [`extract-pem`](../extract-pem/) or equivalent

## Setup

```bash
pip install -r requirements.txt
cp link.conf.example link.conf
```

Fill in `link.conf`:

```ini
[confluent]
environment_id   = env-xxxxxx
cluster_id       = lkc-xxxxxx
link_name        = my-cluster-link
source_bootstrap = broker1.example.com:9093,broker2.example.com:9093
```

`link.conf` is gitignored — your live IDs stay local.

## Usage

```bash
python create_link.py --pem-dir /path/to/pems
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `./link.conf` | Path to config file |
| `--pem-dir PATH` | `./` | Directory containing PEM files |
| `--key-password PASSWORD` | none | Password for encrypted private key; adds `ssl.key.password` |
| `--timeout SECS` | `60` | Seconds to wait for `ACTIVE` state |
| `--dry-run` | off | Print CLI command without executing |
| `--copy-config` | off | Copy SSL properties to clipboard for pasting into the web console |
| `--literal-newlines` | off | Use actual newlines in PEM values instead of `\n` escapes |

### Web console path

Use `--copy-config` to copy the SSL properties block to your clipboard, then paste into **Environments → Cluster links → Create cluster link → Additional configurations**.

```bash
python create_link.py --pem-dir /path/to/pems --copy-config
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```
