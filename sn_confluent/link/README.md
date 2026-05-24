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

For ServiceNow active-active mode (dual-cluster), use `source_host` instead of `source_bootstrap`. The tool creates one link per cluster in `source_clusters` and polls each to `ACTIVE`:

```ini
[confluent]
environment_id      = env-xxxxxx
cluster_id          = lkc-xxxxxx
link_name           = servicenow-link
source_host         = hermes1.service-now.com
instance_name       = snc.yourinstance

# Optional — defaults shown:
# source_clusters     = 4100, 4200
# brokers_per_cluster = 4
```

`link.conf` is gitignored — your live IDs stay local.

## Usage

```bash
sn-confluent link --pem-dir /path/to/pems
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `./link.conf` | Path to config file |
| `--pem-dir PATH` | `./` | Directory containing PEM files |
| `--timeout SECS` | `60` | Seconds to wait for `ACTIVE` state |
| `--dry-run` | off | Print CLI command without executing |
| `--copy-config` | off | Copy SSL properties to clipboard for pasting into the web console |
| `--literal-newlines` | off | Use actual newlines in PEM values instead of `\n` escapes |

Set `KEY_PASSWORD` before running (non-interactive) or omit it to be prompted. **Omitting a key password in a production environment is a security risk** — the private key is stored unencrypted in Confluent Cloud cluster link configuration where any operator with environment access can retrieve it. A prominent warning is printed to stderr when no password is provided.

```bash
KEY_PASSWORD=yourpassword sn-confluent link
```

### Web console path

Use `--copy-config` to copy the SSL properties block to your clipboard, then paste into **Environments → Cluster links → Create cluster link → Additional configurations**.

```bash
sn-confluent link --pem-dir /path/to/pems --copy-config
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```
