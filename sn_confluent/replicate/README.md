# connect-replicator

Deploys a Confluent Replicator (`ReplicatorSourceConnector`) between ServiceNow Hermes and Confluent Cloud. Targets a self-managed Connect worker via REST API. Includes an interactive topic picker wizard.

Replicator is **not** available as a fully-managed Confluent Cloud connector. You must run a self-managed Connect worker backed to your Confluent Cloud cluster.

## Prerequisites

- Python 3.10+
- Confluent CLI installed and authenticated (`confluent login`)
- PEM files from [`extract-pem`](../extract-pem/) (`ca.pem`, `client-cert.pem`, `client-key.pem`)
- A running self-managed Confluent Connect worker backed to your CC cluster

Install Python dependencies:

```bash
pip install -r requirements.txt
```

> **cc-to-sn users:** Your Connect worker **must** have `connector.client.config.override.policy=All` in its worker properties. Without this, `producer.override.*` settings are silently ignored and the connector writes to the wrong cluster. See [Connect security docs](https://docs.confluent.io/platform/current/connect/security.html).

## Configuration

Copy and fill in `link.conf`:

```bash
cp link.conf.example link.conf
```

```ini
[confluent]
# Required (both directions)
environment_id      = env-xxxxx
cluster_id          = lkc-xxxxx
connector_name      = sn-replicator
source_host         = hermes.servicenow.com
pem_dir             = ./pem

# Optional
direction           = cc-to-sn        # default; or sn-to-cc
topics              =                  # comma-sep; leave blank to use wizard or --all
group_id            =                  # default: {connector_name}-group
topic_rename_format =                  # default: ${topic} (keep same name)
connect_url         = http://localhost:8083

# cc-to-sn: SN inbound load balancer port range
inbound_port        = 4000            # LB routes to active DC; ports 4000-4003
brokers_per_cluster = 4

# sn-to-cc: direct access to both active-active clusters
source_clusters     = 4100,4200       # one connector per cluster
```

`link.conf` is gitignored -- your live IDs stay local.

## Topology

### cc-to-sn (Confluent Cloud -> ServiceNow Hermes)

- Connect worker is source-backed to CC (no explicit `src.*` auth needed)
- `producer.override.*` redirects writes to SN Hermes via mTLS
- Single connector -- SN load balancer on `inbound_port` (default 4000) routes to the nearest active DC
- **Requires** `connector.client.config.override.policy=All` on the worker

### sn-to-cc (ServiceNow Hermes -> Confluent Cloud)

- `src.kafka.*` configures SN as the source cluster with mTLS
- Two connectors created automatically (one per active-active SN cluster in `source_clusters`)
- Direct connection to both clusters required (ports 4100, 4200 by default)

## Usage

```bash
# Interactive wizard (cc-to-sn, default)
python setup_replicator.py

# Non-interactive with all topics
python setup_replicator.py --all --no-wizard

# Specific topics
python setup_replicator.py --topics hermes_incidents,hermes_changes

# Opposite direction
python setup_replicator.py --direction sn-to-cc

# Dry-run (prints config, no create)
python setup_replicator.py --dry-run
python setup_replicator.py --all --no-wizard --dry-run

# Override connect worker URL
python setup_replicator.py --connect-url http://my-worker:8083
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `./link.conf` | Path to config file |
| `--pem-dir PATH` | `pem_dir` from config | Directory containing PEM files |
| `--direction DIR` | `cc-to-sn` | Replication direction: `cc-to-sn` or `sn-to-cc` |
| `--topics TOPICS` | none | Comma-separated topic list (skips wizard) |
| `--all` | off | Replicate all topics (skips wizard) |
| `--no-wizard` | off | Disable interactive topic picker |
| `--connect-url URL` | `http://localhost:8083` | Connect worker REST endpoint |
| `--key-password PW` | none | Password for encrypted private key (prefer env var) |
| `--dry-run` | off | Print connector config JSON without creating |

## Private key password

If your `client-key.pem` is encrypted, provide the password in one of three ways (listed by preference):

1. **Environment variable** (recommended): `export REPLICATOR_KEY_PASSWORD=...`
2. **Interactive prompt**: the script asks automatically when the key is encrypted
3. **CLI flag**: `--key-password SECRET` -- works but the password is visible in shell history

## Troubleshooting

| Symptom | Fix |
|---|---|
| Connector status is `FAILED` | Check Connect worker logs. For cc-to-sn, verify `connector.client.config.override.policy=All` in worker properties. |
| "Not authenticated" or 401 from Confluent CLI | Run `confluent login` and retry. |
| PEM files not found | Run `python ../extract-pem/extract_pem.py` first. See [`extract-pem`](../extract-pem/). |
| Connector writes to wrong cluster (cc-to-sn) | Override policy is not set. `producer.override.*` settings are silently ignored without it. |
| Connection refused on connect URL | Verify the Connect worker is running and the URL/port is correct. |

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```
