# worker

Generates a Kafka Connect distributed worker configuration and launch script for running Confluent Replicator against a Confluent Cloud backing cluster.

Replicator is not a fully-managed connector — it must run on a self-managed Connect worker. This subcommand automates the worker setup so you can jump straight to `sn-confluent replicate`.

## Prerequisites

- Python 3.10+
- Confluent CLI installed and authenticated (`confluent login`)
- A Confluent Cloud cluster (the worker's backing cluster)
- A CC API key/secret scoped to that cluster
- Confluent Platform or confluent-hub for the Replicator plugin

## Configuration

Copy and fill in `worker.conf`:

```bash
cp worker.conf.example worker.conf
```

```ini
[confluent]
environment_id = env-xxxxx
cluster_id     = lkc-xxxxx

# CC API credentials (or use CC_API_KEY / CC_API_SECRET env vars)
# cc_api_key    =
# cc_api_secret =

# Optional
# plugin_dir    = ./plugins
# rest_port     = 8083
# group_id      = connect-cluster
# out_dir       = .
```

You can also point `--config` at your existing `link.conf` — the config files share the same `[confluent]` section format and the required keys (`environment_id`, `cluster_id`) are common to both.

## Usage

```bash
# Generate worker config and start script
sn-confluent worker --config worker.conf --out-dir ./sandbox

# Dry run — print generated content without writing files
sn-confluent worker --config worker.conf --dry-run

# Auto-install Replicator plugin via confluent-hub
sn-confluent worker --config worker.conf --auto-install

# Custom Confluent/Kafka home for the start script
sn-confluent worker --config worker.conf --kafka-home /opt/confluent
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `./worker.conf` | Path to config file |
| `--out-dir PATH` | `out_dir` from config or `.` | Where to write generated files |
| `--kafka-home PATH` | `${CONFLUENT_HOME:-/usr/local/confluent}` | Kafka/Confluent home for start script |
| `--auto-install` | off | Install Replicator plugin via confluent-hub without prompting |
| `--dry-run` | off | Print generated content, write no files |

## Generated files

### `connect-worker.properties`

A complete `connect-distributed` worker config:

- Backed to your CC cluster via SASL/SSL (PLAIN + API key)
- `connector.client.config.override.policy=All` — **required** for cc-to-sn Replicator so `producer.override.*` settings write to SN Hermes instead of CC
- `ByteArrayConverter` for both key and value (no schema registry dependency)
- Replication factor 3 for internal topics (CC standard)

File permissions: `0600` (credentials are embedded).

### `start-worker.sh`

A bash script that invokes `connect-distributed` with the generated properties. Points at `CONFLUENT_HOME` (or the `--kafka-home` value) on the target machine.

## Replicator plugin

`io.confluent.connect.replicator.ReplicatorSourceConnector` is proprietary and must be installed separately:

**Via confluent-hub (recommended):**
```bash
confluent-hub install confluentinc/kafka-connect-replicator:latest \
  --component-dir ./plugins
```

**Via Confluent Platform tarball:**
```bash
# Download from https://www.confluent.io/installation/
# Copy share/java/kafka-connect-replicator/ into your plugin dir
```

Use `--auto-install` to let `sn-confluent worker` invoke confluent-hub automatically.

## API credentials

Provide CC API credentials via:

1. Environment variables (recommended for automation):
   ```bash
   export CC_API_KEY=<key>
   export CC_API_SECRET=<secret>
   sn-confluent worker --config worker.conf
   ```

2. Config file (`cc_api_key` / `cc_api_secret` keys)
3. Interactive prompt (if neither env var nor config key is set)

## On the sandbox host

```bash
# 1. Copy sandbox/ to your Linux host (or run on WSL/Docker)
scp -r sandbox/ user@sandbox-host:~/connect/

# 2. Install Replicator plugin (if not using --auto-install)
confluent-hub install confluentinc/kafka-connect-replicator:latest \
  --component-dir ~/connect/plugins

# 3. Start the worker
bash ~/connect/start-worker.sh

# 4. Deploy the Replicator connector
sn-confluent replicate --connect-url http://sandbox-host:8083 --all --no-wizard
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `connect-distributed not found` | Set `CONFLUENT_HOME` on the target host or pass `--kafka-home` at generation time |
| Worker fails to start — auth error | Verify API key/secret and cluster ID match the CC cluster |
| Worker starts but connector FAILED | Check `connector.client.config.override.policy=All` is in properties (it's hardcoded) |
| Plugin class not found | Install Replicator plugin into `plugin_dir`; re-run `sn-confluent worker --auto-install` |
| Internal topic replication errors | CC may need the internal topics pre-created; run `confluent kafka topic create _connect-configs --partitions 1 --replication-factor 3` etc. |

## Tests

```bash
pytest sn_confluent/worker/tests/
```
