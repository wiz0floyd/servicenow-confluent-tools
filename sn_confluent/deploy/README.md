# deploy

Deploys the ServiceNow Hermes Kafka connector to Confluent Cloud as a fully-managed Custom Connector — either as a **sink** (Confluent Cloud → Hermes, i.e. messages flow from a CC topic into ServiceNow) or a **source** (Hermes → Confluent Cloud, i.e. messages flow from ServiceNow into a CC topic). The tool uploads the connector plugin JAR, registers it as a Custom Connector Plugin, and creates or updates the connector instance on the target cluster.

## Prerequisites

- Python 3.10+
- Confluent CLI installed (`confluent` on `PATH`)
- A `deploy.conf` filled in from the example (see below)
- The connector plugin ZIP built from the sibling `ServiceNow-source-and-sink-connector/` repo (auto-detected by default)
- A Confluent Cloud API key + secret scoped to the target cluster (`kafka_api_key` / `kafka_api_secret` in config, or `CC_API_KEY` / `CC_API_SECRET` env vars)
- A Cloud-level API key for the Confluent CLI itself (`cloud_api_key` / `cloud_api_secret`, or `CC_CLOUD_API_KEY` / `CC_CLOUD_API_SECRET` env vars)

## Configuration

Copy and fill in `deploy.conf`:

```bash
cp deploy.conf.example deploy.conf
```

```ini
[confluent]
environment_id  = env-xxxxx
cluster_id      = lkc-xxxxx

connector_name  = hermes-sink-prod

# Plugin ID from a prior upload — leave blank to upload on first run.
# After uploading, paste the returned ccp-xxxxx value here to skip future uploads.
plugin_id       =

topics          = my-topic
instance_name   = myinstance
hermes_topic    = snc.myinstance.sn_streamconnect.my-topic

keystore_path   = /path/to/keystore.p12
keystore_password = changeit
truststore_path = /path/to/truststore.p12
truststore_password = changeit

kafka_api_key   =
kafka_api_secret =

cloud_api_key   =
cloud_api_secret =

# Optional
# cloud       = aws       # aws (default), gcp, azure
# tasks_max   = 1
# plugin_file = ../ServiceNow-source-and-sink-connector/target/components/packages/servicenow-hermes-kafka-connector-0.1.0.zip
# plugin_name = hermes-sink-plugin
```

## Usage

```bash
# Deploy a sink connector (CC topic → Hermes)
sn-confluent deploy sink --config deploy.conf

# Deploy a source connector (Hermes → CC topic)
sn-confluent deploy source --config deploy.conf
```

```bash
# Skip the plugin upload step when the plugin already exists
sn-confluent deploy sink --plugin-id ccp-xxxxx

# Re-upload a new JAR to an existing plugin
sn-confluent deploy sink --plugin-id ccp-xxxxx --update-plugin

# Point at a specific connector ZIP
sn-confluent deploy sink --plugin-file ../ServiceNow-source-and-sink-connector/target/components/packages/servicenow-hermes-kafka-connector-0.1.0.zip

# Dry run — print connector config JSON without creating anything
sn-confluent deploy sink --dry-run

# Non-interactive (CI/CD) — fail if any required value is missing
sn-confluent deploy sink --no-wizard
```

## Options

| Flag | Default | Description |
|---|---|---|
| `--config PATH` | `./deploy.conf` | Path to deploy.conf |
| `--plugin-id ID` | — | Existing Custom Connector Plugin ID — skip the upload step |
| `--update-plugin` | off | Update the plugin at `--plugin-id` with a new JAR instead of creating a new plugin |
| `--plugin-file PATH` | auto-detected in sibling repo | Connector ZIP path |
| `--cloud {aws,gcp,azure}` | `aws` | Cloud provider for plugin upload |
| `--timeout SECONDS` | `900` | Connector status poll timeout |
| `--pem-dir PATH` | — | Directory containing `ca.pem`, `client-cert.pem`, `client-key.pem` for Hermes topic listing |
| `--dry-run` | off | Print connector config JSON without creating anything |
| `--no-wizard` | off | Non-interactive: fail immediately if any required value is missing from config |

## Wizard vs. non-interactive

By default an interactive wizard prompts for any required values missing from the config file. Pass `--no-wizard` to disable prompts and fail immediately if the config is incomplete — useful in CI/CD pipelines.

## Tests

```bash
pytest sn_confluent/deploy/tests/
```
