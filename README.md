# sn-confluent

[![CI](https://github.com/wiz0floyd/servicenow-confluent-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/wiz0floyd/servicenow-confluent-tools/actions/workflows/ci.yml)

A unified CLI for integrating ServiceNow's Kafka infrastructure with Confluent Cloud.

> **Disclaimer:** This is personal work and is not supported, endorsed, or affiliated with ServiceNow or Confluent. Use at your own risk.

## Requirements

- Python 3.9+
- **Confluent CLI** — required for cluster link and topic mirroring operations. [Download here](https://docs.confluent.io/confluent-cli/current/install.html)

## Install

```bash
pip install -e .
```

Installs the `sn-confluent` console entry point.

## Subcommands

| Subcommand | Description |
|---|---|
| `sn-confluent extract` | Extract `ca.pem`, `client-cert.pem`, and `client-key.pem` from PKCS12 keystores |
| `sn-confluent link` | Create a Confluent Cloud cluster link from a source Kafka cluster using mTLS |
| `sn-confluent mirror` | Mirror ServiceNow Kafka topics to Confluent Cloud across both DC cluster links with a checkbox UI |
| `sn-confluent worker` | Generate Connect worker config and start script for Confluent Replicator |
| `sn-confluent replicate` | Deploy a Confluent Replicator connector between ServiceNow Hermes and Confluent Cloud (bidirectional, interactive wizard) |
| `sn-confluent deploy sink` | Deploy the Hermes Kafka Connector to Confluent Cloud as a **sink** (CC → Hermes) |
| `sn-confluent deploy source` | Deploy the Hermes Kafka Connector to Confluent Cloud as a **source** (Hermes → CC) |
| `sn-confluent api-key` | Create, list, or delete Confluent Cloud Kafka API keys |
| `sn-confluent setup` | Guided end-to-end wizard that orchestrates the steps above in one run |

Run `sn-confluent <subcommand> --help` for command-specific options. Most subcommands have a README at `sn_confluent/<subcommand>/README.md`.

## Typical workflow

```bash
# Step 1 — extract PEM files from PKCS12 keystores
sn-confluent extract --keystore /path/to/keystore --truststore /path/to/truststore --out-dir /tmp/pems

# Step 2 — create the cluster link
cp sn_confluent/link/link.conf.example sn_confluent/link/link.conf  # fill in your cluster IDs
sn-confluent link --pem-dir /tmp/pems

# Step 3 — mirror topics interactively
sn-confluent mirror --pem-dir /tmp/pems

# Step 4 — deploy Replicator (alternative to native cluster links)
sn-confluent replicate --pem-dir /tmp/pems

# Alternatively — deploy via the Hermes custom Kafka Connector
cp sn_confluent/deploy/deploy.conf.example sn_confluent/deploy/deploy.conf  # fill in your cluster/API details
sn-confluent deploy sink    # CC → Hermes
# or
sn-confluent deploy source  # Hermes → CC
```

Or run all four with the guided wizard:

```bash
sn-confluent setup
```
