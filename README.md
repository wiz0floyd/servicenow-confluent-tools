# ServiceNow — Confluent Tools

A collection of CLI tools for integrating ServiceNow's Kafka infrastructure with Confluent Cloud.

> **Disclaimer:** This is personal work and is not supported, endorsed, or affiliated with ServiceNow or Confluent. Use at your own risk.

## Requirements

- **Confluent CLI** — required for cluster link and topic mirroring operations. [Download here](https://docs.confluent.io/confluent-cli/current/install.html)

## Tools

| Tool | Description |
|---|---|
| [`extract-pem/`](extract-pem/) | Extract `ca.pem`, `client-cert.pem`, and `client-key.pem` from Java KeyStore / TrustStore files |
| [`cluster-link/`](cluster-link/) | Create a Confluent Cloud Cluster Link from a source Kafka cluster using mTLS |
| [`mirror-topics/`](mirror-topics/) | Mirror ServiceNow Kafka topics to Confluent Cloud across both DC cluster links with a checkbox UI |
| [`connect-replicator/`](connect-replicator/) | Deploy a Confluent Replicator connector between ServiceNow Hermes and Confluent Cloud (bidirectional, interactive wizard) |

## Typical workflow

```bash
# Step 1 — extract PEM files from JKS keystores
cd extract-pem
pip install -r requirements.txt
python extract_pem.py --keystore ../path/to/keystore --truststore ../path/to/truststore --out-dir /tmp/pems

# Step 2 — create the cluster link
cd ../cluster-link
pip install -r requirements.txt
cp link.conf.example link.conf   # fill in your cluster IDs
python create_link.py --pem-dir /tmp/pems
```

See each tool's README for full options.
