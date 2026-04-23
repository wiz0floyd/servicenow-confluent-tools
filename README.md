# ServiceNow — Confluent Tools

A collection of CLI tools for integrating ServiceNow's Kafka infrastructure with Confluent Cloud.

## Tools

| Tool | Description |
|---|---|
| [`extract-pem/`](extract-pem/) | Extract `ca.pem`, `client-cert.pem`, and `client-key.pem` from Java KeyStore / TrustStore files |
| [`cluster-link/`](cluster-link/) | Create a Confluent Cloud Cluster Link from a source Kafka cluster using mTLS |

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
