# extract-pem

Extracts `ca.pem`, `client-cert.pem`, and `client-key.pem` from PKCS12 / Java KeyStore files for use with Confluent Cloud mTLS.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python extract_pem.py \
  --keystore path/to/keystore \
  --truststore path/to/truststore \
  --out-dir /tmp/pems
```

Prompts once for the keystore password, then writes:

| File | Contents |
|---|---|
| `ca.pem` | CA certificates from the truststore |
| `client-cert.pem` | Client certificate chain from the keystore |
| `client-key.pem` | Client private key (PKCS8) |

## Options

| Flag | Default | Description |
|---|---|---|
| `--keystore PATH` | `./keystore` | Path to PKCS12 keystore |
| `--truststore PATH` | `./truststore` | Path to PKCS12 truststore |
| `--out-dir PATH` | `./` | Directory to write PEM files |

### Encrypted key output

If your Confluent Cloud cluster requires `ssl.key.password`, set `KEY_PASSWORD` to produce an encrypted PKCS8 key:

```bash
# Non-interactive (CI / scripting)
KEY_PASSWORD=yourpassword sn-confluent extract

# Interactive — omit the env var and you'll be prompted
sn-confluent extract
```

Supply the same password when running `sn-confluent link`.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```
