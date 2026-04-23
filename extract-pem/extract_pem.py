# Extract PEM files from PKCS12 keystores for Confluent Cloud mTLS setup.
#
# Setup: pip install -r requirements.txt
# Usage: python extract_pem.py [--keystore PATH] [--truststore PATH] [--out-dir PATH]
#
# Outputs:
#   ca.pem          — CA certificates from truststore (upload to Confluent Cloud)
#   client-cert.pem — Client certificate chain from keystore (leaf + intermediates)
#   client-key.pem  — Client private key from keystore (PKCS8; encrypted if --key-password given)

import argparse
import getpass
import os
import sys

from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption, BestAvailableEncryption
from cryptography.exceptions import InvalidTag


def load_p12(path: str, password: str):
    """Load a PKCS12 file. Prints a clear error and exits 1 on failure."""
    if not os.path.exists(path):
        print(f"Error: File not found: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path, "rb") as fh:
        data = fh.read()
    pw_bytes = password.encode("utf-8")
    try:
        return pkcs12.load_key_and_certificates(data, pw_bytes)
    except (ValueError, InvalidTag) as exc:
        print(
            f"Error: Incorrect password or corrupted keystore: {path}",
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as exc:
        print(f"Error: Could not load {path}: {exc}", file=sys.stderr)
        sys.exit(1)


def extract_ca_pem(truststore_path: str, password: str) -> bytes:
    """Return concatenated PEM bytes of all CA certs in the truststore."""
    _key, cert, additional = load_p12(truststore_path, password)
    certs = []
    if cert is not None:
        certs.append(cert)
    if additional:
        certs.extend(additional)
    if not certs:
        print("Warning: No certificates found in truststore.", file=sys.stderr)
        return b""
    return b"".join(c.public_bytes(Encoding.PEM) for c in certs)


def extract_client_cert_pem(keystore_path: str, password: str) -> bytes:
    """Return concatenated PEM bytes of the client cert chain from the keystore."""
    _key, cert, additional = load_p12(keystore_path, password)
    certs = []
    if cert is not None:
        certs.append(cert)
    if additional:
        certs.extend(additional)
    if not certs:
        print("Warning: No certificates found in keystore.", file=sys.stderr)
        return b""
    return b"".join(c.public_bytes(Encoding.PEM) for c in certs)


def extract_client_key_pem(keystore_path: str, password: str, key_encrypt_password: str = None) -> bytes:
    """Return PKCS8 PEM bytes of the private key from the keystore.

    If key_encrypt_password is given, the output is encrypted PKCS8
    (BEGIN ENCRYPTED PRIVATE KEY); otherwise it is unencrypted.
    """
    key, _cert, _additional = load_p12(keystore_path, password)
    if key is None:
        print("Warning: No private key found in keystore.", file=sys.stderr)
        return b""
    enc = (
        BestAvailableEncryption(key_encrypt_password.encode("utf-8"))
        if key_encrypt_password
        else NoEncryption()
    )
    return key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=enc,
    )


def set_key_permissions(path: str) -> None:
    """Restrict private key file to owner-read-only on Unix. Warn on Windows."""
    if os.name != "nt":
        os.chmod(path, 0o600)
    else:
        print(
            f"Warning: On Windows, manually restrict access to {path}",
            file=sys.stderr,
        )


def _write(path: str, data: bytes) -> None:
    with open(path, "wb") as fh:
        fh.write(data)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract CA and client PEM files from PKCS12 keystores for mTLS."
    )
    parser.add_argument(
        "--keystore", default="keystore",
        help="Path to PKCS12 keystore (default: ./keystore)",
    )
    parser.add_argument(
        "--truststore", default="truststore",
        help="Path to PKCS12 truststore (default: ./truststore)",
    )
    parser.add_argument(
        "--out-dir", default=".",
        help="Directory to write output PEM files (default: ./)",
    )
    parser.add_argument(
        "--key-password", default=None, metavar="PASSWORD",
        help="Encrypt the output private key with this password (adds ssl.key.password support)",
    )
    args = parser.parse_args()

    password = getpass.getpass("Keystore password: ")

    ca_pem = extract_ca_pem(args.truststore, password)
    client_cert_pem = extract_client_cert_pem(args.keystore, password)
    client_key_pem = extract_client_key_pem(args.keystore, password, args.key_password)

    ca_path = os.path.join(args.out_dir, "ca.pem")
    cert_path = os.path.join(args.out_dir, "client-cert.pem")
    key_path = os.path.join(args.out_dir, "client-key.pem")

    _write(ca_path, ca_pem)
    _write(cert_path, client_cert_pem)
    _write(key_path, client_key_pem)
    set_key_permissions(key_path)

    print(f"Written: {ca_path}")
    print(f"Written: {cert_path}")
    print(f"Written: {key_path}")


if __name__ == "__main__":
    main()
