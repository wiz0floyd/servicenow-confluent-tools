import os
import datetime
import tempfile
import pytest

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import pkcs12, BestAvailableEncryption, NoEncryption
from cryptography.hazmat.backends import default_backend

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TEST_PASSWORD = "testpass"


def _make_key():
    return rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )


def _make_cert(key, cn="test"):
    now = datetime.datetime.now(datetime.timezone.utc)
    return (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(key, hashes.SHA256(), default_backend())
    )


def make_keystore_p12(password=TEST_PASSWORD, num_chain_certs=0):
    """PKCS12 with a private key, leaf cert, and optional chain certs."""
    key = _make_key()
    leaf = _make_cert(key, cn="client")
    chain = [_make_cert(_make_key(), cn=f"intermediate-{i}") for i in range(num_chain_certs)]
    return pkcs12.serialize_key_and_certificates(
        name=b"client",
        key=key,
        cert=leaf,
        cas=chain or None,
        encryption_algorithm=BestAvailableEncryption(password.encode()),
    )


def make_truststore_p12(num_certs=2, password=TEST_PASSWORD):
    """PKCS12 with only CA certs (no private key)."""
    certs = [_make_cert(_make_key(), cn=f"ca-{i}") for i in range(num_certs)]
    return pkcs12.serialize_key_and_certificates(
        name=None,
        key=None,
        cert=None,
        cas=certs,
        encryption_algorithm=BestAvailableEncryption(password.encode()),
    )


def write_p12(tmp_path, filename, data):
    path = tmp_path / filename
    path.write_bytes(data)
    return str(path)


# ---------------------------------------------------------------------------
# extract_ca_pem
# ---------------------------------------------------------------------------

def test_extract_ca_pem_returns_pem_bytes(tmp_path):
    from extract_pem import extract_ca_pem
    path = write_p12(tmp_path, "truststore", make_truststore_p12(num_certs=1))
    result = extract_ca_pem(path, TEST_PASSWORD)
    assert result.startswith(b"-----BEGIN CERTIFICATE-----")
    assert result.strip().endswith(b"-----END CERTIFICATE-----")


def test_extract_ca_pem_includes_all_certs(tmp_path):
    from extract_pem import extract_ca_pem
    path = write_p12(tmp_path, "truststore", make_truststore_p12(num_certs=3))
    result = extract_ca_pem(path, TEST_PASSWORD)
    assert result.count(b"-----BEGIN CERTIFICATE-----") == 3


def test_extract_ca_pem_empty_store_returns_empty_bytes(tmp_path, capsys):
    from extract_pem import extract_ca_pem
    # PKCS12 with no certs at all — use a keystore-style but with no chain
    # The simplest empty-ish case: truststore with 0 certs triggers the warning path
    # We test this by patching load_p12 to return (None, None, None)
    from unittest.mock import patch
    with patch("extract_pem.load_p12", return_value=(None, None, None)):
        result = extract_ca_pem("fake", TEST_PASSWORD)
    assert result == b""
    captured = capsys.readouterr()
    assert "Warning" in captured.err


# ---------------------------------------------------------------------------
# extract_client_cert_pem
# ---------------------------------------------------------------------------

def test_extract_client_cert_pem_returns_pem_bytes(tmp_path):
    from extract_pem import extract_client_cert_pem
    path = write_p12(tmp_path, "keystore", make_keystore_p12())
    result = extract_client_cert_pem(path, TEST_PASSWORD)
    assert result.startswith(b"-----BEGIN CERTIFICATE-----")


def test_extract_client_cert_pem_includes_chain(tmp_path):
    from extract_pem import extract_client_cert_pem
    path = write_p12(tmp_path, "keystore", make_keystore_p12(num_chain_certs=1))
    result = extract_client_cert_pem(path, TEST_PASSWORD)
    assert result.count(b"-----BEGIN CERTIFICATE-----") == 2


def test_extract_client_cert_pem_empty_store_warns(capsys):
    from extract_pem import extract_client_cert_pem
    from unittest.mock import patch
    with patch("extract_pem.load_p12", return_value=(None, None, None)):
        result = extract_client_cert_pem("fake", TEST_PASSWORD)
    assert result == b""
    captured = capsys.readouterr()
    assert "Warning" in captured.err


# ---------------------------------------------------------------------------
# extract_client_key_pem
# ---------------------------------------------------------------------------

def test_extract_client_key_pem_returns_pem_bytes(tmp_path):
    from extract_pem import extract_client_key_pem
    path = write_p12(tmp_path, "keystore", make_keystore_p12())
    result = extract_client_key_pem(path, TEST_PASSWORD)
    assert b"-----BEGIN PRIVATE KEY-----" in result


def test_extract_client_key_pem_is_unencrypted(tmp_path):
    from extract_pem import extract_client_key_pem
    path = write_p12(tmp_path, "keystore", make_keystore_p12())
    result = extract_client_key_pem(path, TEST_PASSWORD)
    assert b"-----BEGIN PRIVATE KEY-----" in result
    assert b"-----BEGIN ENCRYPTED PRIVATE KEY-----" not in result


def test_extract_client_key_pem_is_encrypted_when_key_password_given(tmp_path):
    from extract_pem import extract_client_key_pem
    path = write_p12(tmp_path, "keystore", make_keystore_p12())
    result = extract_client_key_pem(path, TEST_PASSWORD, key_encrypt_password="secretpw")
    assert b"-----BEGIN ENCRYPTED PRIVATE KEY-----" in result
    assert b"-----BEGIN PRIVATE KEY-----" not in result


def test_extract_client_key_pem_no_key_warns(capsys):
    from extract_pem import extract_client_key_pem
    from unittest.mock import patch
    with patch("extract_pem.load_p12", return_value=(None, None, None)):
        result = extract_client_key_pem("fake", TEST_PASSWORD)
    assert result == b""
    captured = capsys.readouterr()
    assert "Warning" in captured.err


# ---------------------------------------------------------------------------
# set_key_permissions
# ---------------------------------------------------------------------------

def test_set_key_permissions_sets_0600_on_unix():
    from extract_pem import set_key_permissions
    if os.name == "nt":
        pytest.skip("Unix-only test")
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        set_key_permissions(path)
        assert oct(os.stat(path).st_mode)[-3:] == "600"
    finally:
        os.unlink(path)


def test_set_key_permissions_warns_on_windows(capsys):
    from extract_pem import set_key_permissions
    from unittest.mock import patch
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        with patch("os.name", "nt"):
            set_key_permissions(path)
        captured = capsys.readouterr()
        assert "Warning" in captured.err
    finally:
        os.unlink(path)


# ---------------------------------------------------------------------------
# load_p12 error handling
# ---------------------------------------------------------------------------

def test_load_p12_exits_if_file_not_found():
    from extract_pem import load_p12
    with pytest.raises(SystemExit) as exc:
        load_p12("/nonexistent/path/keystore", TEST_PASSWORD)
    assert exc.value.code == 1


def test_load_p12_exits_on_wrong_password(tmp_path):
    from extract_pem import load_p12
    path = write_p12(tmp_path, "keystore", make_keystore_p12(password="correct"))
    with pytest.raises(SystemExit) as exc:
        load_p12(path, "wrongpassword")
    assert exc.value.code == 1
