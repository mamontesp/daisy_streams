"""Unit tests for Snowflake key-pair and externalbrowser authentication."""

import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from snowflake_adapter.client import (
    SnowflakeConnectorClient,
    load_private_key_bytes,
)
from snowflake_adapter.config import Settings


@pytest.fixture
def rsa_keys():
    """Generate a test RSA private key and return unencrypted & encrypted PEMs."""
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )
    unencrypted_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")

    passphrase = "test_passphrase_123"
    encrypted_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(passphrase.encode("utf-8")),
    ).decode("utf-8")

    return {
        "unencrypted_pem": unencrypted_pem,
        "encrypted_pem": encrypted_pem,
        "passphrase": passphrase,
    }


def test_load_private_key_bytes_from_content(rsa_keys):
    """Test loading unencrypted private key from raw string content."""
    der_bytes = load_private_key_bytes(content=rsa_keys["unencrypted_pem"])
    assert isinstance(der_bytes, bytes)
    assert len(der_bytes) > 0


def test_load_private_key_bytes_from_file(rsa_keys, tmp_path):
    """Test loading unencrypted private key from a temporary file path."""
    key_file = tmp_path / "rsa_key.p8"
    key_file.write_text(rsa_keys["unencrypted_pem"])

    der_bytes = load_private_key_bytes(path=str(key_file))
    assert isinstance(der_bytes, bytes)
    assert len(der_bytes) > 0


def test_load_encrypted_private_key_with_passphrase(rsa_keys):
    """Test loading encrypted private key with correct passphrase."""
    der_bytes = load_private_key_bytes(
        content=rsa_keys["encrypted_pem"],
        passphrase=rsa_keys["passphrase"],
    )
    assert isinstance(der_bytes, bytes)
    assert len(der_bytes) > 0


def test_load_encrypted_private_key_wrong_passphrase(rsa_keys):
    """Test loading encrypted private key with incorrect passphrase raises error."""
    with pytest.raises(Exception):
        load_private_key_bytes(
            content=rsa_keys["encrypted_pem"],
            passphrase="wrong_password",
        )


def test_load_private_key_bytes_missing_inputs():
    """Test error when neither path nor content is provided."""
    with pytest.raises(ValueError, match="Key-pair authentication requires either"):
        load_private_key_bytes()


def test_snowflake_connector_rejects_unsupported_authenticator():
    """Test that unsupported authenticators like 'password' are explicitly rejected."""
    with pytest.raises(ValueError, match="Unsupported authenticator 'password'"):
        SnowflakeConnectorClient(
            account="xy12345",
            user="test_user",
            authenticator="password",
        )


def test_snowflake_connector_externalbrowser():
    """Test connecting with externalbrowser sets appropriate connection parameter."""
    with patch("snowflake.connector.connect") as mock_connect:
        mock_connect.return_value = MagicMock()

        client = SnowflakeConnectorClient(
            account="xy12345",
            user="dev_user",
            authenticator="externalbrowser",
            role="DEV_ROLE",
            warehouse="DEV_WH",
            database="DEV_DB",
            schema="DEV_SCHEMA",
        )

        mock_connect.assert_called_once()
        kwargs = mock_connect.call_args[1]
        assert kwargs["authenticator"] == "externalbrowser"
        assert kwargs["user"] == "dev_user"
        assert "private_key" not in kwargs
        assert "password" not in kwargs


def test_snowflake_connector_key_pair(rsa_keys):
    """Test connecting with key_pair passes DER encoded bytes to snowflake connector."""
    with patch("snowflake.connector.connect") as mock_connect:
        mock_connect.return_value = MagicMock()

        client = SnowflakeConnectorClient(
            account="xy12345",
            user="DAISY_SERVICE_USER",
            authenticator="key_pair",
            private_key_content=rsa_keys["unencrypted_pem"],
            role="DAISY_INGESTION_ROLE",
            warehouse="COMPUTE_WH",
        )

        mock_connect.assert_called_once()
        kwargs = mock_connect.call_args[1]
        assert kwargs["user"] == "DAISY_SERVICE_USER"
        assert "private_key" in kwargs
        assert isinstance(kwargs["private_key"], bytes)
        assert "password" not in kwargs
        assert "authenticator" not in kwargs


def test_settings_authenticator_configuration():
    """Test that Settings parses authenticator correctly."""
    s1 = Settings(SNOWFLAKE_AUTHENTICATOR="key_pair")
    assert s1.snowflake_authenticator == "key_pair"

    s2 = Settings(SNOWFLAKE_AUTHENTICATOR="externalbrowser")
    assert s2.snowflake_authenticator == "externalbrowser"
