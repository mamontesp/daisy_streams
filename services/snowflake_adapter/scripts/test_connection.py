#!/usr/bin/env python3
"""Quick Snowflake connection test script (outside Docker)."""

import os
import sys
from pathlib import Path

# Add services/snowflake_adapter to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from dotenv import load_dotenv
    # Load .env from project root
    root_env = Path(__file__).resolve().parent.parent.parent.parent / ".env"
    if root_env.exists():
        load_dotenv(root_env)
except ImportError:
    pass

from snowflake_adapter.config import get_settings
from snowflake_adapter.client import SnowflakeConnectorClient, load_private_key_bytes


def main():
    print("=" * 60)
    print("Snowflake Key-Pair Connection Test")
    print("=" * 60)

    try:
        settings = get_settings()
    except Exception as e:
        print(f"❌ Failed to load configuration: {e}")
        sys.exit(1)

    print(f"Account:    {settings.snowflake_account}")
    print(f"User:       {settings.snowflake_user}")
    print(f"Auth:       {settings.snowflake_authenticator}")
    print(f"Key Path:   {settings.snowflake_private_key_path}")

    # Step 1: Test private key decryption
    print("\n[Step 1/2] Decrypting private key...")
    try:
        pk_bytes = load_private_key_bytes(
            path=settings.snowflake_private_key_path,
            content=settings.snowflake_private_key_content,
            passphrase=settings.snowflake_private_key_passphrase,
        )
        print("  ✅ Private key successfully decrypted!")
    except Exception as e:
        print(f"  ❌ Decryption failed: {e}")
        print("\nTroubleshooting:")
        print("  - Check that SNOWFLAKE_PRIVATE_KEY_PASSPHRASE in .env matches the password entered when generating the key.")
        print("  - Test in terminal: openssl rsa -in secrets/rsa_key.p8 -check")
        sys.exit(1)

    # Step 2: Test Snowflake connection
    print("\n[Step 2/2] Connecting to Snowflake...")
    try:
        client = SnowflakeConnectorClient(
            account=settings.snowflake_account,
            user=settings.snowflake_user,
            authenticator=settings.snowflake_authenticator,
            private_key_path=settings.snowflake_private_key_path,
            private_key_content=settings.snowflake_private_key_content,
            private_key_passphrase=settings.snowflake_private_key_passphrase,
            role=settings.snowflake_role,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            schema=settings.snowflake_schema,
        )

        rows = client.execute_query(
            "SELECT CURRENT_USER() AS CURR_USER, CURRENT_ROLE() AS CURR_ROLE, "
            "CURRENT_WAREHOUSE() AS CURR_WH, CURRENT_DATABASE() AS CURR_DB"
        )
        if rows:
            print("  ✅ Connection successful! Active session:")
            for k, v in rows[0].items():
                print(f"     - {k}: {v}")
        else:
            print("  ✅ Connection established successfully!")

        client.close()
        print("\n🎉 All checks passed! The adapter is ready to run.")
    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  - Ensure the public key (rsa_key.pub) is registered to the user in Snowflake:")
        print("    ALTER USER <user> SET RSA_PUBLIC_KEY='...';")
        print("  - Check network access / IP whitelisting in Snowflake.")
        sys.exit(1)


if __name__ == "__main__":
    main()
