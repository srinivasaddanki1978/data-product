"""
Cost Optimization Dashboard — Deploy as Streamlit in Snowflake (SiS)

Adapted from the Tensor project's deploy_sis.py pattern. Uses Snowpark session
to upload files directly to stage and create the Streamlit app, bypassing
`snow streamlit deploy` which has host format issues with account IDs like
`chc70950.us-east-1`.

Usage:
    cd streamlit_app
    python deploy_sis.py

    # Or with explicit connection name:
    python deploy_sis.py --connection cost_optimization

Requires:
    - snowflake-snowpark-python
    - Connection `cost_optimization` in ~/.snowflake/connections.toml
"""

import argparse
import datetime
import os
import sys
from pathlib import Path

from snowflake.snowpark import Session

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent
DEFAULT_CONNECTION = "cost_optimization"

APP_NAME = "COST_OPTIMIZATION_DASHBOARD"
APP_DATABASE = "COST_OPTIMIZATION_DB"
APP_SCHEMA = "PUBLIC"
STAGE_NAME = f"{APP_DATABASE}.{APP_SCHEMA}.STREAMLIT_STAGE"
STAGE_PREFIX = "cost_opt_app"

# Files/dirs to exclude from upload
EXCLUDE = {
    "__pycache__", ".git", ".venv", "venv", "output",
    "deploy_sis.py", "snowflake.yml",
}


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _read_connections_toml():
    """Parse ~/.snowflake/connections.toml and return as nested dict."""
    toml_path = Path.home() / ".snowflake" / "connections.toml"
    if not toml_path.exists():
        return {}

    connections = {}
    current_section = None

    for line in toml_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current_section = line[1:-1].strip()
            connections[current_section] = {}
        elif "=" in line and current_section:
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            connections[current_section][key] = value

    return connections


def create_session(connection_name: str):
    """Create a Snowpark session from ~/.snowflake/connections.toml."""
    connections = _read_connections_toml()

    if connection_name not in connections:
        print(f"  ERROR: Connection '{connection_name}' not found in connections.toml")
        print(f"  Available connections: {', '.join(connections.keys())}")
        sys.exit(1)

    conn = connections[connection_name]
    config = {
        "account": conn["account"],
        "user": conn["user"],
        "role": conn.get("role", "ACCOUNTADMIN"),
        "warehouse": conn.get("warehouse", "COST_OPT_WH"),
        "database": conn.get("database", APP_DATABASE),
        "schema": conn.get("schema", APP_SCHEMA),
    }

    # Handle authentication methods
    authenticator = conn.get("authenticator", "").upper()

    if authenticator == "PROGRAMMATIC_ACCESS_TOKEN":
        config["authenticator"] = "PROGRAMMATIC_ACCESS_TOKEN"
        # Token can be inline in toml or in a file
        if "token" in conn:
            config["token"] = conn["token"]
        else:
            token_file = PROJECT_ROOT.parent / "Connect-token-secret.txt"
            if token_file.exists():
                config["token"] = token_file.read_text(encoding="utf-8").strip()
            else:
                print("  ERROR: No token found in connections.toml or Connect-token-secret.txt")
                sys.exit(1)

    elif authenticator == "SNOWFLAKE_JWT":
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend

        key_path = conn.get("private_key_file", "")
        if not key_path or not Path(key_path).exists():
            print(f"  ERROR: Private key file not found: {key_path}")
            sys.exit(1)

        password = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSWORD", "")
        with open(key_path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(),
                password=password.encode() if password else None,
                backend=default_backend(),
            )
        config["authenticator"] = "SNOWFLAKE_JWT"
        config["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    elif authenticator == "EXTERNALBROWSER":
        config["authenticator"] = "externalbrowser"

    else:
        # Password-based or other authenticator
        if "password" in conn:
            config["password"] = conn["password"]
        elif authenticator:
            config["authenticator"] = authenticator

    session = Session.builder.configs(config).create()
    print(f"  Connected as {config['user']} / {config.get('role', 'N/A')}")
    return session


# ---------------------------------------------------------------------------
# File handling
# ---------------------------------------------------------------------------

def should_include(rel_path: str) -> bool:
    """Check whether a file should be uploaded."""
    parts = Path(rel_path).parts
    for part in parts:
        if part in EXCLUDE or part.startswith("."):
            return False
    ext = Path(rel_path).suffix
    return ext in (".py", ".yml", ".yaml", ".toml")


def upload_files(session, deploy_ts: str):
    """Upload all app files to stage with cache-busting timestamps."""
    # Clear existing files
    print(f"  Clearing @{STAGE_NAME}/{STAGE_PREFIX}/ ...")
    session.sql(f"REMOVE @{STAGE_NAME}/{STAGE_PREFIX}/").collect()

    uploaded = 0
    for root, dirs, files in os.walk(PROJECT_ROOT):
        dirs[:] = [d for d in dirs if d not in EXCLUDE and not d.startswith(".")]
        for fname in files:
            abs_path = Path(root) / fname
            rel_path = abs_path.relative_to(PROJECT_ROOT).as_posix()
            if not should_include(rel_path):
                continue

            # For .py files, inject deploy timestamp to bust SiS module cache
            if fname.endswith(".py"):
                content = abs_path.read_text(encoding="utf-8")
                content = f"# _deploy_ts={deploy_ts}\n" + content

                # Write to a temp file for PUT
                import tempfile
                tmp = Path(tempfile.mktemp(suffix=f"_{fname}"))
                tmp.write_text(content, encoding="utf-8")
                upload_path = tmp
            else:
                upload_path = abs_path

            stage_dir = f"@{STAGE_NAME}/{STAGE_PREFIX}/{Path(rel_path).parent.as_posix()}"
            stage_dir = stage_dir.rstrip("/.")

            put_sql = (
                f"PUT 'file://{upload_path.as_posix()}' '{stage_dir}/' "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            session.sql(put_sql).collect()
            uploaded += 1
            print(f"  [upload] {rel_path}")

            # Clean up temp file
            if fname.endswith(".py") and upload_path != abs_path:
                upload_path.unlink(missing_ok=True)

    return uploaded


# ---------------------------------------------------------------------------
# Main deployment
# ---------------------------------------------------------------------------

def deploy(connection_name: str):
    deploy_ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    print("=" * 60)
    print("Cost Optimization Dashboard — SiS Deployment")
    print(f"  Timestamp: {deploy_ts}")
    print("=" * 60)

    # Step 1: Connect
    print(f"\n[1/4] Connecting to Snowflake (connection: {connection_name}) ...")
    session = create_session(connection_name)

    try:
        # Step 2: Create stage if not exists
        print("\n[2/4] Preparing stage ...")
        session.sql(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}").collect()
        print(f"  Stage ready: @{STAGE_NAME}/{STAGE_PREFIX}/")

        # Step 3: Upload files with cache-busting
        print("\n[3/4] Uploading files ...")
        uploaded = upload_files(session, deploy_ts)
        print(f"\n  Uploaded {uploaded} files")

        # Step 4: Create Streamlit app
        print("\n[4/4] Creating Streamlit app ...")
        fqn = f"{APP_DATABASE}.{APP_SCHEMA}.{APP_NAME}"
        sql = f"""
        CREATE OR REPLACE STREAMLIT {fqn}
            ROOT_LOCATION = '@{STAGE_NAME}/{STAGE_PREFIX}'
            MAIN_FILE = 'app.py'
            QUERY_WAREHOUSE = 'COST_OPT_WH'
            TITLE = 'Cost Optimization Dashboard'
            COMMENT = 'Snowflake Cost Optimization Framework — deployed {deploy_ts}'
        """
        session.sql(sql).collect()
        print(f"  Streamlit app created: {fqn}")

        # Verify
        print("\n--- Verification ---")
        rows = session.sql(f"SHOW STREAMLITS IN {APP_DATABASE}.{APP_SCHEMA}").collect()
        found = [r for r in rows if r["name"] == APP_NAME]
        if found:
            print(f"  App found: {found[0]['name']}  (owner: {found[0]['owner']})")
        else:
            print("  WARNING: App NOT found!")

        files = session.sql(f"LIST @{STAGE_NAME}/{STAGE_PREFIX}/").collect()
        print(f"  Files on stage: {len(files)}")
        for f in files:
            print(f"    {f['name']}")

    finally:
        session.close()

    print("\n" + "=" * 60)
    print("Deployment complete!")
    print(f"Open Snowsight -> Streamlit Apps -> {APP_NAME}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deploy Cost Optimization Dashboard to Snowflake")
    parser.add_argument(
        "--connection", "-c",
        default=DEFAULT_CONNECTION,
        help=f"Connection name from ~/.snowflake/connections.toml (default: {DEFAULT_CONNECTION})",
    )
    args = parser.parse_args()
    deploy(args.connection)
