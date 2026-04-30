"""
Cost Optimization Dashboard — Deploy as Streamlit in Snowflake (SiS)

Uses Snowpark session to upload files directly to stage and create the
Streamlit app, bypassing `snow streamlit deploy` host format issues.

Usage:
    cd streamlit_app
    python deploy_sis.py
"""

import os
import sys
from pathlib import Path
from snowflake.snowpark import Session

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent
TOKEN_FILE = PROJECT_ROOT.parent / "Connect-token-secret.txt"

SNOWFLAKE_CONFIG = {
    "account": "chc70950.us-east-1",
    "user": "SRINIVAS",
    "role": "ACCOUNTADMIN",
    "warehouse": "COST_OPT_WH",
    "database": "COST_OPTIMIZATION_DB",
    "schema": "PUBLIC",
    "authenticator": "PROGRAMMATIC_ACCESS_TOKEN",
}

APP_NAME = "COST_OPTIMIZATION_DASHBOARD"
STAGE_NAME = "COST_OPTIMIZATION_DB.PUBLIC.STREAMLIT_STAGE"
STAGE_PREFIX = "cost_opt_app"

# Files/dirs to exclude from upload
EXCLUDE = {
    "__pycache__", ".git", ".venv", "venv",
    "deploy_sis.py", "snowflake.yml", "output",
}


def create_session():
    """Create a Snowpark session using programmatic access token."""
    token = TOKEN_FILE.read_text(encoding="utf-8").strip()
    config = {**SNOWFLAKE_CONFIG, "token": token}
    session = Session.builder.configs(config).create()
    print(f"  Connected as {config['user']} / {config['role']}")
    return session


def should_include(rel_path: str) -> bool:
    """Check whether a file should be uploaded."""
    parts = Path(rel_path).parts
    for part in parts:
        if part in EXCLUDE or part.startswith("."):
            return False
    ext = Path(rel_path).suffix
    return ext in (".py", ".yml", ".yaml", ".toml")


def deploy():
    print("=" * 60)
    print("Cost Optimization Dashboard — SiS Deployment")
    print("=" * 60)

    # Step 1: Connect
    print("\n[1/4] Connecting to Snowflake ...")
    session = create_session()

    try:
        # Step 2: Create stage if not exists
        print("\n[2/4] Preparing stage ...")
        session.sql(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}").collect()
        session.sql(f"REMOVE @{STAGE_NAME}/{STAGE_PREFIX}/").collect()
        print(f"  Stage ready: @{STAGE_NAME}/{STAGE_PREFIX}/")

        # Step 3: Upload files
        print("\n[3/4] Uploading files ...")
        uploaded = 0
        for root, dirs, files in os.walk(PROJECT_ROOT):
            dirs[:] = [d for d in dirs if d not in EXCLUDE and not d.startswith(".")]
            for fname in files:
                abs_path = Path(root) / fname
                rel_path = abs_path.relative_to(PROJECT_ROOT).as_posix()
                if not should_include(rel_path):
                    continue

                stage_dir = f"@{STAGE_NAME}/{STAGE_PREFIX}/{Path(rel_path).parent.as_posix()}"
                stage_dir = stage_dir.rstrip("/.")

                put_sql = (
                    f"PUT 'file://{abs_path.as_posix()}' '{stage_dir}/' "
                    f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                session.sql(put_sql).collect()
                uploaded += 1
                print(f"  [upload] {rel_path}")

        print(f"\n  Uploaded {uploaded} files")

        # Step 4: Create Streamlit app
        print("\n[4/4] Creating Streamlit app ...")
        fqn = f"COST_OPTIMIZATION_DB.PUBLIC.{APP_NAME}"
        sql = f"""
        CREATE OR REPLACE STREAMLIT {fqn}
            ROOT_LOCATION = '@{STAGE_NAME}/{STAGE_PREFIX}'
            MAIN_FILE = 'app.py'
            QUERY_WAREHOUSE = 'COST_OPT_WH'
            TITLE = 'Cost Optimization Dashboard'
            COMMENT = 'Snowflake Cost Optimization Framework — interactive dashboard'
        """
        session.sql(sql).collect()
        print(f"  Streamlit app created: {fqn}")

        # Verify
        print("\n--- Verification ---")
        rows = session.sql("SHOW STREAMLITS IN COST_OPTIMIZATION_DB.PUBLIC").collect()
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
    deploy()
