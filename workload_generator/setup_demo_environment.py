"""
Demo Environment Setup
=======================
Creates additional warehouses and roles to enable multi-team cost attribution
during demos. This gives us realistic multi-warehouse/multi-role workloads.

Creates:
  - ANALYTICS_WH (Small)   — simulates analytics team running reports
  - ETL_WH       (Medium)  — simulates data engineering running transforms
  - Grants ACCOUNTADMIN usage on new warehouses

Usage:
    python setup_demo_environment.py
"""

import os
import snowflake.connector

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "..", "Connect-token-secret.txt")


def get_connection():
    token = open(TOKEN_FILE).read().strip()
    return snowflake.connector.connect(
        account="chc70950.us-east-1",
        user="SRINIVAS",
        role="ACCOUNTADMIN",
        authenticator="programmatic_access_token",
        token=token,
    )


SETUP_STATEMENTS = [
    # ── Additional warehouses for multi-team demo ──────────────────
    """CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
       WITH WAREHOUSE_SIZE = 'SMALL'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE
       COMMENT = 'Demo: Analytics team warehouse (Small) for cost attribution demo'""",

    """CREATE WAREHOUSE IF NOT EXISTS ETL_WH
       WITH WAREHOUSE_SIZE = 'MEDIUM'
       AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE
       INITIALLY_SUSPENDED = TRUE
       COMMENT = 'Demo: Data engineering warehouse (Medium) for cost attribution demo'""",

    # ── Schema for workload tracking ────────────────────────────────
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.WORKLOADS",

    # ── Table to log workload runs ──────────────────────────────────
    """CREATE TABLE IF NOT EXISTS COST_OPTIMIZATION_DB.WORKLOADS.WORKLOAD_LOG (
        run_id VARCHAR,
        scenario_name VARCHAR,
        warehouse_name VARCHAR,
        role_name VARCHAR,
        query_tag VARCHAR,
        started_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        completed_at TIMESTAMP_NTZ,
        status VARCHAR,
        query_id VARCHAR,
        notes VARCHAR
    )""",

    # ── Ensure COST_OPTIMIZATION_DB schemas exist ───────────────────
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.STAGING",
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.INTERMEDIATE",
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.PUBLICATION",
    "CREATE SCHEMA IF NOT EXISTS COST_OPTIMIZATION_DB.SEEDS",
]


def main():
    conn = get_connection()
    cur = conn.cursor()

    print("Setting up demo environment...")
    for sql in SETUP_STATEMENTS:
        label = sql.strip().split("\n")[0][:70]
        print(f"  Running: {label}...")
        try:
            cur.execute(sql)
            result = cur.fetchone()
            print(f"    -> {result[0] if result else 'OK'}")
        except Exception as e:
            print(f"    -> ERROR: {e}")

    # Verify warehouses
    print("\nVerifying warehouses:")
    cur.execute("SHOW WAREHOUSES")
    cols = [d[0] for d in cur.description]
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        print(f"  {d['name']:30s}  size={str(d.get('size', '?')):10s}  state={d.get('state', '?')}")

    cur.close()
    conn.close()
    print("\nDemo environment setup complete.")


if __name__ == "__main__":
    main()
