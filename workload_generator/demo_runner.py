"""
Demo Runner — Full End-to-End Demo Orchestrator
=================================================
Runs the complete demo flow:

  1. Setup  — Create demo warehouses (if not exist)
  2. Scan   — Discover current environment
  3. Run    — Execute workload scenarios
  4. Verify — Check that queries appeared in ACCOUNT_USAGE
  5. Build  — Trigger dbt build to process new data
  6. Check  — Validate publication tables have new data

Usage:
    python demo_runner.py --step setup       # Just create warehouses
    python demo_runner.py --step run         # Run all workloads
    python demo_runner.py --step verify      # Check ACCOUNT_USAGE for recent queries
    python demo_runner.py --step all         # Full flow (setup + run + verify)
    python demo_runner.py --step quick-demo  # Quick: 3 key scenarios only
"""

import argparse
import os
import time
import snowflake.connector

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "..", "Connect-token-secret.txt")


def get_connection(warehouse="COST_OPT_WH"):
    token = open(TOKEN_FILE).read().strip()
    return snowflake.connector.connect(
        account="chc70950.us-east-1",
        user="SRINIVAS",
        role="ACCOUNTADMIN",
        authenticator="programmatic_access_token",
        token=token,
        warehouse=warehouse,
    )


def step_verify():
    """Check ACCOUNT_USAGE for recent workload queries."""
    print("\n" + "=" * 60)
    print("VERIFICATION: Checking ACCOUNT_USAGE for recent queries")
    print("=" * 60)

    conn = get_connection()
    cur = conn.cursor()

    # Check recent queries with our tags
    cur.execute("""
        SELECT
            query_tag,
            warehouse_name,
            execution_status,
            total_elapsed_time / 1000.0 as elapsed_sec,
            bytes_scanned,
            bytes_spilled_to_local_storage,
            bytes_spilled_to_remote_storage,
            partitions_scanned,
            partitions_total,
            rows_produced,
            credits_used_cloud_services,
            end_time
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_tag LIKE 'team:%scenario:%'
          AND start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
        ORDER BY end_time DESC
        LIMIT 50
    """)

    rows = cur.fetchall()
    if not rows:
        print("  No tagged queries found in ACCOUNT_USAGE yet.")
        print("  ACCOUNT_USAGE has up to 45-minute latency.")
        print("  Try again in a few minutes.")
    else:
        print(f"  Found {len(rows)} tagged queries:\n")
        for r in rows:
            tag = r[0] or ""
            # Extract scenario from tag
            scenario = ""
            for part in tag.split(";"):
                if part.startswith("scenario:"):
                    scenario = part.split(":")[1]
            spill = (r[5] or 0) + (r[6] or 0)
            scan_ratio = f"{r[7]}/{r[8]}" if r[7] and r[8] else "N/A"

            print(f"  {scenario:25s}  wh={r[1]:15s}  status={r[2]:8s}  "
                  f"time={r[3]:>6.1f}s  scan={scan_ratio:>12s}  "
                  f"spill={spill:>10,}  rows={r[9] or 0:>12,}")

    # Check anti-pattern indicators
    print("\n  Anti-Pattern Indicators:")
    cur.execute("""
        SELECT
            COUNT(CASE WHEN partitions_total > 100
                        AND partitions_scanned::FLOAT / NULLIF(partitions_total, 0) > 0.8
                       THEN 1 END) as full_scans,
            COUNT(CASE WHEN REGEXP_LIKE(UPPER(TRIM(query_text)), '^SELECT\\\\s+\\\\*\\\\s+FROM.*', 'i')
                       THEN 1 END) as select_stars,
            COUNT(CASE WHEN bytes_spilled_to_local_storage > 0
                        OR bytes_spilled_to_remote_storage > 0
                       THEN 1 END) as spills,
            COUNT(*) as total
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE query_tag LIKE 'team:%scenario:%'
          AND start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
          AND execution_status = 'SUCCESS'
    """)
    r = cur.fetchone()
    print(f"    Full table scans detected: {r[0]}")
    print(f"    SELECT * queries detected: {r[1]}")
    print(f"    Spill-to-storage queries:  {r[2]}")
    print(f"    Total tagged queries:      {r[3]}")

    # Check warehouse metering
    print("\n  Recent Warehouse Metering:")
    cur.execute("""
        SELECT warehouse_name, SUM(credits_used) as credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE start_time >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
        GROUP BY warehouse_name
        ORDER BY credits DESC
    """)
    for r in cur.fetchall():
        print(f"    {r[0]:25s}  credits={float(r[1]):>8.4f}")

    cur.close()
    conn.close()


def step_check_publication():
    """Check publication tables for data after dbt build."""
    print("\n" + "=" * 60)
    print("CHECKING PUBLICATION TABLES")
    print("=" * 60)

    conn = get_connection()
    cur = conn.cursor()

    tables = [
        "PUBLICATION.PUB__COST_SUMMARY",
        "PUBLICATION.PUB__COST_BY_WAREHOUSE",
        "PUBLICATION.PUB__COST_BY_USER",
        "PUBLICATION.PUB__WAREHOUSE_EFFICIENCY",
        "PUBLICATION.PUB__COST_TRENDS_DAILY",
        "PUBLICATION.PUB__ALL_RECOMMENDATIONS",
        "PUBLICATION.PUB__ANTIPATTERN_SUMMARY",
        "PUBLICATION.PUB__ALERT_HISTORY",
    ]

    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM COST_OPTIMIZATION_DB.{table}")
            count = cur.fetchone()[0]
            status = "OK" if count > 0 else "EMPTY"
            print(f"  [{status:5s}] {table:50s}  rows={count:>6,}")
        except Exception as e:
            print(f"  [ERROR] {table:50s}  {e}")

    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Demo Runner")
    parser.add_argument("--step", default="all",
                        choices=["setup", "scan", "run", "verify", "check", "all", "quick-demo"],
                        help="Which step to execute")
    args = parser.parse_args()

    if args.step in ("setup", "all"):
        print("Step 1: Setting up demo environment...")
        from setup_demo_environment import main as setup_main
        setup_main()

    if args.step in ("scan", "all"):
        print("\nStep 2: Scanning environment...")
        from scan_environment import main as scan_main
        scan_main()

    if args.step in ("run", "all"):
        print("\nStep 3: Running workloads...")
        from generate_workloads import main as workload_main
        import sys
        sys.argv = ["generate_workloads.py", "--scenario", "all"]
        workload_main()

    if args.step == "quick-demo":
        print("\nQuick Demo: Running 3 key scenarios...")
        from generate_workloads import main as workload_main
        import sys
        sys.argv = ["generate_workloads.py", "--scenario", "full_scan,repeated,multi_warehouse"]
        workload_main()

    if args.step in ("verify", "all"):
        step_verify()

    if args.step in ("check", "all"):
        step_check_publication()


if __name__ == "__main__":
    main()
