"""
Snowflake Environment Scanner
==============================
Scans the entire Snowflake account to discover:
  - All databases, schemas, tables (with row counts and sizes)
  - All warehouses (with sizes, auto-suspend, state)
  - All roles and users
  - Query history statistics
  - Metering and storage summaries

This replaces static seed files with live metadata discovery.
Run this before generating workloads to understand what's available.

Usage:
    python scan_environment.py
"""

import json
import os
import snowflake.connector
from datetime import datetime

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "..", "Connect-token-secret.txt")


def get_connection():
    token = open(TOKEN_FILE).read().strip()
    return snowflake.connector.connect(
        account="chc70950.us-east-1",
        user="SRINIVAS",
        role="ACCOUNTADMIN",
        authenticator="programmatic_access_token",
        token=token,
        warehouse="COST_OPT_WH",
    )


def scan_databases(cur):
    print("\n" + "=" * 70)
    print("DATABASES")
    print("=" * 70)
    cur.execute("SHOW DATABASES")
    cols = [d[0] for d in cur.description]
    databases = []
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        db = {
            "name": d["name"],
            "owner": d.get("owner", ""),
            "kind": d.get("kind", ""),
            "origin": d.get("origin", ""),
        }
        databases.append(db)
        kind_tag = f" [{db['kind']}]" if db["kind"] and db["kind"] != "STANDARD" else ""
        print(f"  {db['name']:35s}{kind_tag}")
    return databases


def scan_warehouses(cur):
    print("\n" + "=" * 70)
    print("WAREHOUSES")
    print("=" * 70)
    cur.execute("SHOW WAREHOUSES")
    cols = [d[0] for d in cur.description]
    warehouses = []
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        wh = {
            "name": d["name"],
            "size": d.get("size", "Unknown"),
            "type": d.get("type", "STANDARD"),
            "auto_suspend": d.get("auto_suspend", 600),
            "auto_resume": d.get("auto_resume", "true"),
            "state": d.get("state", "UNKNOWN"),
            "min_cluster_count": d.get("min_cluster_count", 1),
            "max_cluster_count": d.get("max_cluster_count", 1),
        }
        warehouses.append(wh)
        print(f"  {wh['name']:30s}  size={str(wh['size']):10s}  "
              f"auto_suspend={wh['auto_suspend']}s  state={wh['state']}")
    return warehouses


def scan_roles(cur):
    print("\n" + "=" * 70)
    print("ROLES")
    print("=" * 70)
    cur.execute("SHOW ROLES")
    roles = []
    for r in cur.fetchall():
        roles.append(r[1])
        print(f"  {r[1]}")
    return roles


def scan_users(cur):
    print("\n" + "=" * 70)
    print("USERS")
    print("=" * 70)
    cur.execute("SHOW USERS")
    users = []
    for r in cur.fetchall():
        users.append(r[0])
        print(f"  {r[0]}")
    return users


def scan_tables(cur, databases):
    print("\n" + "=" * 70)
    print("TABLES WITH DATA (top tables per database)")
    print("=" * 70)
    all_tables = []
    for db in databases:
        db_name = db["name"]
        if db_name in ("SNOWFLAKE",):
            continue
        try:
            cur.execute(f"""
                SELECT table_schema, table_name, row_count, bytes, table_type
                FROM {db_name}.INFORMATION_SCHEMA.TABLES
                WHERE table_type IN ('BASE TABLE', 'VIEW')
                  AND (row_count > 0 OR table_type = 'VIEW')
                ORDER BY bytes DESC NULLS LAST
                LIMIT 10
            """)
            rows = cur.fetchall()
            if rows:
                print(f"\n  {db_name}:")
                for r in rows:
                    gb = (r[3] or 0) / (1024 ** 3)
                    row_count = r[2] or 0
                    table_info = {
                        "database": db_name,
                        "schema": r[0],
                        "table": r[1],
                        "row_count": row_count,
                        "bytes": r[3] or 0,
                        "type": r[4],
                    }
                    all_tables.append(table_info)
                    print(f"    {r[0]:15s}.{r[1]:35s}  rows={row_count:>15,}  "
                          f"size={gb:>10.4f} GB  [{r[4]}]")
        except Exception:
            pass  # Skip databases we can't access
    return all_tables


def scan_query_history(cur):
    print("\n" + "=" * 70)
    print("QUERY HISTORY SUMMARY (last 90 days)")
    print("=" * 70)
    cur.execute("""
        SELECT
            COUNT(*) as total_queries,
            COUNT(DISTINCT user_name) as unique_users,
            COUNT(DISTINCT warehouse_name) as unique_warehouses,
            COUNT(DISTINCT role_name) as unique_roles,
            COUNT(DISTINCT database_name) as unique_databases,
            MIN(start_time) as earliest,
            MAX(start_time) as latest,
            SUM(credits_used_cloud_services) as total_cloud_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE start_time >= DATEADD('day', -90, CURRENT_DATE())
    """)
    r = cur.fetchone()
    stats = {
        "total_queries": r[0],
        "unique_users": r[1],
        "unique_warehouses": r[2],
        "unique_roles": r[3],
        "unique_databases": r[4],
        "earliest": str(r[5]),
        "latest": str(r[6]),
        "total_cloud_credits": float(r[7] or 0),
    }
    for k, v in stats.items():
        print(f"  {k:25s}: {v}")
    return stats


def scan_metering(cur):
    print("\n" + "=" * 70)
    print("WAREHOUSE METERING (all time)")
    print("=" * 70)
    cur.execute("""
        SELECT warehouse_name,
               COUNT(*) as intervals,
               SUM(credits_used) as total_credits,
               SUM(credits_used_compute) as compute_credits,
               SUM(credits_used_cloud_services) as cloud_credits,
               MIN(start_time) as first_metered,
               MAX(start_time) as last_metered
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        GROUP BY warehouse_name
        ORDER BY total_credits DESC
    """)
    metering = []
    for r in cur.fetchall():
        m = {
            "warehouse": r[0],
            "intervals": r[1],
            "total_credits": float(r[2]),
            "compute_credits": float(r[3]),
            "cloud_credits": float(r[4]),
            "first_metered": str(r[5]),
            "last_metered": str(r[6]),
        }
        metering.append(m)
        print(f"  {m['warehouse']:25s}  credits={m['total_credits']:>10.4f}  "
              f"compute={m['compute_credits']:>10.4f}  cloud={m['cloud_credits']:>10.4f}")
    return metering


def main():
    print(f"Snowflake Environment Scan — {datetime.now().isoformat()}")
    print("=" * 70)

    conn = get_connection()
    cur = conn.cursor()

    databases = scan_databases(cur)
    warehouses = scan_warehouses(cur)
    roles = scan_roles(cur)
    users = scan_users(cur)
    tables = scan_tables(cur, databases)
    query_stats = scan_query_history(cur)
    metering = scan_metering(cur)

    # Save full scan results as JSON for reference
    scan_result = {
        "scan_timestamp": datetime.now().isoformat(),
        "databases": databases,
        "warehouses": warehouses,
        "roles": roles,
        "users": users,
        "tables": tables,
        "query_stats": query_stats,
        "metering": metering,
    }

    output_file = os.path.join(os.path.dirname(__file__), "environment_scan.json")
    with open(output_file, "w") as f:
        json.dump(scan_result, f, indent=2, default=str)

    print(f"\n{'=' * 70}")
    print(f"Scan complete. Results saved to: {output_file}")
    print(f"  Databases: {len(databases)}")
    print(f"  Warehouses: {len(warehouses)}")
    print(f"  Roles: {len(roles)}")
    print(f"  Users: {len(users)}")
    print(f"  Tables with data: {len(tables)}")
    print(f"{'=' * 70}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
