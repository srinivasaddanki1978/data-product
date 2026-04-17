"""
Workload Generator for Cost Optimization Framework Demo
=========================================================
Generates intentional Snowflake workloads that trigger every detection
mechanism in the framework:

  1. FULL_TABLE_SCAN   — scans all partitions on large TPC-DS tables
  2. SELECT_STAR       — SELECT * on large tables
  3. SPILL_TO_STORAGE  — complex ops on X-Small warehouse → memory spill
  4. REPEATED_QUERIES  — same parameterized query run 25+ times
  5. CARTESIAN_JOIN    — cross join producing millions of rows
  6. LARGE_SORT        — ORDER BY on millions of rows without LIMIT
  7. EXPENSIVE_QUERY   — multi-table TPC-DS joins costing real credits
  8. MULTI_WAREHOUSE   — distribute load across COMPUTE/ANALYTICS/ETL WHs
  9. COST_SPIKE        — burst of expensive queries to trigger cost anomaly
  10. IDLE_WAREHOUSE   — resume warehouse, run nothing, let it idle

Each scenario uses query_tag for attribution tracking:
    query_tag = 'team:{team};scenario:{scenario};run_id:{id}'

The framework's dbt models then detect these patterns from ACCOUNT_USAGE.

Usage:
    python generate_workloads.py --scenario all          # Run all scenarios
    python generate_workloads.py --scenario full_scan     # Run one scenario
    python generate_workloads.py --scenario repeated      # Run repeated queries
    python generate_workloads.py --list                   # List available scenarios
"""

import argparse
import os
import time
import uuid
import snowflake.connector

TOKEN_FILE = os.path.join(os.path.dirname(__file__), "..", "Connect-token-secret.txt")

# ── TPC-DS table references (IMPORTED shared database) ────────────
TPCDS = "TPCDS_10TB.TPCDS_SF10TCL"
STORE_SALES = f"{TPCDS}.STORE_SALES"          # 28.8B rows, 1.17 TB
CATALOG_SALES = f"{TPCDS}.CATALOG_SALES"      # 14.4B rows, 871 GB
WEB_SALES = f"{TPCDS}.WEB_SALES"              # 7.2B rows, 435 GB
CUSTOMER = f"{TPCDS}.CUSTOMER"                 # 65M rows, 2.17 GB
ITEM = f"{TPCDS}.ITEM"                         # 402K rows
DATE_DIM = f"{TPCDS}.DATE_DIM"                # 73K rows
STORE = f"{TPCDS}.STORE"                       # 1.5K rows
INVENTORY = f"{TPCDS}.INVENTORY"               # 1.3B rows, 3.4 GB

# ── Smaller tables for safer demos ────────────────────────────────
BRAZIL = "BRAZIL_ECOMMERCE_DB.PUBLIC"
GLOBALMART = "GLOBALMART"


def get_connection(warehouse="COMPUTE_WH"):
    token = open(TOKEN_FILE).read().strip()
    return snowflake.connector.connect(
        account="chc70950.us-east-1",
        user="SRINIVAS",
        role="ACCOUNTADMIN",
        authenticator="programmatic_access_token",
        token=token,
        warehouse=warehouse,
    )


def run_query(cur, sql, tag, log_cur=None, run_id=None, scenario=None, warehouse=None):
    """Execute a query with a tracking tag and optional logging."""
    cur.execute(f"ALTER SESSION SET QUERY_TAG = '{tag}'")
    print(f"  [{tag}] Executing...")
    start = time.time()
    try:
        cur.execute(sql)
        row = cur.fetchone()
        elapsed = time.time() - start
        qid = cur.sfqid
        print(f"    -> OK ({elapsed:.1f}s) query_id={qid}")
        if log_cur and run_id:
            log_cur.execute(f"""
                INSERT INTO COST_OPTIMIZATION_DB.WORKLOADS.WORKLOAD_LOG
                (run_id, scenario_name, warehouse_name, role_name, query_tag, completed_at, status, query_id)
                VALUES ('{run_id}', '{scenario}', '{warehouse}', 'ACCOUNTADMIN', '{tag}',
                        CURRENT_TIMESTAMP(), 'SUCCESS', '{qid}')
            """)
        return qid
    except Exception as e:
        elapsed = time.time() - start
        print(f"    -> FAILED ({elapsed:.1f}s): {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# SCENARIO DEFINITIONS
# ═══════════════════════════════════════════════════════════════════

def scenario_full_table_scan(run_id):
    """
    FULL TABLE SCAN: Scan a large percentage of partitions on TPC-DS tables.
    Triggers: int__antipattern_full_table_scan (partitions_scanned/total > 0.8)
    Warehouse: COMPUTE_WH (X-Small) — keeps cost low, scan ratio high.
    """
    print("\n--- Scenario: FULL TABLE SCAN ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        # Scan all of INVENTORY (1.3B rows, 3.4 GB) — full scan, moderate cost
        f"""SELECT inv_warehouse_sk, COUNT(*) as cnt, SUM(inv_quantity_on_hand) as total_qty
            FROM {INVENTORY}
            GROUP BY inv_warehouse_sk
            ORDER BY total_qty DESC""",

        # Scan CUSTOMER with non-selective filter — will scan most partitions
        f"""SELECT c_birth_country, COUNT(*) as customer_count, AVG(c_birth_year) as avg_birth_year
            FROM {CUSTOMER}
            WHERE c_birth_year > 1950
            GROUP BY c_birth_country
            ORDER BY customer_count DESC""",

        # Scan ITEM fully
        f"""SELECT i_category, i_class, COUNT(*) as item_count,
                   AVG(i_current_price) as avg_price
            FROM {ITEM}
            GROUP BY i_category, i_class
            ORDER BY item_count DESC""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:analytics;scenario:full_table_scan;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "full_table_scan", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_select_star(run_id):
    """
    SELECT *: Queries using SELECT * that scan unnecessary columns.
    Triggers: int__antipattern_select_star (regex match on SELECT *)
    """
    print("\n--- Scenario: SELECT * ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        f"SELECT * FROM {CUSTOMER} LIMIT 500000",
        f"SELECT * FROM {ITEM} LIMIT 100000",
        f"SELECT * FROM {BRAZIL}.ORDERS LIMIT 50000",
        f"SELECT * FROM {BRAZIL}.GEOLOCATION LIMIT 100000",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:analytics;scenario:select_star;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "select_star", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_spill_to_storage(run_id):
    """
    SPILL TO STORAGE: Run memory-intensive operations on X-Small warehouse.
    The small warehouse memory will force spill to local/remote storage.
    Triggers: int__antipattern_spill_to_storage (bytes_spilled > 0)
    """
    print("\n--- Scenario: SPILL TO STORAGE ---")
    conn = get_connection("COMPUTE_WH")  # X-Small → will spill
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        # Window function over large dataset — will spill on X-Small
        f"""SELECT inv_item_sk, inv_warehouse_sk, inv_date_sk,
                   SUM(inv_quantity_on_hand) OVER (
                       PARTITION BY inv_warehouse_sk
                       ORDER BY inv_date_sk
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) as running_qty
            FROM {INVENTORY}
            WHERE inv_date_sk BETWEEN 2451911 AND 2451941
            ORDER BY running_qty DESC
            LIMIT 1000""",

        # Large GROUP BY with many distinct values
        f"""SELECT c_customer_sk, c_first_name, c_last_name,
                   c_birth_country, c_birth_year,
                   COUNT(*) OVER (PARTITION BY c_birth_country) as country_count
            FROM {CUSTOMER}
            ORDER BY country_count DESC, c_customer_sk
            LIMIT 1000""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:data_engineering;scenario:spill_to_storage;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "spill_to_storage", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_repeated_queries(run_id):
    """
    REPEATED QUERIES: Run the same parameterized query 25 times.
    Triggers: int__antipattern_repeated_queries (same hash > 10 times/day)
    Also triggers: int__alert_repeated_expensive if cost > $1 each
    """
    print("\n--- Scenario: REPEATED QUERIES (25 iterations) ---")
    conn = get_connection("ANALYTICS_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    # Same query structure, different date parameter → same parameterized hash
    base_sql = f"""
        SELECT i_category, COUNT(*) as sale_count,
               SUM(inv_quantity_on_hand) as total_qty,
               AVG(inv_quantity_on_hand) as avg_qty
        FROM {INVENTORY}
        JOIN {ITEM} ON inv_item_sk = i_item_sk
        WHERE inv_date_sk = {{date_sk}}
        GROUP BY i_category
        ORDER BY sale_count DESC
    """

    # Run 25 times with different date_sk values
    for i in range(25):
        date_sk = 2451911 + i  # Different dates, same query structure
        sql = base_sql.format(date_sk=date_sk)
        tag = f"team:analytics;scenario:repeated_query;run_id:{run_id};iteration:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "repeated_query", "ANALYTICS_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_cartesian_join(run_id):
    """
    CARTESIAN JOIN: Cross join producing disproportionately many rows.
    Triggers: int__antipattern_cartesian_join (rows_produced >> rows_scanned)
    """
    print("\n--- Scenario: CARTESIAN JOIN ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        # STORE (1.5K) × DATE_DIM filtered (365) = ~547K rows — safe but detectable
        f"""SELECT s.s_store_name, d.d_date, d.d_day_name
            FROM {STORE} s, {DATE_DIM} d
            WHERE d.d_year = 2000
            AND s.s_state = 'TN'""",

        # PROMOTION (2K) × HOUSEHOLD_DEMOGRAPHICS (7.2K) = ~14.4M rows
        f"""SELECT p.p_promo_name, hd.hd_income_band_sk, hd.hd_buy_potential
            FROM {TPCDS}.PROMOTION p, {TPCDS}.HOUSEHOLD_DEMOGRAPHICS hd
            WHERE p.p_channel_tv = 'Y'""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:analytics;scenario:cartesian_join;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "cartesian_join", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_large_sort_no_limit(run_id):
    """
    LARGE SORT WITHOUT LIMIT: ORDER BY on millions of rows, no LIMIT clause.
    Triggers: int__antipattern_large_sort_no_limit (ORDER BY, rows > 100K, no LIMIT)
    """
    print("\n--- Scenario: LARGE SORT NO LIMIT ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        # Sort all 402K items — no LIMIT
        f"""SELECT i_item_sk, i_item_id, i_product_name, i_current_price, i_category
            FROM {ITEM}
            ORDER BY i_current_price DESC, i_item_id""",

        # Sort 1M+ geolocation rows — no LIMIT
        f"""SELECT * FROM {BRAZIL}.GEOLOCATION
            ORDER BY geolocation_lat, geolocation_lng""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:data_engineering;scenario:large_sort_no_limit;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "large_sort_no_limit", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_expensive_multi_join(run_id):
    """
    EXPENSIVE MULTI-TABLE JOIN: Real TPC-DS analytics query.
    Generates significant cost to show up in cost attribution.
    Runs on ANALYTICS_WH (Small) and ETL_WH (Medium) for comparison.
    """
    print("\n--- Scenario: EXPENSIVE MULTI-TABLE JOIN ---")

    # Same query, different warehouses → shows multi-warehouse attribution
    query = f"""
        SELECT d.d_year, d.d_quarter_name, i.i_category, i.i_class,
               COUNT(*) as sale_count,
               SUM(inv.inv_quantity_on_hand) as total_inventory,
               AVG(inv.inv_quantity_on_hand) as avg_inventory
        FROM {INVENTORY} inv
        JOIN {DATE_DIM} d ON inv.inv_date_sk = d.d_date_sk
        JOIN {ITEM} i ON inv.inv_item_sk = i.i_item_sk
        WHERE d.d_year = 2000
        GROUP BY d.d_year, d.d_quarter_name, i.i_category, i.i_class
        ORDER BY total_inventory DESC
    """

    for wh, team in [("ANALYTICS_WH", "analytics"), ("ETL_WH", "data_engineering")]:
        print(f"\n  Running on {wh}...")
        conn = get_connection(wh)
        cur = conn.cursor()
        log_cur = conn.cursor()
        tag = f"team:{team};scenario:expensive_join;run_id:{run_id};warehouse:{wh}"
        run_query(cur, query, tag, log_cur, run_id, "expensive_join", wh)
        cur.close()
        log_cur.close()
        conn.close()


def scenario_cost_spike(run_id):
    """
    COST SPIKE: Burst of expensive queries to trigger cost anomaly detection.
    Triggers: int__alert_cost_daily_spike (daily cost > 2x 30-day avg)
    Runs multiple expensive queries in rapid succession.
    """
    print("\n--- Scenario: COST SPIKE (burst of expensive queries) ---")
    conn = get_connection("ANALYTICS_WH")  # Small warehouse → 2 credits/hr
    cur = conn.cursor()
    log_cur = conn.cursor()

    spike_queries = [
        f"""SELECT d.d_year, d.d_month_seq,
                   COUNT(*) as inv_records,
                   SUM(inv_quantity_on_hand) as total_qty,
                   AVG(inv_quantity_on_hand) as avg_qty
            FROM {INVENTORY} inv
            JOIN {DATE_DIM} d ON inv.inv_date_sk = d.d_date_sk
            GROUP BY d.d_year, d.d_month_seq
            ORDER BY d.d_year, d.d_month_seq""",

        f"""SELECT i.i_category, i.i_brand,
                   COUNT(DISTINCT inv.inv_warehouse_sk) as warehouses,
                   SUM(inv.inv_quantity_on_hand) as total_qty
            FROM {INVENTORY} inv
            JOIN {ITEM} i ON inv.inv_item_sk = i.i_item_sk
            GROUP BY i.i_category, i.i_brand
            HAVING total_qty > 100000
            ORDER BY total_qty DESC""",

        f"""SELECT c.c_birth_country, c.c_birth_year,
                   COUNT(*) as customer_count
            FROM {CUSTOMER} c
            GROUP BY c.c_birth_country, c.c_birth_year
            ORDER BY customer_count DESC""",

        f"""SELECT ca.ca_state, ca.ca_city, COUNT(*) as address_count
            FROM {TPCDS}.CUSTOMER_ADDRESS ca
            GROUP BY ca.ca_state, ca.ca_city
            ORDER BY address_count DESC""",
    ]

    for i, sql in enumerate(spike_queries):
        tag = f"team:analytics;scenario:cost_spike;run_id:{run_id};burst:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "cost_spike", "ANALYTICS_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_multi_warehouse(run_id):
    """
    MULTI-WAREHOUSE LOAD: Same queries on different warehouses.
    Shows cost attribution differences between warehouse sizes.
    COMPUTE_WH=XS (1 credit/hr), ANALYTICS_WH=Small (2), ETL_WH=Medium (4)
    """
    print("\n--- Scenario: MULTI-WAREHOUSE ATTRIBUTION ---")

    query = f"""
        SELECT i_category, i_class,
               COUNT(*) as item_count,
               AVG(i_current_price) as avg_price,
               MAX(i_current_price) as max_price,
               MIN(i_current_price) as min_price
        FROM {ITEM}
        GROUP BY i_category, i_class
        ORDER BY item_count DESC
    """

    for wh, team in [
        ("COMPUTE_WH", "platform"),
        ("ANALYTICS_WH", "analytics"),
        ("ETL_WH", "data_engineering"),
    ]:
        print(f"\n  Running on {wh} (team={team})...")
        conn = get_connection(wh)
        cur = conn.cursor()
        log_cur = conn.cursor()
        tag = f"team:{team};scenario:multi_warehouse;run_id:{run_id};warehouse:{wh}"
        run_query(cur, query, tag, log_cur, run_id, "multi_warehouse", wh)
        cur.close()
        log_cur.close()
        conn.close()


def scenario_idle_warehouse(run_id):
    """
    IDLE WAREHOUSE: Resume a warehouse, don't run anything, let it idle.
    Triggers: int__alert_warehouse_idle (running with 0 queries > 30 min)
    Note: Auto-suspend will eventually suspend it, but the idle period is tracked.
    """
    print("\n--- Scenario: IDLE WAREHOUSE ---")
    conn = get_connection("ETL_WH")
    cur = conn.cursor()

    # Resume the warehouse (creates metering entry) then run a trivial query
    tag = f"team:data_engineering;scenario:idle_warehouse;run_id:{run_id}"
    cur.execute(f"ALTER SESSION SET QUERY_TAG = '{tag}'")
    cur.execute("SELECT 1")  # Minimal query just to resume the warehouse
    print(f"  ETL_WH resumed. It will idle until auto-suspend ({60}s).")
    print("  The idle period will be detected by int__idle_warehouse_periods.")

    cur.close()
    conn.close()


# ═══════════════════════════════════════════════════════════════════
# SCENARIO REGISTRY
# ═══════════════════════════════════════════════════════════════════

SCENARIOS = {
    "full_scan":      ("Full Table Scan",         scenario_full_table_scan),
    "select_star":    ("SELECT * Anti-Pattern",   scenario_select_star),
    "spill":          ("Spill to Storage",        scenario_spill_to_storage),
    "repeated":       ("Repeated Queries (25x)",  scenario_repeated_queries),
    "cartesian":      ("Cartesian Join",          scenario_cartesian_join),
    "large_sort":     ("Large Sort No LIMIT",     scenario_large_sort_no_limit),
    "expensive_join": ("Expensive Multi-Join",    scenario_expensive_multi_join),
    "cost_spike":     ("Cost Spike Burst",        scenario_cost_spike),
    "multi_warehouse":("Multi-Warehouse Load",    scenario_multi_warehouse),
    "idle":           ("Idle Warehouse",          scenario_idle_warehouse),
}


def main():
    parser = argparse.ArgumentParser(description="Generate Snowflake workloads for demo")
    parser.add_argument("--scenario", default="all",
                        help="Scenario to run (or 'all'). Use --list to see options.")
    parser.add_argument("--list", action="store_true", help="List available scenarios")
    args = parser.parse_args()

    if args.list:
        print("Available scenarios:")
        for key, (desc, _) in SCENARIOS.items():
            print(f"  {key:20s} — {desc}")
        return

    run_id = str(uuid.uuid4())[:8]
    print(f"Workload Run ID: {run_id}")
    print(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    if args.scenario == "all":
        scenarios_to_run = list(SCENARIOS.keys())
    else:
        scenarios_to_run = [s.strip() for s in args.scenario.split(",")]

    for scenario_key in scenarios_to_run:
        if scenario_key not in SCENARIOS:
            print(f"Unknown scenario: {scenario_key}")
            continue
        desc, func = SCENARIOS[scenario_key]
        print(f"\n{'=' * 60}")
        print(f"Running: {desc} ({scenario_key})")
        print(f"{'=' * 60}")
        try:
            func(run_id)
        except Exception as e:
            print(f"  ERROR in {scenario_key}: {e}")

    print(f"\n{'=' * 60}")
    print(f"All workloads complete. Run ID: {run_id}")
    print(f"")
    print(f"IMPORTANT: ACCOUNT_USAGE has up to 45-minute latency.")
    print(f"Wait 45 minutes, then run:")
    print(f"  snow dbt execute cost_optimization build")
    print(f"")
    print(f"Or check workload log immediately:")
    print(f"  SELECT * FROM COST_OPTIMIZATION_DB.WORKLOADS.WORKLOAD_LOG")
    print(f"  WHERE run_id = '{run_id}' ORDER BY started_at;")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
