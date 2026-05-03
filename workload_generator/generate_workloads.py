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

CUSTOMER_ADDRESS = f"{TPCDS}.CUSTOMER_ADDRESS"  # 32.5M rows
CUSTOMER_DEMOGRAPHICS = f"{TPCDS}.CUSTOMER_DEMOGRAPHICS"  # 1.9M rows
HOUSEHOLD_DEMOGRAPHICS = f"{TPCDS}.HOUSEHOLD_DEMOGRAPHICS"  # 7.2K rows
INCOME_BAND = f"{TPCDS}.INCOME_BAND"            # 20 rows
PROMOTION = f"{TPCDS}.PROMOTION"                 # 2K rows
WAREHOUSE = f"{TPCDS}.WAREHOUSE"                 # 20 rows

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
    FULL TABLE SCAN — "Inventory Health Scorecard"
    Triggers: int__antipattern_full_table_scan (partitions_scanned/total > 0.80)
    Warehouse: COMPUTE_WH (X-Small) — keeps cost low, scan ratio high.

    Query 1: Supply chain inventory health with CTEs, window functions, CASE
             health scoring. Joins INVENTORY (1.3B) + WAREHOUSE + ITEM + DATE_DIM
             with NO date filter → scans all partitions.
    Query 2: Customer demographic segmentation across 65M rows with broad filter.
    """
    print("\n--- Scenario: FULL TABLE SCAN (Inventory Health Scorecard) ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        # Query 1 — Supply chain inventory health scorecard (broad filter → >80% scan)
        f"""WITH warehouse_inventory AS (
                SELECT
                    w.w_warehouse_name,
                    i.i_category,
                    d.d_quarter_name,
                    SUM(inv.inv_quantity_on_hand)                           AS total_qty,
                    AVG(inv.inv_quantity_on_hand)                           AS avg_qty,
                    STDDEV(inv.inv_quantity_on_hand)                        AS qty_volatility,
                    COUNT(CASE WHEN inv.inv_quantity_on_hand = 0 THEN 1 END) AS stockout_days,
                    COUNT(*)                                               AS total_records,
                    ROW_NUMBER() OVER (
                        PARTITION BY w.w_warehouse_name, i.i_category
                        ORDER BY d.d_quarter_name DESC
                    ) AS quarter_rank
                FROM {INVENTORY} inv
                JOIN {WAREHOUSE} w   ON inv.inv_warehouse_sk  = w.w_warehouse_sk
                JOIN {ITEM} i        ON inv.inv_item_sk       = i.i_item_sk
                JOIN {DATE_DIM} d    ON inv.inv_date_sk       = d.d_date_sk
                WHERE d.d_year IN (2000, 2001, 2002)
                GROUP BY w.w_warehouse_name, i.i_category, d.d_quarter_name
            )
            SELECT
                w_warehouse_name,
                i_category,
                d_quarter_name,
                total_qty,
                avg_qty,
                qty_volatility,
                stockout_days,
                total_records,
                ROUND(stockout_days * 100.0 / NULLIF(total_records, 0), 2) AS stockout_rate_pct,
                CASE
                    WHEN stockout_days * 100.0 / NULLIF(total_records, 0) > 20 THEN 'CRITICAL'
                    WHEN stockout_days * 100.0 / NULLIF(total_records, 0) > 10 THEN 'AT_RISK'
                    WHEN qty_volatility > avg_qty * 0.5                        THEN 'VOLATILE'
                    ELSE 'HEALTHY'
                END AS inventory_health_status
            FROM warehouse_inventory
            WHERE quarter_rank <= 4
            ORDER BY stockout_rate_pct DESC, total_qty DESC""",

        # Query 2 — Customer demographic segmentation (broad filter → scans most partitions)
        f"""WITH customer_profiles AS (
                SELECT
                    c.c_customer_sk,
                    c.c_first_name,
                    c.c_last_name,
                    c.c_birth_year,
                    c.c_birth_country,
                    ca.ca_state,
                    ca.ca_zip,
                    cd.cd_gender,
                    cd.cd_education_status,
                    cd.cd_credit_rating,
                    hd.hd_buy_potential,
                    ib.ib_lower_bound                                       AS income_lower,
                    ib.ib_upper_bound                                       AS income_upper,
                    CASE
                        WHEN c.c_birth_year >= 1997 THEN 'Gen Z'
                        WHEN c.c_birth_year >= 1981 THEN 'Millennial'
                        WHEN c.c_birth_year >= 1965 THEN 'Gen X'
                        WHEN c.c_birth_year >= 1946 THEN 'Boomer'
                        ELSE 'Silent'
                    END AS generation,
                    NTILE(10) OVER (ORDER BY ib.ib_upper_bound)            AS income_decile
                FROM {CUSTOMER} c
                LEFT JOIN {CUSTOMER_ADDRESS} ca             ON c.c_current_addr_sk     = ca.ca_address_sk
                LEFT JOIN {CUSTOMER_DEMOGRAPHICS} cd        ON c.c_current_cdemo_sk    = cd.cd_demo_sk
                LEFT JOIN {HOUSEHOLD_DEMOGRAPHICS} hd       ON c.c_current_hdemo_sk    = hd.hd_demo_sk
                LEFT JOIN {INCOME_BAND} ib                  ON hd.hd_income_band_sk    = ib.ib_income_band_sk
                WHERE c.c_birth_year > 1940
            )
            SELECT
                generation,
                ca_state,
                cd_gender,
                cd_education_status,
                income_decile,
                COUNT(*)                                                   AS customer_count,
                ROUND(AVG(income_upper), 0)                                AS avg_income_upper,
                COUNT(CASE WHEN cd_credit_rating = 'High' THEN 1 END)     AS high_credit_count,
                ROUND(COUNT(CASE WHEN cd_credit_rating = 'High' THEN 1 END) * 100.0
                      / NULLIF(COUNT(*), 0), 1)                            AS high_credit_pct
            FROM customer_profiles
            GROUP BY generation, ca_state, cd_gender, cd_education_status, income_decile
            ORDER BY customer_count DESC""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:analytics;scenario:full_table_scan;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "full_table_scan", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_select_star(run_id):
    """
    SELECT * — "Customer 360° View"
    Triggers: int__antipattern_select_star (regex ^SELECT\\s+\\*\\s+FROM.* with bytes > 1MB)

    Query 1: SELECT * from 5-table CUSTOMER join filtered to 5 states.
    Query 2: SELECT * from INVENTORY + ITEM + DATE_DIM + WAREHOUSE for Dec 2002.
    """
    print("\n--- Scenario: SELECT * (Customer 360° View) ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        # Query 1 — SELECT * from 5-table customer join (millions of rows, all columns)
        f"""SELECT * FROM {CUSTOMER} c
            JOIN {CUSTOMER_ADDRESS} ca          ON c.c_current_addr_sk  = ca.ca_address_sk
            JOIN {CUSTOMER_DEMOGRAPHICS} cd     ON c.c_current_cdemo_sk = cd.cd_demo_sk
            JOIN {HOUSEHOLD_DEMOGRAPHICS} hd    ON c.c_current_hdemo_sk = hd.hd_demo_sk
            JOIN {INCOME_BAND} ib               ON hd.hd_income_band_sk = ib.ib_income_band_sk
            WHERE ca.ca_state IN ('TX', 'CA', 'NY', 'FL', 'IL')""",

        # Query 2 — SELECT * from inventory pipeline for a single month
        f"""SELECT * FROM {INVENTORY} inv
            JOIN {ITEM} i       ON inv.inv_item_sk      = i.i_item_sk
            JOIN {DATE_DIM} d   ON inv.inv_date_sk      = d.d_date_sk
            JOIN {WAREHOUSE} w  ON inv.inv_warehouse_sk  = w.w_warehouse_sk
            WHERE d.d_year = 2002 AND d.d_moy = 12""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:analytics;scenario:select_star;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "select_star", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_spill_to_storage(run_id):
    """
    SPILL TO STORAGE — "Demand Forecasting Pipeline"
    Triggers: int__antipattern_spill_to_storage (bytes_spilled > 0)
    Warehouse: COMPUTE_WH (X-Small — deliberately undersized to force spill)

    5 concurrent window functions in a CTE: 7-day MA, 30-day MA, 30-day volatility,
    7-day lag, month-start FIRST_VALUE. Outer query calculates z-scores, inventory
    status, month-to-date change. Filtered to d_year=2002 (~260M rows) → will spill.
    """
    print("\n--- Scenario: SPILL TO STORAGE (Demand Forecasting Pipeline) ---")
    conn = get_connection("COMPUTE_WH")  # X-Small → will spill
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        f"""WITH demand_signals AS (
                SELECT
                    inv.inv_item_sk,
                    inv.inv_warehouse_sk,
                    inv.inv_date_sk,
                    inv.inv_quantity_on_hand,
                    -- 7-day moving average
                    AVG(inv.inv_quantity_on_hand) OVER (
                        PARTITION BY inv.inv_item_sk, inv.inv_warehouse_sk
                        ORDER BY inv.inv_date_sk
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS ma_7day,
                    -- 30-day moving average
                    AVG(inv.inv_quantity_on_hand) OVER (
                        PARTITION BY inv.inv_item_sk, inv.inv_warehouse_sk
                        ORDER BY inv.inv_date_sk
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS ma_30day,
                    -- 30-day volatility (standard deviation)
                    STDDEV(inv.inv_quantity_on_hand) OVER (
                        PARTITION BY inv.inv_item_sk, inv.inv_warehouse_sk
                        ORDER BY inv.inv_date_sk
                        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                    ) AS volatility_30day,
                    -- 7-day lag for week-over-week change
                    LAG(inv.inv_quantity_on_hand, 7) OVER (
                        PARTITION BY inv.inv_item_sk, inv.inv_warehouse_sk
                        ORDER BY inv.inv_date_sk
                    ) AS qty_7day_ago,
                    -- Month-start baseline
                    FIRST_VALUE(inv.inv_quantity_on_hand) OVER (
                        PARTITION BY inv.inv_item_sk, inv.inv_warehouse_sk, d.d_moy
                        ORDER BY inv.inv_date_sk
                    ) AS month_start_qty
                FROM {INVENTORY} inv
                JOIN {DATE_DIM} d ON inv.inv_date_sk = d.d_date_sk
                WHERE d.d_year = 2002 AND d.d_moy BETWEEN 1 AND 6
            )
            SELECT
                inv_item_sk,
                inv_warehouse_sk,
                inv_date_sk,
                inv_quantity_on_hand,
                ma_7day,
                ma_30day,
                volatility_30day,
                -- Z-score: how far current qty deviates from 30-day norm
                CASE
                    WHEN volatility_30day > 0
                    THEN ROUND((inv_quantity_on_hand - ma_30day) / volatility_30day, 2)
                    ELSE 0
                END AS z_score,
                -- Inventory status classification
                CASE
                    WHEN inv_quantity_on_hand = 0                          THEN 'STOCKOUT'
                    WHEN inv_quantity_on_hand < ma_30day * 0.3             THEN 'LOW_STOCK'
                    WHEN inv_quantity_on_hand > ma_30day * 2.0             THEN 'OVERSTOCK'
                    ELSE 'NORMAL'
                END AS inventory_status,
                -- Month-to-date change
                ROUND((inv_quantity_on_hand - month_start_qty) * 100.0
                      / NULLIF(month_start_qty, 0), 1)                    AS mtd_change_pct,
                -- Week-over-week change
                ROUND((inv_quantity_on_hand - qty_7day_ago) * 100.0
                      / NULLIF(qty_7day_ago, 0), 1)                       AS wow_change_pct
            FROM demand_signals
            ORDER BY z_score DESC
            LIMIT 100000""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:data_engineering;scenario:spill_to_storage;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "spill_to_storage", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_repeated_queries(run_id):
    """
    REPEATED QUERIES — "Daily Category Performance Report"
    Triggers: int__antipattern_repeated_queries (same hash > 10 times/day AND total cost > $5)
    Warehouse: ANALYTICS_WH

    Dashboard-style query: inventory by warehouse + category + class with value
    ranking, out-of-stock rates, and inventory valuation. Run 25 times with
    different inv_date_sk values (2452275-2452299). Same query structure →
    same parameterized hash.
    """
    print("\n--- Scenario: REPEATED QUERIES (Daily Category Performance x25) ---")
    conn = get_connection("ANALYTICS_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    # Same query structure, different date parameter → same parameterized hash
    base_sql = f"""
        WITH daily_inventory AS (
            SELECT
                w.w_warehouse_name,
                i.i_category,
                i.i_class,
                SUM(inv.inv_quantity_on_hand)                                AS total_qty,
                COUNT(DISTINCT inv.inv_item_sk)                              AS unique_items,
                COUNT(CASE WHEN inv.inv_quantity_on_hand = 0 THEN 1 END)    AS out_of_stock_items,
                SUM(inv.inv_quantity_on_hand * i.i_current_price)            AS inventory_value,
                RANK() OVER (
                    PARTITION BY w.w_warehouse_name
                    ORDER BY SUM(inv.inv_quantity_on_hand * i.i_current_price) DESC
                ) AS value_rank
            FROM {INVENTORY} inv
            JOIN {WAREHOUSE} w  ON inv.inv_warehouse_sk = w.w_warehouse_sk
            JOIN {ITEM} i       ON inv.inv_item_sk      = i.i_item_sk
            WHERE inv.inv_date_sk = {{date_sk}}
            GROUP BY w.w_warehouse_name, i.i_category, i.i_class
        )
        SELECT
            w_warehouse_name,
            i_category,
            i_class,
            total_qty,
            unique_items,
            out_of_stock_items,
            ROUND(out_of_stock_items * 100.0 / NULLIF(unique_items, 0), 1) AS oos_rate_pct,
            ROUND(inventory_value, 2)                                       AS inventory_value,
            value_rank
        FROM daily_inventory
        ORDER BY inventory_value DESC
    """

    # Run 25 times with different date_sk values
    for i in range(25):
        date_sk = 2452275 + i  # Different dates, same query structure
        sql = base_sql.format(date_sk=date_sk)
        tag = f"team:analytics;scenario:repeated_query;run_id:{run_id};iteration:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "repeated_query", "ANALYTICS_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_cartesian_join(run_id):
    """
    CARTESIAN JOIN — "Market Basket Affinity Analysis"
    Triggers: int__antipattern_cartesian_join (rows_produced > 10x bytes_scanned AND > 1M rows)
    Warehouse: COMPUTE_WH

    PROMOTION (2K) × HOUSEHOLD_DEMOGRAPHICS (7.2K) × CUSTOMER_DEMOGRAPHICS (1.9M)
    using comma-join syntax. Only hd↔ib has a proper join — PROMOTION and
    CUSTOMER_DEMOGRAPHICS have no join condition → massive cartesian product.
    Rich business logic with targeting strategy classification.
    """
    print("\n--- Scenario: CARTESIAN JOIN (Market Basket Affinity) ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        # PROMOTION × HOUSEHOLD_DEMOGRAPHICS × CUSTOMER_DEMOGRAPHICS
        # Only hd↔ib has a proper join; PROMOTION and CD have no join → cartesian
        f"""SELECT
                p.p_promo_name,
                p.p_channel_email,
                p.p_channel_tv,
                p.p_channel_catalog,
                hd.hd_buy_potential,
                ib.ib_lower_bound                                           AS hh_income_lower,
                ib.ib_upper_bound                                           AS hh_income_upper,
                cd.cd_gender,
                cd.cd_education_status,
                cd.cd_marital_status,
                cd.cd_credit_rating,
                CASE
                    WHEN cd.cd_credit_rating = 'High'
                         AND ib.ib_upper_bound > 100000
                         AND hd.hd_buy_potential = '>10000'
                    THEN 'PREMIUM_TARGET'
                    WHEN cd.cd_credit_rating IN ('High', 'Good')
                         AND ib.ib_upper_bound > 50000
                    THEN 'GROWTH_PROSPECT'
                    WHEN p.p_channel_email = 'Y' AND cd.cd_education_status = 'College'
                    THEN 'DIGITAL_NATIVE'
                    ELSE 'MASS_MARKET'
                END AS targeting_strategy,
                CASE
                    WHEN p.p_channel_tv = 'Y' AND p.p_channel_catalog = 'Y'
                    THEN 'MULTI_CHANNEL'
                    WHEN p.p_channel_tv = 'Y'
                    THEN 'TV_ONLY'
                    WHEN p.p_channel_email = 'Y'
                    THEN 'EMAIL_ONLY'
                    ELSE 'OTHER'
                END AS channel_mix
            FROM {PROMOTION} p,
                 {HOUSEHOLD_DEMOGRAPHICS} hd,
                 {INCOME_BAND} ib,
                 {CUSTOMER_DEMOGRAPHICS} cd
            WHERE hd.hd_income_band_sk = ib.ib_income_band_sk
              AND p.p_channel_tv = 'Y'""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:analytics;scenario:cartesian_join;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "cartesian_join", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_large_sort_no_limit(run_id):
    """
    LARGE SORT WITHOUT LIMIT — "Customer Lifetime Value Ranking"
    Triggers: int__antipattern_large_sort_no_limit (ORDER BY + rows > 100K + no LIMIT)
    Warehouse: COMPUTE_WH

    Customer lifetime value scoring with 5-table join, loyalty tier classification
    (PLATINUM/GOLD/SILVER/BRONZE), credit scoring, state-level income ranking.
    Sorts entire ~60M customer result set with no LIMIT.
    """
    print("\n--- Scenario: LARGE SORT NO LIMIT (Customer Lifetime Value) ---")
    conn = get_connection("COMPUTE_WH")
    cur = conn.cursor()
    log_cur = conn.cursor()

    queries = [
        f"""WITH customer_ltv AS (
                SELECT
                    c.c_customer_sk,
                    c.c_customer_id,
                    c.c_first_name,
                    c.c_last_name,
                    c.c_birth_year,
                    ca.ca_state,
                    ca.ca_city,
                    cd.cd_gender,
                    cd.cd_credit_rating,
                    cd.cd_education_status,
                    ib.ib_lower_bound                                       AS income_lower,
                    ib.ib_upper_bound                                       AS income_upper,
                    hd.hd_buy_potential,
                    -- Loyalty tier based on buy potential and credit
                    CASE
                        WHEN hd.hd_buy_potential = '>10000' AND cd.cd_credit_rating = 'High'
                        THEN 'PLATINUM'
                        WHEN hd.hd_buy_potential IN ('>10000', '5001-10000') AND cd.cd_credit_rating IN ('High', 'Good')
                        THEN 'GOLD'
                        WHEN hd.hd_buy_potential IN ('5001-10000', '1001-5000')
                        THEN 'SILVER'
                        ELSE 'BRONZE'
                    END AS loyalty_tier,
                    -- Credit score band
                    CASE
                        WHEN cd.cd_credit_rating = 'High'   THEN 3
                        WHEN cd.cd_credit_rating = 'Good'   THEN 2
                        WHEN cd.cd_credit_rating = 'Low'    THEN 1
                        ELSE 0
                    END AS credit_score,
                    -- State-level income rank
                    DENSE_RANK() OVER (
                        PARTITION BY ca.ca_state
                        ORDER BY ib.ib_upper_bound DESC
                    ) AS state_income_rank
                FROM {CUSTOMER} c
                LEFT JOIN {CUSTOMER_ADDRESS} ca          ON c.c_current_addr_sk  = ca.ca_address_sk
                LEFT JOIN {CUSTOMER_DEMOGRAPHICS} cd     ON c.c_current_cdemo_sk = cd.cd_demo_sk
                LEFT JOIN {HOUSEHOLD_DEMOGRAPHICS} hd    ON c.c_current_hdemo_sk = hd.hd_demo_sk
                LEFT JOIN {INCOME_BAND} ib               ON hd.hd_income_band_sk = ib.ib_income_band_sk
                WHERE ca.ca_state IN ('TX','CA','NY','FL','IL','PA','OH','GA','NC','MI')
            )
            SELECT
                c_customer_sk,
                c_customer_id,
                c_first_name,
                c_last_name,
                ca_state,
                ca_city,
                loyalty_tier,
                credit_score,
                income_upper,
                state_income_rank,
                cd_education_status,
                -- Composite LTV score
                (credit_score * 30) + (COALESCE(income_upper, 0) / 1000) + (
                    CASE loyalty_tier
                        WHEN 'PLATINUM' THEN 100
                        WHEN 'GOLD'     THEN 70
                        WHEN 'SILVER'   THEN 40
                        ELSE 10
                    END
                ) AS ltv_score
            FROM customer_ltv
            ORDER BY ltv_score DESC, ca_state, c_customer_sk""",
    ]

    for i, sql in enumerate(queries):
        tag = f"team:data_engineering;scenario:large_sort_no_limit;run_id:{run_id};query:{i+1}"
        run_query(cur, sql, tag, log_cur, run_id, "large_sort_no_limit", "COMPUTE_WH")

    cur.close()
    log_cur.close()
    conn.close()


def scenario_expensive_multi_join(run_id):
    """
    EXPENSIVE MULTI-TABLE JOIN — "Quarterly Inventory Valuation Report"
    Triggers: High cost from multi-table joins
    Warehouse: ANALYTICS_WH + ETL_WH (runs on both for cost comparison)

    Complex inventory valuation: INVENTORY × ITEM × DATE_DIM × WAREHOUSE + CROSS JOIN
    with STORE. Calculates line values, quarterly summaries, cumulative running totals.
    """
    print("\n--- Scenario: EXPENSIVE MULTI-TABLE JOIN (Quarterly Valuation) ---")

    query = f"""
        WITH inventory_valued AS (
            SELECT
                d.d_year,
                d.d_quarter_name,
                d.d_moy,
                w.w_warehouse_name,
                w.w_state,
                i.i_category,
                i.i_class,
                i.i_brand,
                inv.inv_quantity_on_hand,
                i.i_current_price,
                inv.inv_quantity_on_hand * i.i_current_price                AS line_value,
                SUM(inv.inv_quantity_on_hand * i.i_current_price) OVER (
                    PARTITION BY d.d_year, d.d_quarter_name, w.w_warehouse_name
                    ORDER BY i.i_category, i.i_class
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_quarterly_value
            FROM {INVENTORY} inv
            JOIN {ITEM} i       ON inv.inv_item_sk      = i.i_item_sk
            JOIN {DATE_DIM} d   ON inv.inv_date_sk      = d.d_date_sk
            JOIN {WAREHOUSE} w  ON inv.inv_warehouse_sk  = w.w_warehouse_sk
            WHERE d.d_year = 2002 AND d.d_moy BETWEEN 1 AND 3
        ),
        quarterly_summary AS (
            SELECT
                d_year,
                d_quarter_name,
                w_warehouse_name,
                w_state,
                i_category,
                COUNT(*)                                                   AS record_count,
                SUM(line_value)                                            AS total_value,
                AVG(line_value)                                            AS avg_line_value,
                MAX(cumulative_quarterly_value)                             AS max_cumulative,
                COUNT(DISTINCT i_brand)                                    AS brand_count,
                SUM(CASE WHEN inv_quantity_on_hand = 0 THEN 1 ELSE 0 END) AS zero_stock_records
            FROM inventory_valued
            GROUP BY d_year, d_quarter_name, w_warehouse_name, w_state, i_category
        )
        SELECT
            qs.*,
            s.s_store_name,
            s.s_state                                                      AS store_state,
            ROUND(qs.zero_stock_records * 100.0 / NULLIF(qs.record_count, 0), 2) AS zero_stock_pct,
            RANK() OVER (
                PARTITION BY qs.d_quarter_name
                ORDER BY qs.total_value DESC
            ) AS value_rank
        FROM quarterly_summary qs
        CROSS JOIN {STORE} s
        ORDER BY qs.total_value DESC
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
    COST SPIKE — "Burst of Heavy Analytics"
    Triggers: int__alert_cost_daily_spike (daily cost > 2x 30-day avg)
    Warehouse: ANALYTICS_WH

    4 complex queries fired rapidly — variations of scenarios 1, 3, 6, 7.
    Creates a visible cost spike in the daily cost anomaly detector.
    """
    print("\n--- Scenario: COST SPIKE (Burst of Heavy Analytics) ---")
    conn = get_connection("ANALYTICS_WH")  # Small warehouse → 2 credits/hr
    cur = conn.cursor()
    log_cur = conn.cursor()

    spike_queries = [
        # Burst 1 — Inventory health (variation of scenario 1)
        f"""WITH warehouse_health AS (
                SELECT
                    w.w_warehouse_name,
                    i.i_category,
                    SUM(inv.inv_quantity_on_hand)                           AS total_qty,
                    COUNT(CASE WHEN inv.inv_quantity_on_hand = 0 THEN 1 END) AS stockout_count,
                    COUNT(*)                                               AS total_records,
                    STDDEV(inv.inv_quantity_on_hand)                        AS qty_stddev,
                    RANK() OVER (
                        PARTITION BY w.w_warehouse_name
                        ORDER BY SUM(inv.inv_quantity_on_hand) DESC
                    ) AS category_rank
                FROM {INVENTORY} inv
                JOIN {WAREHOUSE} w   ON inv.inv_warehouse_sk = w.w_warehouse_sk
                JOIN {ITEM} i        ON inv.inv_item_sk      = i.i_item_sk
                JOIN {DATE_DIM} d    ON inv.inv_date_sk      = d.d_date_sk
                WHERE d.d_year IN (2001, 2002)
                GROUP BY w.w_warehouse_name, i.i_category
            )
            SELECT *, ROUND(stockout_count * 100.0 / NULLIF(total_records, 0), 2) AS stockout_pct
            FROM warehouse_health
            ORDER BY stockout_pct DESC""",

        # Burst 2 — Demand signal analysis (variation of scenario 3)
        f"""SELECT
                inv.inv_item_sk,
                inv.inv_warehouse_sk,
                inv.inv_date_sk,
                inv.inv_quantity_on_hand,
                AVG(inv.inv_quantity_on_hand) OVER (
                    PARTITION BY inv.inv_item_sk, inv.inv_warehouse_sk
                    ORDER BY inv.inv_date_sk ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS ma_7day,
                STDDEV(inv.inv_quantity_on_hand) OVER (
                    PARTITION BY inv.inv_item_sk, inv.inv_warehouse_sk
                    ORDER BY inv.inv_date_sk ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS volatility_30day
            FROM {INVENTORY} inv
            JOIN {DATE_DIM} d ON inv.inv_date_sk = d.d_date_sk
            WHERE d.d_year = 2001 AND d.d_moy BETWEEN 1 AND 3
            ORDER BY volatility_30day DESC
            LIMIT 100000""",

        # Burst 3 — Customer LTV ranking (variation of scenario 6)
        f"""SELECT
                c.c_customer_sk,
                c.c_first_name,
                c.c_last_name,
                ca.ca_state,
                cd.cd_credit_rating,
                ib.ib_upper_bound                                          AS income_upper,
                CASE
                    WHEN hd.hd_buy_potential = '>10000' AND cd.cd_credit_rating = 'High' THEN 'PLATINUM'
                    WHEN hd.hd_buy_potential IN ('>10000', '5001-10000')                  THEN 'GOLD'
                    ELSE 'SILVER'
                END AS loyalty_tier,
                DENSE_RANK() OVER (PARTITION BY ca.ca_state ORDER BY ib.ib_upper_bound DESC) AS state_rank
            FROM {CUSTOMER} c
            LEFT JOIN {CUSTOMER_ADDRESS} ca      ON c.c_current_addr_sk  = ca.ca_address_sk
            LEFT JOIN {CUSTOMER_DEMOGRAPHICS} cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
            LEFT JOIN {HOUSEHOLD_DEMOGRAPHICS} hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
            LEFT JOIN {INCOME_BAND} ib           ON hd.hd_income_band_sk = ib.ib_income_band_sk
            WHERE ca.ca_state IN ('TX','CA','NY','FL','IL')
            ORDER BY income_upper DESC NULLS LAST, c.c_customer_sk""",

        # Burst 4 — Inventory valuation (variation of scenario 7)
        f"""SELECT
                d.d_quarter_name,
                w.w_warehouse_name,
                i.i_category,
                i.i_class,
                SUM(inv.inv_quantity_on_hand * i.i_current_price)          AS total_value,
                COUNT(DISTINCT i.i_brand)                                  AS brand_count,
                AVG(inv.inv_quantity_on_hand)                              AS avg_qty,
                SUM(inv.inv_quantity_on_hand * i.i_current_price) /
                    NULLIF(SUM(inv.inv_quantity_on_hand), 0)               AS weighted_avg_price,
                RANK() OVER (
                    PARTITION BY d.d_quarter_name
                    ORDER BY SUM(inv.inv_quantity_on_hand * i.i_current_price) DESC
                ) AS value_rank
            FROM {INVENTORY} inv
            JOIN {ITEM} i       ON inv.inv_item_sk      = i.i_item_sk
            JOIN {DATE_DIM} d   ON inv.inv_date_sk      = d.d_date_sk
            JOIN {WAREHOUSE} w  ON inv.inv_warehouse_sk  = w.w_warehouse_sk
            WHERE d.d_year = 2002 AND d.d_moy BETWEEN 1 AND 6
            GROUP BY d.d_quarter_name, w.w_warehouse_name, i.i_category, i.i_class
            ORDER BY total_value DESC""",
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
