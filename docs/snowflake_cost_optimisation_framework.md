# Snowflake Cost Optimisation Framework

**Solution Proposal**

---

| | |
|---|---|
| **Prepared by** | Srinivas Addanki |
| **Date** | 11 April 2026 |
| **Version** | 1.0 |
| **Classification** | Confidential |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Problem Statement](#2-problem-statement)
3. [Solution Overview](#3-solution-overview)
4. [Phase 1 — Cost Visibility and Attribution](#4-phase-1--cost-visibility-and-attribution)
5. [Phase 2 — Query Optimisation and Recommendations](#5-phase-2--query-optimisation-and-recommendations)
6. [Technical Architecture](#6-technical-architecture)
7. [Delivery Plan](#7-delivery-plan)
8. [Prerequisites and Assumptions](#8-prerequisites-and-assumptions)
9. [Risk and Mitigation](#9-risk-and-mitigation)
10. [Why This Approach](#10-why-this-approach)
11. [Team and Expertise](#11-team-and-expertise)
12. [Next Steps](#12-next-steps)
13. [Appendix — Key Snowflake Metadata Views](#appendix--key-snowflake-metadata-views)

---

## 1. Executive Summary

Organisations running multiple business teams on Snowflake often face a common challenge: without standardised governance and visibility, compute and storage costs grow unpredictably. Teams adopt their own patterns, warehouses are provisioned without sizing guidelines, and there is no single view of who is spending how much and why.

This proposal presents a **Snowflake Cost Optimisation Framework** delivered in two phases:

- **Phase 1 (4 weeks):** Analyse the Snowflake environment, identify cost drivers, and deliver an interactive dashboard showing cost attribution by warehouse, team, data product, and user.
- **Phase 2 (4 weeks):** Evaluate query efficiency, detect anti-patterns, and provide prioritised optimisation recommendations with estimated savings.

The framework is built entirely on **Snowflake-native metadata** — no external monitoring tools are required. All analysis runs inside the customer's own Snowflake account, ensuring data never leaves their environment.

---

## 2. Problem Statement

### Current Challenges

| Challenge | Impact |
|-----------|--------|
| **No cost attribution** | Cannot answer "which team or process is driving our Snowflake bill" |
| **Oversized warehouses** | Teams provision large warehouses "just in case" — paying for idle compute |
| **No query governance** | Inefficient SQL patterns (full table scans, SELECT *, missing filters) run unchecked |
| **Storage sprawl** | Unused tables, excessive Time Travel retention, and forgotten clones accumulate silently |
| **Reactive cost management** | Costs are reviewed monthly from invoices, not proactively monitored |
| **No standardisation** | Each team adopts its own patterns — no shared best practices |

### Business Impact

- Snowflake costs grow 15–30% quarter-over-quarter without intervention
- Finance teams cannot allocate cloud costs to business units accurately
- Engineering teams lack data to justify optimisation work vs. new features
- Potential savings of 20–40% remain unrealised

---

## 3. Solution Overview

### Core Idea

Snowflake captures rich metadata about every query, warehouse, and storage object in its `SNOWFLAKE.ACCOUNT_USAGE` schema. This data is available for the past 365 days at no additional cost. Our framework transforms this raw metadata into actionable cost intelligence.

### Solution Components

```
┌─────────────────────────────────────────────────────────────┐
│                   SNOWFLAKE ACCOUNT                          │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │  ACCOUNT     │    │     dbt      │    │  Streamlit    │  │
│  │  _USAGE      │───>│  Transform   │───>│  Dashboard    │  │
│  │  (metadata)  │    │  Layer       │    │  (interactive)│  │
│  └──────────────┘    └──────────────┘    └───────────────┘  │
│                             │                                │
│                             v                                │
│                      ┌──────────────┐                        │
│                      │ Optimisation │                        │
│                      │ Recommender  │                        │
│                      │ (Phase 2)    │                        │
│                      └──────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

**Everything runs inside Snowflake.** No data extraction. No third-party SaaS tools. No ongoing licence costs.

---

## 4. Phase 1 — Cost Visibility and Attribution

### Objective

Provide clear, drillable answers to:

- What is our total Snowflake spend (compute, storage, serverless)?
- Which warehouses are the most expensive?
- Which teams, users, and data products drive the most cost?
- Where is storage being wasted?
- What are the cost trends over the past 90 days?

### 4.1 Compute Cost Analysis

**Data Source:** `WAREHOUSE_METERING_HISTORY`, `QUERY_HISTORY`

| Metric | Description |
|--------|-------------|
| Credits consumed per warehouse | Hourly/daily/monthly credit burn |
| Cost per query (estimated) | Query runtime weighted by warehouse size and credit price |
| Warehouse idle time | Periods where a warehouse is running but executing no queries |
| Queue wait time | Queries waiting for available compute slots |
| Cost by user / role | Which users or service accounts drive the most compute |
| Cost by data product | Attributed via query tags, warehouse assignment, or role mapping |
| Peak vs. off-peak usage | Identify scheduling opportunities to shift workloads |

**Credit-to-Cost Conversion:**

```
Estimated Query Cost ($) = (query_execution_time_seconds / 3600)
                           × warehouse_credit_per_hour
                           × credit_price_dollars
```

Where `warehouse_credit_per_hour` depends on warehouse size (XS=1, S=2, M=4, L=8, XL=16, etc.) and `credit_price_dollars` is based on the customer's Snowflake contract.

### 4.2 Storage Cost Analysis

**Data Source:** `TABLE_STORAGE_METRICS`, `STORAGE_USAGE`

| Metric | Description |
|--------|-------------|
| Active storage | Data currently in use |
| Time Travel storage | Historical data retained for the Time Travel window |
| Fail-safe storage | 7-day regulatory recovery storage (non-configurable) |
| Storage by database / schema | Identify which areas consume the most storage |
| Unused tables | Tables with zero reads in 30/60/90 days (via `ACCESS_HISTORY`) |
| Clone overhead | Storage consumed by zero-copy clones that have diverged |

### 4.3 Serverless Cost Analysis

**Data Source:** `PIPE_USAGE_HISTORY`, `AUTOMATIC_CLUSTERING_HISTORY`, `MATERIALIZED_VIEW_REFRESH_HISTORY`, `SERVERLESS_TASK_HISTORY`

| Metric | Description |
|--------|-------------|
| Snowpipe credits | Cost of continuous data loading |
| Auto-clustering credits | Cost of maintaining clustering on tables |
| Materialized view refresh credits | Cost of keeping MVs up-to-date |
| Task execution credits | Cost of scheduled serverless tasks |

### 4.4 Dashboard Deliverable

An interactive Streamlit dashboard (hosted natively in Snowflake) with:

- **Executive Summary** — Total spend, compute/storage/serverless split, month-over-month trend
- **Warehouse Deep Dive** — Per-warehouse cost, utilisation, idle %, queue time
- **Team Attribution** — Cost allocated to business teams via role/warehouse/query tag mapping
- **Storage Explorer** — Table-level storage breakdown, unused table list, Time Travel waste
- **Trend Analysis** — 90-day cost trends with anomaly highlighting

---

## 5. Phase 2 — Query Optimisation and Recommendations

### Objective

Identify **why** costs are high and provide actionable recommendations to reduce them, prioritised by estimated savings.

### 5.1 Warehouse Right-Sizing

| Signal | Detection Method | Recommendation |
|--------|-----------------|----------------|
| Consistent queuing | `QUERY_HISTORY.queued_overload_time > 0` frequently | Scale up warehouse or enable multi-cluster |
| Consistent idle time | Warehouse running with no queries for extended periods | Reduce auto-suspend interval (e.g., 300s to 60s) |
| Oversized for workload | Small queries running on L/XL warehouses | Downsize warehouse or route to a smaller one |
| Uneven load | Spikes at certain hours, idle otherwise | Schedule workloads to consolidate usage windows |

### 5.2 Query Anti-Pattern Detection

| Anti-Pattern | Detection Method | Impact |
|-------------|-----------------|--------|
| **Full table scans** | `PARTITIONS_SCANNED / PARTITIONS_TOTAL > 0.8` | Excessive compute for large tables |
| **SELECT \*** | Query text pattern matching | Scans all columns; wastes I/O |
| **Missing filters** | High `BYTES_SCANNED` relative to `ROWS_PRODUCED` | Reads far more data than needed |
| **Spill to storage** | `BYTES_SPILLED_TO_LOCAL/REMOTE_STORAGE > 0` | Query needs more memory than warehouse provides |
| **Repeated identical queries** | Same `QUERY_PARAMETERIZED_HASH` running frequently | Results should be cached or materialised |
| **Cartesian joins** | `ROWS_PRODUCED >> ROWS_SCANNED` in join queries | Missing or incorrect join conditions |
| **Excessive ORDER BY** | Large result sets with ORDER BY but no LIMIT | Sorting millions of rows unnecessarily |

### 5.3 Storage Optimisation Recommendations

| Opportunity | Detection | Recommendation |
|-------------|-----------|----------------|
| Unused tables | No reads in `ACCESS_HISTORY` for 90+ days | Archive or drop |
| Excessive Time Travel | Retention set to 90 days on non-critical tables | Reduce to 1 day (save ~99% of TT storage) |
| Transient table candidates | Tables that are rebuilt daily (ephemeral data) | Convert to TRANSIENT (eliminates Fail-safe storage) |
| Stale clones | Clones created months ago, not accessed | Drop |
| Large staging tables | Temporary/staging data persisted permanently | Add lifecycle policies or auto-drop |

### 5.4 Prioritised Recommendations Report

Each recommendation will include:

| Field | Description |
|-------|-------------|
| **Category** | Warehouse / Query / Storage |
| **Object** | Warehouse name, query ID, table name |
| **Current Cost** | Estimated monthly cost of the current pattern |
| **Estimated Savings** | Projected monthly saving if recommendation is applied |
| **Effort** | Low (config change) / Medium (query rewrite) / High (architecture change) |
| **Risk** | Impact assessment of making the change |
| **Action** | Specific steps to implement the recommendation |
| **Priority** | Ranked by savings ÷ effort (ROI-based) |

---

## 6. Technical Architecture

### Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Data Modelling** | dbt (data build tool) | Industry-standard transformation framework; version-controlled, testable, documented |
| **Data Platform** | Snowflake | Customer's existing platform; zero data movement |
| **Dashboards** | Streamlit in Snowflake | Native to Snowflake; no separate hosting; interactive |
| **Infrastructure** | Terraform (optional) | Reproducible deployment of schemas, roles, warehouses |
| **Scheduling** | dbt Cloud or Snowflake Tasks | Automated daily refresh of cost models |

### Data Flow

```
SNOWFLAKE.ACCOUNT_USAGE (raw metadata, 365 days)
        │
        ▼
┌─────────────────────────────┐
│  dbt Staging Models         │  Source definitions + light cleaning
│  (src__query_history, etc.) │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│  dbt Intermediate Models    │  Business logic: cost calculations,
│  (int__warehouse_credits,   │  utilisation metrics, pattern detection
│   int__query_costs, etc.)   │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│  dbt Publication Models     │  Consumer-ready: cost_by_warehouse,
│  (cost_summary,             │  cost_by_team, optimisation_candidates
│   recommendations)          │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│  Streamlit Dashboard        │  Interactive visualisation and
│                             │  recommendation explorer
└─────────────────────────────┘
```

### Model Design Principles

- **Idempotent:** Models can be re-run at any time without side effects
- **Incremental:** Large tables (query history) use incremental materialisation for efficiency
- **Tested:** dbt tests validate data quality (uniqueness, referential integrity, accepted values)
- **Documented:** Every model and column is documented in dbt, browsable via dbt docs
- **Configurable:** Credit prices, warehouse mappings, and team allocations are maintained as seed files — easy to update without code changes

---

## 7. Delivery Plan

### Phase 1 — Cost Visibility and Attribution (4 Weeks)

| Week | Activities | Deliverables |
|------|-----------|-------------|
| **Week 1** | Environment access and discovery. Grant `IMPORTED PRIVILEGES` on `SNOWFLAKE` database. Profile the account: number of warehouses, databases, users, query volume. Set up dbt project structure. | Discovery report: account profile, initial findings |
| **Week 2** | Build core dbt models: warehouse credit usage, query cost attribution, storage breakdown. Configure team/data product mapping (seed files or role-based). | Working dbt models for compute, storage, and serverless costs |
| **Week 3** | Build Streamlit dashboard: executive summary, warehouse deep-dive, team attribution, storage explorer, trend analysis. | Interactive dashboard v1 |
| **Week 4** | Refinement, testing, and documentation. Walkthrough with stakeholders. Knowledge transfer. | Final Phase 1 dashboard, user guide, dbt documentation |

### Phase 2 — Optimisation Recommendations (4 Weeks)

| Week | Activities | Deliverables |
|------|-----------|-------------|
| **Week 5** | Build warehouse right-sizing models. Analyse utilisation patterns (idle time, queuing, peak/off-peak). | Warehouse sizing recommendations |
| **Week 6** | Build query anti-pattern detection. Identify full scans, spill-to-storage, repeated queries, SELECT *. | Query-level optimisation candidates |
| **Week 7** | Build storage optimisation models. Identify unused tables, Time Travel waste, transient candidates. Prioritise all recommendations by ROI. | Storage recommendations + prioritised action list |
| **Week 8** | Enhance dashboard with recommendations tab. Final testing. Stakeholder walkthrough. Knowledge transfer and handover. | Complete framework, final report with estimated savings |

### Summary Timeline

```
Week 1     Week 2     Week 3     Week 4     Week 5     Week 6     Week 7     Week 8
┌──────────────────────────────────────┐┌──────────────────────────────────────┐
│          PHASE 1                     ││          PHASE 2                     │
│  Discovery → Models → Dashboard →   ││  Warehouse → Query → Storage →      │
│                          Handover   ││                        Final Report  │
└──────────────────────────────────────┘└──────────────────────────────────────┘
         ▲                                        ▲
         │                                        │
    Dashboard v1                          Full Recommendations
    (Cost Visibility)                    (Optimisation Engine)
```

**Total Duration:** 8 weeks (Phase 1 and Phase 2 delivered sequentially)

---

## 8. Prerequisites and Assumptions

### Customer Prerequisites

| # | Requirement | Purpose |
|---|------------|---------|
| 1 | Grant `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database to the analytics role | Access to `ACCOUNT_USAGE` metadata views |
| 2 | Provide a dedicated Snowflake warehouse (Small or Medium) for running the framework | Compute for dbt models and dashboard queries |
| 3 | Provide a dedicated database/schema for the framework objects | Storage for the cost models and dashboard |
| 4 | Share Snowflake contract details (credit price, edition) | Accurate dollar-cost conversion |
| 5 | Provide organisational mapping (warehouses/roles to teams) | Cost attribution to business units |
| 6 | Nominate a technical point of contact | Collaboration during discovery and validation |

### Assumptions

- Customer is on Snowflake Enterprise Edition or higher (required for `ACCESS_HISTORY`)
- `ACCOUNT_USAGE` views have data for at least 30 days (ideally 90+ days for trend analysis)
- The framework warehouse will not be used for other workloads (to avoid cost contamination)
- Query tags are either already in use or can be introduced for future cost attribution
- Customer has dbt Cloud or is open to using it (alternatively, Snowflake Tasks can schedule the models)

---

## 9. Risk and Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| `IMPORTED PRIVILEGES` not granted promptly | Medium | Blocks all work | Raise as Day 1 action; provide exact GRANT statement |
| Customer on Standard Edition (no `ACCESS_HISTORY`) | Low | Cannot detect unused tables | Fall back to `INFORMATION_SCHEMA.TABLE_STORAGE_METRICS` + manual review |
| No query tags in use | Medium | Limits cost attribution granularity | Attribute by warehouse and role instead; recommend query tagging as a future improvement |
| Very high query volume (>1M queries/day) | Low | Slow model builds | Use incremental materialisation with 7-day lookback windows |
| Stakeholder availability for walkthroughs | Medium | Delays sign-off | Schedule walkthroughs at project start; async review via shared dashboard |

---

## 10. Why This Approach

### vs. Snowflake-Native Cost Management (Budgets, Resource Monitors)

Snowflake provides basic budgets and resource monitors, but they only alert on thresholds — they do not explain **why** costs are high or **what** to do about it. Our framework provides the analytical layer that turns alerts into action.

### vs. Third-Party SaaS Tools (Select.dev, Keebo, Sundeck)

| Factor | Our Framework | SaaS Tools |
|--------|--------------|------------|
| **Data residency** | Stays in customer's Snowflake account | Data sent to third-party |
| **Customisation** | Fully tailored to customer's org structure | Generic, one-size-fits-all |
| **Ongoing cost** | No licence fees (dbt + Streamlit are free/included) | $500–$5,000+/month |
| **Business context** | Integrates team mappings, query tags, data product names | Limited to Snowflake metadata only |
| **Extensibility** | Customer can add new models, metrics, dashboards | Limited to vendor roadmap |
| **Transparency** | All logic is visible in dbt SQL models | Black-box recommendations |

### Proven Expertise

Our team operates a production-grade dbt + Snowflake + Streamlit platform that already:

- Manages 10+ data products on Snowflake
- Implements query tagging for cost attribution
- Queries `SNOWFLAKE.ACCOUNT_USAGE` for lineage and monitoring
- Runs interactive Streamlit dashboards for operational observability
- Integrates with Datadog and Incident.io for alerting

This framework extends our proven patterns to a new use case — cost optimisation.

---

## 11. Team and Expertise

### Proposed Team Composition

| Role | Responsibility | Allocation |
|------|---------------|-----------|
| **Lead Engineer** | Architecture, dbt model development, Snowflake expertise | Full-time (8 weeks) |
| **Dashboard Developer** | Streamlit dashboard development, UX design | Part-time (Weeks 3–4, 7–8) |
| **Project Lead** | Stakeholder management, delivery oversight, customer communication | Part-time (throughout) |

### Key Competencies

- Snowflake architecture and performance tuning
- dbt (data build tool) — modelling, testing, documentation
- Streamlit dashboard development
- Terraform infrastructure-as-code for Snowflake
- Data observability and monitoring frameworks

---

## 12. Next Steps

| # | Action | Owner | Timeline |
|---|--------|-------|----------|
| 1 | Review and approve this proposal | Customer | By 15 April 2026 |
| 2 | Schedule a kickoff call | Both | Week of 16 April |
| 3 | Provide environment access and prerequisites | Customer | Before kickoff |
| 4 | Begin Phase 1 discovery | Delivery team | Kickoff + 1 day |

---

## Appendix — Key Snowflake Metadata Views

The framework leverages the following `SNOWFLAKE.ACCOUNT_USAGE` views. These are available to any Snowflake account with `IMPORTED PRIVILEGES` granted on the `SNOWFLAKE` database.

### Compute

| View | Description | Retention |
|------|-------------|-----------|
| `WAREHOUSE_METERING_HISTORY` | Credit consumption per warehouse, per hour | 365 days |
| `QUERY_HISTORY` | Full detail of every query executed: runtime, bytes scanned, spill, partitions, user, warehouse, query tag | 365 days |
| `WAREHOUSE_LOAD_HISTORY` | Warehouse utilisation: running, queued, blocked queries per interval | 365 days |

### Storage

| View | Description | Retention |
|------|-------------|-----------|
| `TABLE_STORAGE_METRICS` | Active, Time Travel, and Fail-safe bytes per table | Current snapshot |
| `STORAGE_USAGE` | Total account-level storage over time | 365 days |
| `DATABASE_STORAGE_USAGE_HISTORY` | Storage per database over time | 365 days |

### Access and Lineage

| View | Description | Retention |
|------|-------------|-----------|
| `ACCESS_HISTORY` | Which tables/columns were read or written by each query | 365 days |
| `LOGIN_HISTORY` | User login events (for activity analysis) | 365 days |
| `SESSIONS` | Session details including client application | 365 days |

### Serverless Features

| View | Description | Retention |
|------|-------------|-----------|
| `AUTOMATIC_CLUSTERING_HISTORY` | Credits consumed by auto-clustering | 365 days |
| `MATERIALIZED_VIEW_REFRESH_HISTORY` | Credits consumed by MV refreshes | 365 days |
| `PIPE_USAGE_HISTORY` | Credits consumed by Snowpipe | 365 days |
| `SERVERLESS_TASK_HISTORY` | Credits consumed by serverless tasks | 365 days |
| `SEARCH_OPTIMIZATION_HISTORY` | Credits consumed by search optimisation | 365 days |

---

*This document is confidential and intended for internal use and customer presentation.*
