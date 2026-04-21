# Snowflake Cost Optimisation Framework

## A Complete Solution for Cost Visibility, Attribution, and Savings

---

## What This Framework Is

The Snowflake Cost Optimisation Framework is a fully self-contained analytics system that lives entirely inside a customer's Snowflake account. It takes the raw metadata that Snowflake already captures about every query, every warehouse, and every storage object, and transforms it into clear answers about where money is being spent, why costs are growing, and exactly what to do about it.

There are no external tools, no third-party SaaS subscriptions, and no data leaving the customer's environment. The entire framework — data models, business logic, dashboards, alerting, and recommendations — runs natively on Snowflake using dbt for data transformation, Streamlit for interactive visualisation, and Snowflake Tasks for automation.

---

## The Problem We Solve

Organisations running multiple teams on Snowflake face a recurring set of challenges that grow more painful as usage scales:

**Nobody knows who is spending what.** When the monthly Snowflake invoice arrives, finance teams cannot break it down by business unit, team, or data product. The bill says "you spent $50,000 on compute" — but it does not say which team drove $30,000 of that, or which single query cost $800 to run.

**Warehouses are oversized and idle.** Teams provision large warehouses "just in case" and leave them running long after their workloads finish. A medium warehouse sitting idle for four hours costs the same as running queries for four hours — but delivers zero value.

**Inefficient queries run unchecked.** Without governance, engineers write queries that scan entire tables when they only need a handful of rows, join tables without proper conditions (creating explosive result sets), or run the same expensive query dozens of times a day when the results could be cached.

**Storage accumulates silently.** Tables that no one has read in months continue to occupy storage. Time Travel retention is set to maximum on tables that do not need it. Clones diverge from their source and quietly consume dedicated storage.

**Cost management is reactive, not proactive.** Most organisations review Snowflake costs from monthly invoices — weeks after the spending has already happened. By the time anyone notices a spike, thousands of dollars may have been wasted.

The result is that Snowflake costs typically grow 15 to 30 percent quarter-over-quarter without intervention, with potential savings of 20 to 40 percent left unrealised.

---

## What We Deliver

The framework delivers ten core capabilities across two phases:

### 1. Complete Cost Visibility

We build a comprehensive picture of every dollar spent on Snowflake, broken down across three cost categories:

- **Compute costs** — the credits consumed by warehouses executing queries. We track this at every level of detail: by warehouse, by team, by user, by individual query, by hour of day, and by day of week. We calculate the estimated dollar cost of each query based on how long it ran, how large the warehouse was, and what the customer pays per credit.

- **Storage costs** — the ongoing cost of data at rest. We break this down by database, schema, and individual table, distinguishing between active data (what is actually in use), Time Travel data (historical snapshots kept for recovery), and Fail-safe data (Snowflake's built-in seven-day protection layer). We identify tables that are consuming storage but have not been read in 90 or more days.

- **Serverless costs** — the credits consumed by Snowflake's automated features: Snowpipe (continuous data loading), automatic clustering, materialised view refreshes, serverless tasks, and search optimisation. These costs often go unnoticed because no warehouse is visibly running, but they can add up significantly.

### 2. Team-Level Cost Attribution

One of the most valuable capabilities is answering "which team is responsible for which portion of the bill." We derive team ownership from three sources, applied in priority order:

- **Query tags** — structured labels attached to queries by applications or workload generators. When a query carries a tag like `team:analytics`, we know exactly which team ran it.

- **Role names** — Snowflake roles often follow naming conventions that map to teams. An `ANALYST_ROLE` maps to the Analytics team, a `SYSADMIN` role maps to the Platform team, and so on.

- **Warehouse names** — when a warehouse is dedicated to a team (such as `ETL_WH` for Data Engineering or `ANALYTICS_WH` for the Analytics team), any query running on that warehouse can be attributed accordingly.

This approach works without requiring manual mapping files. The framework dynamically reads the actual roles, users, and warehouses from Snowflake's own metadata, so it always reflects the current state of the environment.

### 3. Query Anti-Pattern Detection

The framework automatically identifies six categories of wasteful query behaviour:

- **Full table scans** — queries that read more than 80 percent of a table's partitions when they should be using filters to read only what is needed. On a table with billions of rows, this means scanning terabytes of data unnecessarily.

- **SELECT \* queries** — queries that retrieve every column from a table when only a few columns are needed. This wastes I/O bandwidth and memory.

- **Spill to storage** — queries that exhaust the warehouse's in-memory capacity and overflow to disk (or worse, to remote storage). This is a signal that either the query needs optimisation or the warehouse needs to be larger for that specific workload.

- **Repeated identical queries** — the same query (identified by its parameterised hash) running dozens of times a day. These are candidates for result caching, materialisation, or consolidation into a single run.

- **Cartesian joins** — queries where the number of rows produced vastly exceeds the number of rows scanned, indicating a missing or incorrect join condition. A cartesian join between two tables of 10,000 rows each produces 100 million rows — an expensive mistake.

- **Large sorts without limits** — queries that sort hundreds of thousands or millions of rows using ORDER BY but never apply a LIMIT clause. If only the top results are needed, the sort is doing far more work than necessary.

Each detected anti-pattern includes the specific query, the user who ran it, the warehouse it ran on, the estimated cost wasted, and a plain-language recommendation for how to fix it.

### 4. Proactive Alerting with Microsoft Teams Integration

Rather than waiting for someone to open a dashboard and notice a problem, the framework actively monitors for cost anomalies and sends notifications directly to Microsoft Teams channels. Six alert types are built in:

- **Daily cost spike** — today's spend exceeds twice the 30-day rolling average. Something unusual is happening.

- **Warehouse idle** — a warehouse has been running for 30 or more minutes with zero queries. It is consuming credits for nothing.

- **Budget threshold** — monthly credit usage has crossed 80 percent of the budget (warning) or 100 percent (critical).

- **Heavy query spill** — a query spilled more than one gigabyte to remote storage. This specific query needs attention.

- **Storage growth anomaly** — a database's storage grew more than 20 percent in a single week. Something unexpected may have been loaded.

- **Repeated expensive query** — the same query has run more than 20 times today, costing more than a dollar each time. The results should be cached or the pattern should change.

Each alert type can be independently enabled or disabled. Teams channel routing can be toggled on or off for any alert. The system tracks alert episodes — if the same warehouse is idle for three consecutive check cycles, it sends one notification for the episode, not three. When the condition resolves, it logs the resolution. This prevents alert fatigue while ensuring nothing is missed.

The alerting runs on a Snowflake Task that executes every 15 minutes, calling a Python stored procedure that posts formatted Adaptive Cards to Teams via webhook. The entire pipeline is native to Snowflake — no external scheduler or middleware is needed.

### 5. Prioritised Savings Recommendations

The framework produces a unified, ranked list of every savings opportunity it has identified across all three categories (warehouse optimisation, query improvement, storage cleanup). Each recommendation includes:

- A plain-language description of what is happening and why it matters
- The current monthly cost of the inefficiency
- The estimated monthly savings if the recommendation is applied
- The effort level required (low for a configuration change, medium for a query rewrite, high for an architecture change)
- A confidence level based on the strength of the signal
- The actual SQL command to apply the fix, where applicable (such as an ALTER WAREHOUSE statement to resize a warehouse or reduce its auto-suspend timeout)
- A priority score that ranks recommendations by return on investment — highest savings relative to lowest effort appear first

This "Recommendations Hub" is the centrepiece of the client presentation. It answers the question every executive asks: "How much can we save, and what do we do first?"

### 6. Cost Forecasting

The framework projects future Snowflake costs using linear regression on the last 90 days of historical spending. Forecasts are generated at three levels:

- **Daily total cost forecast** — 90 days of forward projections with 95% confidence intervals, broken down by compute, storage, and serverless categories. The regression uses Snowflake's built-in `REGR_SLOPE` and `REGR_INTERCEPT` functions — no external packages required.

- **Monthly aggregated forecast** — daily projections rolled up into monthly totals, presented alongside the last three months of actuals for visual comparison. A "projected annual spend" figure combines year-to-date actuals with the remaining forecast for the current calendar year.

- **Per-team forecast** — each team's monthly cost trajectory is projected independently, giving finance teams early warning when a specific team's spend is accelerating. Teams require at least two months of historical data before projections are generated.

The forecasting page shows KPI cards (next month, next quarter, projected annual), an actuals-vs-forecast line chart with shaded confidence bands, a stacked bar chart of forecast by cost category, and a filterable team projections table.

### 7. Recommendation Tracking and ROI Verification

Recommendations alone are not enough — organisations need to track whether savings were actually realised. The framework adds a full recommendation lifecycle:

- **Status tracking** — each recommendation progresses through states: OPEN, ACCEPTED, IMPLEMENTED, REJECTED, or DEFERRED. Status is tracked via a seed file that operations teams update as they action recommendations.

- **Actual savings measurement** — for implemented warehouse recommendations, the framework compares the pre-recommendation cost (from the recommendation record) against the current 30-day cost of the target warehouse, calculating the real dollar savings achieved.

- **ROI computation** — actual savings divided by estimated savings gives a concrete ROI percentage for each implemented recommendation. This proves the framework's value to stakeholders with hard numbers.

- **Conversion funnel** — an ROI dashboard shows how many recommendations are open, accepted, implemented, rejected, and deferred, with total estimated and actual savings across all categories.

### 8. Seasonality-Aware Anomaly Detection

Simple threshold-based alerting generates false positives. The framework uses a seasonality-aware approach for cost spike detection:

- **Day-of-week baselines** — costs vary predictably by weekday (Monday ETL runs cost more than weekend idle time). The system computes 90-day averages per day of week.

- **Month-end adjustment** — batch processing at month-end naturally spikes costs. The system identifies the last three days of each month and applies a higher standard deviation threshold to avoid false alarms.

- **Trend adjustment** — if costs are growing steadily (as they do in most organisations), the baseline shifts upward with the trend. A linear regression on weekly totals adjusts the day-of-week average so that organic growth does not trigger alerts.

- **Z-score detection** — instead of a fixed "2x rolling average" rule, alerts fire when costs exceed the adjusted baseline by more than 2.0 standard deviations (configurable). This adapts to each organisation's natural cost patterns.

- **Graceful fallback** — when fewer than 30 days of history exist (new deployments), the system falls back to the simple multiplier approach until sufficient data accumulates.

### 9. Scheduled Executive Reports

Executives rarely log into dashboards. The framework delivers cost intelligence proactively via weekly email summaries:

- **Weekly comparison** — this week's cost vs last week's cost, with percentage change and direction indicator.

- **Top cost drivers** — the three highest-cost warehouses, surfacing where the money is going.

- **Top savings opportunities** — the three largest unrealised recommendations, showing what could be saved.

- **Alert summary** — count of new alert episodes in the past seven days.

- **Native delivery** — reports are sent via Snowflake's built-in `SYSTEM$SEND_EMAIL` function on a Monday 8 AM UTC schedule, requiring no external email infrastructure.

### 10. Data Freshness Transparency

Snowflake's `ACCOUNT_USAGE` views have up to 45 minutes of latency. Users viewing a dashboard need to know whether they are looking at current data or stale data. The framework:

- **Monitors six key data sources** — query history, warehouse metering, storage usage, warehouse load history, login history, and database storage history. For each source, it computes the age of the most recent record.

- **Classifies freshness** — FRESH (under 30 minutes), STALE (30 to 60 minutes), CRITICAL (over 60 minutes). The overall status reflects the worst-case source.

- **Displays a banner** — the main dashboard page shows a green, yellow, or red banner with the data timestamp and age. The Alert Management page shows per-source freshness detail.

---

## How It Works — The Architecture

The framework is built in four layers, each serving a distinct purpose:

### Layer 1: Staging

The staging layer connects to Snowflake's built-in `ACCOUNT_USAGE` metadata views — 14 views covering query history, warehouse metering, storage metrics, access history, login history, and serverless feature usage. These views contain 365 days of historical data at no additional cost to the customer.

The staging models clean and standardise this raw data: column names are normalised, timestamps are consistently typed, and each model mirrors exactly one source view. No business logic is applied here — the goal is a clean, reliable foundation.

### Layer 2: Intermediate (Business Logic)

The intermediate layer is where the intelligence lives. Over 40 models (31 intermediate and 10 alert models) perform cost calculations, utilisation analysis, anti-pattern detection, anomaly identification, alert state tracking, cost forecasting, seasonality baselines, recommendation lifecycle management, and data freshness monitoring.

Key computations include:
- Converting query execution time and warehouse size into estimated dollar cost per query
- Calculating warehouse utilisation as the ratio of active compute to total available capacity
- Detecting idle periods where warehouses are running but executing nothing
- Identifying query anti-patterns by analysing partition scan ratios, spill volumes, result set sizes, and query frequency
- Computing rolling averages and flagging anomalies where current costs exceed historical baselines
- Tracking alert episodes to prevent duplicate notifications
- Projecting future costs using linear regression on historical trends
- Computing seasonality-aware baselines for smarter anomaly detection
- Tracking recommendation lifecycle and calculating realised ROI
- Monitoring data source freshness to surface staleness to end users

### Layer 3: Publication (Dashboard-Ready)

The publication layer aggregates and shapes data for consumption by the Streamlit dashboard and the recommendations engine. These models are pre-computed, optimised for fast dashboard queries, and designed to answer specific business questions without requiring the dashboard to perform complex joins or calculations at query time.

### Layer 4: Presentation (Streamlit Dashboard)

The dashboard is a twelve-page interactive application running natively inside Snowflake via Streamlit-in-Snowflake. It requires no external hosting, no separate authentication, and no data movement. Users access it directly within their Snowflake environment. A data freshness banner on the main page shows whether the underlying data is current or stale.

The twelve pages are:

1. **Executive Summary** — total spend, month-over-month trends, compute/storage/serverless split, top warehouses and users by cost, data freshness banner
2. **Warehouse Deep Dive** — per-warehouse utilisation, idle time, queue contention, credit consumption, efficiency scoring
3. **Team Attribution** — cost broken down by team, drillable to individual users and query types
4. **Storage Explorer** — storage by database and table, unused table identification, Time Travel waste highlighting
5. **Trend Analysis** — 90-day cost trends with anomaly flags, day-of-week and hour-of-day heatmaps
6. **Alert Management** — active alerts, alert history, configuration status, pipeline health, seasonality-aware detection info, data source freshness
7. **Warehouse Optimiser** — right-sizing recommendations with before/after comparisons and SQL commands to apply
8. **Query Optimiser** — anti-pattern summary, top optimisation candidates ranked by waste, trend analysis
9. **Storage Optimiser** — unused tables, Time Travel waste, transient table candidates with savings estimates
10. **Recommendations Hub** — the unified savings report with ROI tracking dashboard, conversion funnel, and export capability
11. **Cost Forecast** — linear trend projections with confidence intervals, team forecasts, and projected annual spend
12. **Report Settings** — weekly executive report preview, recipient configuration, and manual trigger

---

## How We Prove It Works — The Demo Approach

A framework that only analyses historical data cannot demonstrate its detection capabilities convincingly. If we tell a client "this will catch full table scans," they will ask "show me." To make that possible, we take an active approach:

### Environment Discovery

Before generating any workload, we scan the actual Snowflake environment to understand what exists: which databases and tables are available, how large they are, which warehouses are provisioned, and what roles and users are active. This scan produces a complete inventory that informs which tables to target and which warehouses to use.

In our demo environment, we discovered 26 databases, including a TPC-DS dataset at 10TB scale with tables containing billions of rows — ideal for demonstrating detection on realistic data volumes.

### Intentional Workload Generation

We created a workload generator that executes ten carefully designed scenarios, each targeting a specific detection capability of the framework:

**Anti-Pattern Scenarios:**
- Scanning 1.3 billion rows of inventory data without filters to trigger the full table scan detector
- Running SELECT * against a 65-million-row customer table to trigger the select-star detector
- Executing memory-intensive window functions on a small warehouse to force spill-to-storage
- Running the same join query 25 times with minor variations to trigger the repeated query detector
- Joining two dimension tables without a proper ON condition to create a cartesian product
- Sorting a million-row table with ORDER BY but no LIMIT to trigger the large sort detector

**Cost and Attribution Scenarios:**
- Running the same expensive join on three different warehouses (X-Small, Small, Medium) to demonstrate that the framework correctly attributes different costs to different teams based on warehouse assignment
- Executing four expensive queries in rapid succession on a single warehouse to create a daily cost spike that triggers the anomaly alert
- Resuming a warehouse and running no queries on it to trigger the idle warehouse alert

Every workload query is tagged with a structured label identifying the team, the scenario name, and a unique run identifier. The dbt models parse these tags to perform team-level attribution, creating a complete chain from "this team ran this query" to "this query cost this much" to "this anti-pattern was detected."

### Demo Flow

The demonstration follows a narrative arc:

1. **"Let me run a query"** — execute one of the workload scenarios while the client watches
2. **"Now let's see what the framework caught"** — open the Query Optimiser page and show the detected anti-pattern
3. **"Here's the cost impact"** — switch to the Executive Summary to show how the query affected overall cost
4. **"And here's the recommendation"** — switch to the Recommendations Hub to show the prioritised fix
5. **"This works across teams"** — run the multi-warehouse scenario to show cost attribution across teams
6. **"And it alerts automatically"** — show the Alert Management page where the cost spike triggered a Teams notification

This approach turns the framework from a passive reporting tool into a live, interactive demonstration of detection, attribution, and recommendation capabilities.

---

## What Makes This Different

### Compared to Snowflake's Built-In Cost Tools

Snowflake provides resource monitors and budgets that can alert when spending crosses a threshold. But they only tell you *that* costs are high — not *why* they are high or *what* to do about it. Our framework adds the analytical intelligence layer: root cause analysis, anti-pattern detection, team attribution, and prioritised recommendations with dollar estimates.

### Compared to Third-Party SaaS Tools

Solutions like Select.dev, Keebo, and Sundeck provide similar functionality, but with significant trade-offs:

- **Data leaves the environment.** Third-party tools require exporting Snowflake metadata to their cloud infrastructure. Our framework keeps everything inside the customer's Snowflake account.

- **Ongoing licence costs.** SaaS tools charge $500 to $5,000 or more per month. Our framework has no recurring licence fees — dbt and Streamlit are included with Snowflake.

- **Generic, not tailored.** SaaS tools provide one-size-fits-all analysis. Our framework integrates the customer's team structure, role conventions, query tagging practices, and organisational mapping for context-aware attribution.

- **Black-box recommendations.** SaaS tools generate recommendations from proprietary algorithms. Our framework uses transparent dbt SQL models — every calculation is visible, auditable, and customisable.

- **Vendor roadmap dependency.** SaaS tools evolve on the vendor's schedule. Our framework can be extended by the customer's own team — add a new alert type, a new anti-pattern detector, or a new dashboard page at any time.

### Compared to Manual Analysis

Many organisations rely on ad hoc SQL queries against `ACCOUNT_USAGE` views, run by a platform engineer when someone asks "why is the bill so high this month?" This approach is reactive, inconsistent, and not scalable. Our framework automates the analysis, runs it continuously, and presents the results in a format that non-technical stakeholders can understand and act on.

---

## The Delivery Timeline

The framework is delivered in two phases over eight weeks:

### Phase 1: Cost Visibility and Attribution (Weeks 1 through 4)

**Week 1** establishes the foundation. We set up the dbt project, connect to the 14 Snowflake metadata sources, and build the staging layer that cleans and standardises the raw data. We also load the configuration files that define credit pricing, alert thresholds, and budget targets.

**Week 2** builds the intelligence layer. Intermediate models perform cost attribution at every level — per warehouse, per team, per user, per query. They calculate utilisation metrics, detect idle periods, identify cost anomalies, and compute rolling averages. Publication models shape this data for the dashboard.

**Week 3** delivers the interactive dashboard. Five pages cover the executive summary, warehouse deep-dive, team attribution, storage exploration, and trend analysis. The dashboard is deployed natively inside Snowflake and accessible to anyone with the appropriate role.

**Week 4** adds the alerting system. Seven alert types are implemented with episode-based deduplication. Microsoft Teams integration delivers formatted notifications to designated channels. A Snowflake Task automates the pipeline to run every 15 minutes. An alert management page is added to the dashboard.

At the end of Phase 1, the customer has a fully operational cost visibility system with automated alerting.

### Phase 2: Query Optimisation and Recommendations (Weeks 5 through 8)

**Week 5** tackles warehouse right-sizing. Four models analyse execution times, queue wait times, spill rates, and hourly utilisation patterns to generate resize, auto-suspend, and scheduling recommendations with dollar savings estimates.

**Week 6** builds the query anti-pattern engine. Seven detection models identify full table scans, SELECT * usage, spill-to-storage events, repeated queries, cartesian joins, and unnecessary large sorts. Each finding includes the wasteful query, its cost, and a fix recommendation.

**Week 7** addresses storage optimisation. Five models identify unused tables, Time Travel waste, transient table candidates, and stale clones. A unified recommendations model merges all findings — warehouse, query, and storage — into a single prioritised report ranked by return on investment.

**Week 8** is dedicated to polish, testing, and documentation. The full test suite validates every model. The dashboard is refined. Documentation is completed. The team conducts a final stakeholder walkthrough and knowledge transfer.

At the end of Phase 2, the customer has a complete optimisation engine with a prioritised savings report showing exactly how much can be saved and where to start.

---

## What the Customer Gets

At the conclusion of the eight-week engagement, the customer receives:

1. **A production-ready dbt project** with 72 tested, documented data models and 6 configuration seeds that transform Snowflake's raw metadata into actionable cost intelligence, including cost forecasting, seasonality baselines, recommendation lifecycle tracking, and data freshness monitoring.

2. **A twelve-page interactive dashboard** running natively in Snowflake, providing cost visibility from executive summary down to individual query level, plus cost forecasting with confidence intervals and an ROI tracking dashboard.

3. **An automated alerting pipeline** that monitors costs every 15 minutes using seasonality-aware anomaly detection and sends notifications to Microsoft Teams when anomalies or threshold breaches occur.

4. **A prioritised savings report** that quantifies every identified optimisation opportunity in dollars, ranks them by effort and impact, provides the exact SQL to apply each fix, and tracks implementation status with realised ROI verification.

5. **Cost forecasting** that projects next month, next quarter, and annual spend using linear regression with 95% confidence intervals, broken down by cost category and by team.

6. **Weekly executive email reports** delivered automatically via Snowflake's native email integration, summarising costs, trends, top drivers, savings opportunities, and alert activity.

7. **A workload generation toolkit** that can reproduce any anti-pattern or cost scenario on demand, enabling the team to validate the framework's detection capabilities at any time.

8. **Complete documentation** including a user guide for the dashboard, a technical guide for extending the framework, and an operational runbook for maintaining the pipeline.

9. **A reusable accelerator** that can be deployed to any Snowflake customer environment. The framework dynamically reads the target environment's metadata — databases, warehouses, roles, users — and adapts its analysis accordingly, with no manual configuration beyond credit pricing and alert preferences.

---

## The Technical Foundation

The framework is built on technologies that are already part of the Snowflake ecosystem:

- **dbt (data build tool)** — the industry-standard framework for data transformation, providing version control, automated testing, documentation, and incremental processing. All business logic is expressed as SQL models that are transparent, auditable, and extensible.

- **Streamlit in Snowflake** — Snowflake's native application framework for building interactive dashboards. No separate hosting, no additional authentication, no data extraction required.

- **Snowflake Tasks** — native scheduling for automated pipeline execution. The alert pipeline runs every 15 minutes; the full model refresh runs every six hours.

- **Snowflake External Access** — enables the Teams webhook integration by allowing a stored procedure to make HTTPS calls to Microsoft's webhook endpoint, controlled by network rules and secret management.

- **SNOWFLAKE.ACCOUNT_USAGE** — the 14 metadata views that Snowflake maintains automatically, containing 365 days of query history, warehouse metering, storage metrics, access patterns, and serverless feature usage. This data is available at no additional cost.

No additional software licences, external infrastructure, or ongoing subscription fees are required.

---

## Deployment Steps

### Step 1: Git Repository Integration in Snowflake

The dbt project is deployed to Snowflake via native Git integration. This allows Snowflake to pull the latest code directly from the repository.

```sql
-- Create Git Repository (reuses existing API integration and credentials)
CREATE GIT REPOSITORY COST_OPTIMIZATION_DB.PUBLIC.cost_optimization_repo
  API_INTEGRATION = GLOBALMART_GIT_INTEGRATION
  GIT_CREDENTIALS = GLOBALMART.RAW.GIT_SECRET
  ORIGIN = 'https://github.com/srinivasaddanki1978/data-product.git';
```

Alternatively, via Snowflake UI:
1. Navigate to **Projects** → **dbt Projects** → click **"Add New"**
2. Select **"Create from Git Repository"**
3. Repository URL: `https://github.com/srinivasaddanki1978/data-product.git`
4. API Integration: `GLOBALMART_GIT_INTEGRATION`
5. dbt project subdirectory: `cost_optimization_dbt`

### Step 2: Deploy and Run the dbt Project

```bash
# Deploy (uploads project to Snowflake)
snow dbt deploy cost_optimization --force

# Load reference data (credit pricing, alert config, budgets)
snow dbt execute cost_optimization seed

# Run all 72 models (staging → intermediate → publication → alerts)
snow dbt execute cost_optimization run

# Validate data quality
snow dbt execute cost_optimization test
```

### Step 3: Automated Daily Refresh Schedule

Three Snowflake Tasks refresh the pipeline throughout the business day:

| Task | Schedule (IST) | CRON (UTC) | Purpose |
|------|---------------|------------|---------|
| `refresh_cost_models_morning` | 10:30 AM | `0 5 * * *` | Captures data through ~9:45 AM |
| `refresh_cost_models_midday` | 1:00 PM | `30 7 * * *` | Captures data through ~12:15 PM |
| `refresh_cost_models_afternoon` | 4:00 PM | `30 10 * * *` | Captures data through ~3:15 PM |

Enable the tasks:
```sql
ALTER TASK refresh_cost_models_morning RESUME;
ALTER TASK refresh_cost_models_midday RESUME;
ALTER TASK refresh_cost_models_afternoon RESUME;
```

### Step 4: Deploy Streamlit Dashboard

```bash
cd streamlit_app
snow streamlit deploy --connection cost_optimization
```

---

## Summary

The Snowflake Cost Optimisation Framework transforms Snowflake's built-in metadata from raw operational data into a strategic asset for cost management. It answers the questions that matter — who is spending how much, why costs are growing, what is being wasted, and what to do about it — and delivers those answers through an interactive dashboard, automated alerts, and a prioritised savings report.

It runs entirely inside the customer's Snowflake account, requires no external tools or ongoing fees, and can be deployed to any Snowflake environment as a reusable accelerator.

The framework does not just show what happened. It forecasts what will happen next, detects problems as they occur using seasonality-aware intelligence, explains why they matter, tells you exactly how to fix them, tracks whether the fixes worked, and delivers executive summaries without anyone needing to log in.

---

*Prepared by Bilvantis — Snowflake Cost Optimisation Practice*
