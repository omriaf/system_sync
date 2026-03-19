# System Tables Sync & Usage Dashboard

A Databricks Asset Bundle (DAB) that dynamically copies all accessible system tables into a `system_sync` catalog and deploys a detailed usage analytics dashboard on top of them.

## What This Bundle Does

### 1. Scheduled Job (two tasks)

A Databricks job that runs daily at **6:00 AM UTC**:

| Task | Type | What it does |
|---|---|---|
| **`create_catalog`** | SQL notebook (runs on SQL warehouse) | Runs `CREATE CATALOG IF NOT EXISTS system_sync` to ensure the target catalog exists |
| **`run_sync_pipeline`** | SDP pipeline (runs on serverless) | Discovers and copies all accessible system tables into `system_sync` (see below) |

The `run_sync_pipeline` task depends on `create_catalog` — the catalog is always guaranteed to exist before the pipeline starts.

### 2. SDP Pipeline: Dynamic System Table Sync

A serverless Spark Declarative Pipeline (SDP) that **automatically discovers and copies all readable system tables** from the `system` catalog into the `system_sync` catalog as physical Delta tables.

**How it works:**

- At pipeline planning time, the code enumerates all schemas and tables under the `system` catalog
- For each table, it verifies read access with a zero-row test query
- Accessible tables are registered as `@dp.table` targets in `system_sync.<schema>.<table>`
- Target schemas are created automatically as needed
- Skips `information_schema` (metadata views) and `ai` (typically empty)
- Tables are fully overwritten on each run (complete refresh)

**Additionally**, the pipeline creates a derived `effective_prices` table that intelligently selects between `account_prices` (customer-negotiated pricing) and `list_prices` (public pricing), depending on which has valid data.

![SDP Pipeline DAG](images/SDP.png)

### 3. Lakeview Dashboard: Detailed Usage Analytics

A multi-page AI/BI dashboard that reads from a subset of the synced tables. Because the dashboard queries physical Delta tables (not system tables directly), it benefits from fast load times and responsive filter interactions.

![Usage Dashboard](images/Dashboard.png)

| Page | Description |
|---|---|
| **Executive Summary** | High-level spend overview: contract dates, commitment tracking, discount %, cloud breakdown |
| **By Compute** | Drill into clusters, notebooks, pipelines, and SQL endpoints — top-N, daily/monthly trends |
| **By Job** | Job-level cost attribution with run-level detail, compute type breakdown |
| **By User** | Per-user consumption with product and shared-compute breakdowns |
| **By Query** | Query-level runtime analysis by client app, execution status, compute type |
| **Performance Tuning** | Compute and query performance metrics for optimization |
| **Miscellaneous** | Remaining product-level spend not covered by the above pages |

**Tables used by the dashboard:**

- `system_sync.billing.usage`
- `system_sync.billing.effective_prices` *(derived)*
- `system_sync.compute.clusters`
- `system_sync.compute.node_timeline`
- `system_sync.compute.warehouses`
- `system_sync.lakeflow.jobs`
- `system_sync.lakeflow.job_run_timeline`
- `system_sync.query.history`

> The pipeline syncs **all** accessible system tables — not just the ones above — so additional tables are available for ad-hoc queries and future dashboards.

## Architecture

```
                    Scheduled Job (daily 6 AM UTC)
                    ┌─────────────────────────────────────────────┐
                    │                                             │
                    │  Task 1: create_catalog                     │
                    │  ┌───────────────────────────────────┐      │
                    │  │ CREATE CATALOG IF NOT EXISTS       │      │
                    │  │ system_sync                        │      │
                    │  └──────────────┬────────────────────┘      │
                    │                 │ then                       │
                    │                 ▼                            │
                    │  Task 2: run_sync_pipeline                  │
                    │  ┌───────────────────────────────────┐      │
                    │  │ SDP Pipeline (serverless)         │      │
  system.*.*  ─────────►  • discover all system tables     │      │
  (all accessible   │  │  • copy to system_sync.*          │      │
   tables)          │  │  • create effective_prices        │      │
                    │  └──────────────┬────────────────────┘      │
                    │                 │                            │
                    └─────────────────┼────────────────────────────┘
                                      │ writes
                                      ▼
                             ┌─────────────────┐
                             │  system_sync.*   │
                             │  (Delta tables)  │
                             └────────┬────────┘
                                      │ reads
                                      ▼
                             ┌─────────────────┐
                             │  Lakeview        │
                             │  Dashboard       │
                             └─────────────────┘
```

## Prerequisites

### Required Permissions

| Permission | Why |
|---|---|
| **CREATE CATALOG** on the metastore | The job's first task creates the `system_sync` catalog |
| **USE CATALOG** on `system` | To read source system tables |
| **SELECT** on `system.*.*` | To read individual system tables (the pipeline gracefully skips any tables you can't access) |

> **Note:** System tables are enabled at the account level. If `system.billing.usage` or other tables are not visible in your workspace, ask your account admin to [enable system tables](https://docs.databricks.com/en/administration-guide/system-tables/index.html).

### Required Infrastructure

| Resource | Details |
|---|---|
| **SQL Warehouse** | Any running SQL warehouse — Serverless, Pro, or Classic. You'll need its **warehouse ID**. |
| **Unity Catalog** | Must be enabled on the workspace |
| **Serverless compute** | The SDP pipeline runs on serverless — must be enabled on the workspace |

### Finding Your Warehouse ID

In the Databricks UI: **SQL Warehouses** → click your warehouse → the ID is in the URL.

Or via CLI:

```bash
databricks warehouses list --output json | jq '.[] | {name, id, state}'
```

## Installation

This bundle is deployed using the **Databricks CLI**. DABs (Databricks Asset Bundles) are a CLI-only deployment mechanism — there is no way to deploy a bundle from the UI.

### Step 1: Install and Configure the Databricks CLI

Install the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html) (v0.230+), then configure authentication:

```bash
databricks configure
```

You'll be prompted for your workspace URL and a personal access token. Alternatively, use OAuth:

```bash
databricks auth login --host https://my-workspace.cloud.databricks.com
```

To use a named profile:

```bash
databricks configure --profile my-workspace
```

### Step 2: Get the Bundle

```bash
# From zip:
unzip system_sync.zip && cd system_sync

# From git:
git clone <repo-url> && cd system_sync
```

### Step 3: Validate

```bash
databricks bundle validate --var="warehouse_id=<YOUR_WAREHOUSE_ID>"
```

With a named profile:

```bash
databricks bundle validate --var="warehouse_id=<YOUR_WAREHOUSE_ID>" --profile my-workspace
```

Expected output: `Validation OK!`

### Step 4: Deploy

```bash
databricks bundle deploy --var="warehouse_id=<YOUR_WAREHOUSE_ID>"
```

This creates three resources in your workspace:
- **Spark Declarative Pipeline**: `[dev <your_user>] system_tables_daily_sync`
- **Job**: `[dev <your_user>] system_tables_daily_sync_job`
- **Dashboard**: `[dev <your_user>] Detailed Usage Dashboard- Account Pricing`

### Step 5: Run the Initial Sync

```bash
databricks bundle run system_sync_daily --var="warehouse_id=<YOUR_WAREHOUSE_ID>"
```

The first run will:
1. Create the `system_sync` catalog
2. Discover all accessible system tables
3. Create schemas and copy all tables into `system_sync`
4. Build the derived `effective_prices` table

Typical runtime: **5–15 minutes** (depends on data volume).

### Step 6: Open the Dashboard

Navigate in the Databricks UI to **SQL** → **Dashboards** → search for **"Detailed Usage Dashboard- Account Pricing"**.

## After Deployment: Managing Resources in the UI

Once deployed, all resources are fully manageable from the Databricks UI:

- **Job**: **Workflows** → **Jobs** → `system_tables_daily_sync_job` — view runs, trigger manually, edit schedule
- **Pipeline**: **Pipelines** → `system_tables_daily_sync` — monitor updates, view lineage graph
- **Dashboard**: **SQL** → **Dashboards** → `Detailed Usage Dashboard- Account Pricing` — interact with filters, share with others
- **Tables**: **Catalog** → `system_sync` — browse all synced system tables

To trigger a manual refresh from the UI, go to **Workflows** → **Jobs** → click **Run now**.

## Production Deployment

```bash
databricks bundle deploy -t prod --var="warehouse_id=<YOUR_WAREHOUSE_ID>"
databricks bundle run system_sync_daily -t prod --var="warehouse_id=<YOUR_WAREHOUSE_ID>"
```

## Project Structure

```
system_sync/
├── databricks.yml                                    # Bundle config (single variable: warehouse_id)
├── pyproject.toml                                    # Python project metadata
├── README.md
├── resources/
│   ├── system_sync_etl.pipeline.yml                  # SDP pipeline definition (serverless, photon)
│   ├── system_sync_job.job.yml                       # Job: create_catalog → run_sync_pipeline
│   └── dashboard.yml                                 # Lakeview dashboard resource
└── src/
    ├── create_catalog.sql                            # SQL: CREATE CATALOG IF NOT EXISTS system_sync
    ├── dashboard.lvdash.json                         # Dashboard layout and queries
    └── system_sync_etl/
        └── transformations/
            └── sync_system_tables.py                 # Dynamic discovery + table sync + effective_prices
```

## Customization

### Change the Schedule

Edit `resources/system_sync_job.job.yml` (CLI) or the job's schedule in the UI:

```yaml
schedule:
  quartz_cron_expression: "0 0 6 * * ?"  # Default: daily at 6 AM UTC
  timezone_id: UTC
```

### Skip Additional Schemas

Edit `SKIP_SCHEMAS` in `sync_system_tables.py`:

```python
SKIP_SCHEMAS = {"ai", "information_schema", "storage"}  # add schemas to skip
```

## Troubleshooting

| Issue | Solution |
|---|---|
| `CATALOG_NOT_FOUND: system` | System tables are not enabled. Contact your account admin. |
| `PERMISSION_DENIED: CREATE CATALOG` | You need metastore admin or `CREATE CATALOG` privilege. |
| Pipeline shows 0 tables discovered | Check that you have `SELECT` on at least some `system.*` tables. |
| Dashboard shows no data | Ensure the job completed successfully. Check **Workflows** → **Jobs**. |
| `Warehouse not found` | Verify the warehouse ID is correct and the warehouse is running. |
