# PO Control Utility

Enable or disable Databricks **Predictive Optimization (PO)** at catalog, schema or table scope,
driven by a CSV in a Unity Catalog Volume. Implements
`databricks_po_control_design_for_claude.pdf` (v1.0).

```
po_wrapper  --creates/updates-->  Databricks Job  --runs-->  po_worker  -->  XLSX report in the Volume
(widgets, validation, polling)    (1 notebook task)          (validates CSV, runs the ALTERs)
```

## Project layout

| Path | What it is |
|---|---|
| `notebooks/po_wrapper.py` | Wrapper notebook: widgets, validation, creates/updates the job, starts and polls the run |
| `notebooks/po_worker.py` | Worker notebook: validates the CSV, runs one `ALTER ... PREDICTIVE OPTIMIZATION` per target, writes the report |
| `tests/` | Unit tests (Spark and Jobs API mocked): `python3 -m pytest -q` |
| `sample/` | Example input CSVs (`po_mapping.csv` targets the `po_ctl_*` test objects) |
| `scripts/deploy.sh` | Imports both notebooks into a workspace |
| `integration/` | Tooling and scenario CSVs for testing against a real workspace (see below) |
| `databricks_po_control_design_for_claude.pdf` | The design this implements |

## Quick start
1. Deploy: `scripts/deploy.sh <cli-profile>` (imports to `/Shared/po_control/`). `po_wrapper` and
   `po_worker` must stay in the same folder: the wrapper finds the worker as its sibling.
2. Put a CSV in a Unity Catalog Volume. The header must be exactly
   `object_type,catalog,schema,table,action` (see `sample/`).
3. Open `po_wrapper`, **run the first code cell** (it creates the widgets), set the four widgets, then
   run the remaining cells. Nothing is created or started until the last cell.

Run identity needs `MANAGE` or ownership on the targets (or `CREATE` on a schema for schema scope)
plus read/write on the Volume.

## Wrapper widgets
| Widget | Meaning |
|---|---|
| `csv_path` | Input CSV, `/Volumes/<catalog>/<schema>/<volume>/.../x.csv` |
| `report_path` | The **folder** for the report: an existing folder in the **same volume** as the CSV (the volume itself is fine), e.g. `/Volumes/cat/schema/vol`. No file name: it is generated |
| `compute_type` | `serverless` (default), `job_cluster` or `all_purpose`. Never silently replaced by another type |
| `all_purpose_cluster_id` | Required for `all_purpose`, must be blank otherwise |

Everything else is fixed (there are no widgets for it):

| Setting | Value |
|---|---|
| Job name / mode | `po-control`, always create-or-update: the job is created, or its whole definition is overwritten if it exists. A job with an active run is never modified |
| `job_cluster` | Single node, `Standard_D4s_v3` (Dsv3), Databricks Runtime `17.3.x-scala2.13` (17.3 LTS), single-user access mode. Checked against the workspace first; if unavailable it fails with a clear error instead of hanging in PENDING |
| Worker notebook | `po_worker` in the same folder as `po_wrapper` (derived from the notebook's own path) |
| Waiting | Always waits, at most 180 minutes. On timeout the run is **not** cancelled and no final state is guessed |
| Report file name | Generated: `po_report_<UTC YYYYMMDDTHHMMSSZ>.xlsx` inside `report_path`, so every run gets a new file and no report is ever overwritten. The folder must exist (it is not created) |
| Volume root | Derived from `csv_path` (`/Volumes/<catalog>/<schema>/<volume>`) and handed to the worker, which re-validates both paths against it |

## Behaviour
* Valid SQL only: `object_type` and `action` come from allowlists, identifiers are backtick-quoted
  (embedded backticks doubled). Identifiers containing `.`, `/`, whitespace or control characters
  are rejected. Matching is case-insensitive (Unity Catalog semantics).
* Duplicates run once (`SKIPPED_DUPLICATE`). Conflicting actions on one target: every row is
  `SKIPPED_CONFLICT`, nothing runs.
* One exception boundary per statement, no retries. Row-level failures never fail the task.
* Bad paths/header, unwritable report location or a report-write failure **do** fail the task, and
  DDL never runs before the path, CSV and report-location checks pass.
* The report (`Summary` + `Details` sheets) neutralizes spreadsheet formulas and records the compute
  type. `SUCCESS` means the `ALTER` returned without error, not that every child's effective
  setting changed (parents do not override explicitly configured children).
* Statement syntax verified against the Azure Databricks docs:
  `ALTER CATALOG|SCHEMA|TABLE <name> {ENABLE|DISABLE} PREDICTIVE OPTIMIZATION`
  (tables: managed Delta only).

## Testing
**Unit:** `pip install -r requirements-dev.txt && python3 -m pytest -q`

**Live (`integration/`)**, against a workspace profile and a SQL warehouse ID:

| Tool | Purpose |
|---|---|
| `sql.py <profile> <warehouse_id> "<sql>" ...` | Run SQL (set up test objects) |
| `po_state.py <profile> <warehouse_id> [reset]` | Independently read the effective PO setting of the `po_ctl_*` objects |
| `it_run.py <profile> <csv> [--compute ..] [--cluster-id ..] ...` | Behaves like a user of the wrapper; prints job/run IDs and the report; appends to `runs_log.jsonl` |
| `it_worker_direct.py <profile> key=value ...` | Calls the worker directly, bypassing the wrapper |
| `cluster_diag.py <profile> <cluster_id> <file.py>` | Runs a snippet on an all-purpose cluster |
| `scenarios/` | Scenario CSVs (happy path, mixed bad rows, empty, bad header, bulk, permission denied, parent/child) |

Test objects (same in each workspace): catalogs `po_ctl_admin` (schema `ops`, Volume `po_control`),
`po_ctl_t1` (`sales_stage`, `sales_curated`: `orders`, `customers`, `raw_events`, view `v_orders`),
`po_ctl_t2` (`finance.ledger`) and `po_ctl_t3` (`locked.secret_tbl`; in one workspace its ownership
was moved to a service principal so the run identity gets a real permission denial).

## Findings from live testing
1. Serverless has no `openpyxl`: the job spec declares a serverless environment that installs it.
2. **Serverless auto-retries failed tasks by default**, which would silently re-run DDL. Tasks set
   `max_retries: 0` and (serverless) `disable_auto_optimization: true`.
3. Catalogs created on default-storage workspaces need an explicit `MANAGED LOCATION`.
4. A node type whose `node_info.status` is non-empty (e.g. `NotEnabledOnSubscription`) is not
   launchable: `Standard_DS3_v2` hung a job cluster in PENDING. The wrapper therefore verifies the
   fixed runtime and node type against the workspace before creating the job.
5. Workspaces with an IP access list can block classic clusters' egress IPs. Then **every** Unity
   Catalog operation on classic compute fails (Volumes via FUSE, `dbutils`, Spark, even `ALTER`)
   with `403 ... blocked by Databricks IP ACL`. Only serverless works there. The worker fails safely
   in preflight, before any DDL.
