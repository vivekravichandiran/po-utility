# Databricks notebook source
# MAGIC %md
# MAGIC # PO Control - Wrapper
# MAGIC Reads four widgets, validates them, creates or updates the one-task Databricks Job `po-control`
# MAGIC that runs the `po_worker` notebook (the sibling of this notebook), starts a run, waits for it and
# MAGIC prints `job_id`, `run_id`, state and report path.
# MAGIC
# MAGIC **Widgets**
# MAGIC * `csv_path` - input CSV in a Unity Catalog Volume.
# MAGIC * `report_path` - the **folder** the report is written to (an existing folder in the same Volume
# MAGIC   as the CSV; the Volume itself is fine). The file name is generated:
# MAGIC   `po_report_<UTC YYYYMMDDTHHMMSSZ>.xlsx`, so every run gets a new report and none is overwritten.
# MAGIC * `compute_type` - chosen explicitly, never silently replaced by another:
# MAGIC   * `serverless` - serverless jobs compute (default).
# MAGIC   * `job_cluster` - a new single-node classic job cluster: DBR 17.3 LTS on `Standard_D4s_v3`.
# MAGIC   * `all_purpose` - an existing all-purpose cluster.
# MAGIC * `all_purpose_cluster_id` - required for `all_purpose`, must be blank otherwise.
# MAGIC * Remote workspace (all five blank = local only; otherwise all five are required). Used by CSV rows
# MAGIC   whose `workspace` column is `remote`; rows marked `local` run on the compute chosen above:
# MAGIC   * `remote_workspace_url` - e.g. `https://adb-1234567890123456.7.azuredatabricks.net`
# MAGIC   * `remote_client_id` - client (application) ID of an OAuth service principal in that workspace
# MAGIC   * `remote_secret_scope`, `remote_secret_key` - the secret that holds the OAuth client **secret**.
# MAGIC     The secret itself is never typed into a widget (widget values become visible job parameters);
# MAGIC     the worker reads it from the secret scope at run time.
# MAGIC   * `remote_warehouse_id` - ID of a SQL warehouse in that workspace the service principal can use
# MAGIC
# MAGIC **Fixed behaviour (no widgets)**
# MAGIC * The job is always named `po-control` and is created, or updated (definition overwritten) if it
# MAGIC   exists. A job with an active run is never modified.
# MAGIC * The worker notebook is always `po_worker` in the same folder as this notebook.
# MAGIC * The run is always waited for, for at most 180 minutes. On timeout the run is **not** cancelled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widgets - run this cell first
# MAGIC The input widgets appear at the top of the notebook after this cell runs. Set the values, then
# MAGIC run the remaining cells (or **Run all**). Nothing is created or started until the last cell.

# COMMAND ----------

COMPUTE_TYPES = ("serverless", "job_cluster", "all_purpose")


def create_widgets():  # pragma: no cover - needs the Databricks `dbutils` object
    dbutils.widgets.text("csv_path", "/Volumes/admin/ops/po_control/po_operations.csv", "csv_path")
    dbutils.widgets.text("report_path", "/Volumes/admin/ops/po_control", "report_path (folder)")
    dbutils.widgets.dropdown("compute_type", "serverless", list(COMPUTE_TYPES), "compute_type")
    # all_purpose only: the existing all-purpose cluster the job will run on (required there).
    dbutils.widgets.text("all_purpose_cluster_id", "", "all_purpose_cluster_id (if compute_type=all_purpose)")
    # Remote workspace, used by CSV rows with workspace=remote. All blank = local only.
    dbutils.widgets.text("remote_workspace_url", "", "remote_workspace_url (https://adb-....azuredatabricks.net)")
    dbutils.widgets.text("remote_client_id", "", "remote_client_id (OAuth service principal client ID)")
    dbutils.widgets.text("remote_secret_scope", "", "remote_secret_scope (holds the OAuth client secret)")
    dbutils.widgets.text("remote_secret_key", "", "remote_secret_key (key of the OAuth client secret)")
    dbutils.widgets.text("remote_warehouse_id", "", "remote_warehouse_id (SQL warehouse in the remote workspace)")


if __name__ == "__main__":  # true inside a Databricks notebook, false when imported by the tests
    create_widgets()

# COMMAND ----------

import json
import os
import posixpath
import re
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

# COMMAND ----------

JOB_NAME = "po-control"
WORKER_NOTEBOOK_NAME = "po_worker"  # always the sibling of this notebook
MAX_WAIT_MINUTES = 180  # polling bound only; the run is never cancelled
JOB_CLUSTER_SPARK_VERSION = "17.3.x-scala2.13"  # DBR 17.3 LTS
JOB_CLUSTER_NODE_TYPE_ID = "Standard_D4s_v3"  # Dsv3 series; the job cluster is always single node
REMOTE_WIDGETS = ("remote_workspace_url", "remote_client_id", "remote_secret_scope", "remote_secret_key",
                  "remote_warehouse_id")
# The OAuth client secret is sent to this host by the worker, so only Databricks workspace hosts are accepted.
REMOTE_HOST_SUFFIXES = (".azuredatabricks.net", ".cloud.databricks.com", ".gcp.databricks.com",
                        ".databricks.azure.cn", ".databricks.azure.us", ".cloud.databricks.us")
_HOST_RE = re.compile(r"^[a-z0-9]([a-z0-9\-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9\-]*[a-z0-9])?)+$")
_SECRET_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")
_WAREHOUSE_ID_RE = re.compile(r"^[A-Za-z0-9]{8,64}$")
REPORT_FILE_PREFIX = "po_report_"  # report file: <report_path>/po_report_<UTC timestamp>.xlsx
TASK_KEY = "execute_po_operations"
ENVIRONMENT_KEY = "po_env"
ENVIRONMENT_VERSION = "3"  # serverless environment version; verified against the workspace
CLUSTER_KEY = "po_cluster"
# Access mode for NEW job clusters; Unity Catalog (and /Volumes) needs a UC-capable mode.
JOB_CLUSTER_DATA_SECURITY_MODE = "SINGLE_USER"
# Access modes on an existing all-purpose cluster that cannot use Unity Catalog / Volumes.
NON_UC_ACCESS_MODES = ("NONE", "LEGACY_TABLE_ACL", "LEGACY_PASSTHROUGH", "LEGACY_SINGLE_USER",
                       "LEGACY_SINGLE_USER_STANDARD")
POLL_INTERVAL_SECONDS = 10
TERMINAL_STATES = ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")


class ConfigError(Exception):
    """Invalid wrapper configuration; nothing has been created or started."""


class JobApiError(Exception):
    """Jobs API problem; carries any IDs known at the time of failure."""

    def __init__(self, message, job_id=None, run_id=None):
        super().__init__(message)
        self.job_id = job_id
        self.run_id = run_id


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration validation

# COMMAND ----------


def generate_report_path(report_folder: str, now: Optional[datetime] = None) -> str:
    """Full report file path inside `report_folder`: po_report_<UTC YYYYMMDDTHHMMSSZ>.xlsx."""
    now = now or datetime.now(timezone.utc)
    return posixpath.join(report_folder, "%s%s.xlsx" % (REPORT_FILE_PREFIX, now.strftime("%Y%m%dT%H%M%SZ")))


def _normalize_volume_path(path: str, label: str) -> str:
    raw = str(path).strip()
    if not raw:
        raise ConfigError("%s must not be empty" % label)
    if "\x00" in raw or "\n" in raw or "\r" in raw:
        raise ConfigError("%s contains illegal characters" % label)
    if not raw.startswith("/Volumes/"):
        raise ConfigError("%s must be an absolute /Volumes/... path: %r" % (label, raw))
    if ".." in raw.split("/"):
        raise ConfigError("%s must not contain '..' segments: %r" % (label, raw))
    return posixpath.normpath(raw)


def sibling_worker_path(wrapper_notebook_path: str) -> str:
    """Workspace path of po_worker: same folder as the wrapper (a referential, not a typed, path)."""
    wrapper = str(wrapper_notebook_path).strip()
    if not wrapper.startswith("/"):
        raise ConfigError("cannot determine the wrapper notebook path, got %r" % wrapper)
    return posixpath.join(posixpath.dirname(wrapper), WORKER_NOTEBOOK_NAME)


def normalize_workspace_url(value) -> str:
    """'https://<host>' for a Databricks workspace URL, else ConfigError (same rules as the worker)."""
    raw = str(value if value is not None else "").strip()
    if not raw:
        raise ConfigError("remote_workspace_url must not be empty")
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in raw):
        raise ConfigError("remote_workspace_url contains illegal characters")
    try:
        parts = urllib.parse.urlsplit(raw)
        port = parts.port
    except ValueError:
        raise ConfigError("remote_workspace_url is not a valid URL")
    if parts.scheme.lower() != "https":
        raise ConfigError("remote_workspace_url must start with https://")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise ConfigError("remote_workspace_url must not contain credentials")
    if port not in (None, 443):
        raise ConfigError("remote_workspace_url must not specify a port other than 443")
    if parts.path not in ("", "/"):
        raise ConfigError("remote_workspace_url must be just the workspace URL (https://<host>), no path")
    host = (parts.hostname or "").lower()
    if not _HOST_RE.match(host) or not host.endswith(REMOTE_HOST_SUFFIXES):
        raise ConfigError(
            "remote_workspace_url host %r is not a Databricks workspace host (expected one ending in %s)"
            % (host, ", ".join(REMOTE_HOST_SUFFIXES))
        )
    return "https://" + host


def validate_remote(url, client_id, secret_scope, secret_key, warehouse_id) -> Optional[Dict[str, str]]:
    """None when all five are blank (local only); a normalized dict when all are valid; else ConfigError."""
    values = dict(zip(REMOTE_WIDGETS, (url, client_id, secret_scope, secret_key, warehouse_id)))
    values = {k: str(v if v is not None else "").strip() for k, v in values.items()}
    if not any(values.values()):
        return None
    missing = [k for k, v in values.items() if not v]
    if missing:
        raise ConfigError(
            "remote workspace settings are incomplete, missing: %s (fill all five, or clear all five "
            "for local-only runs)" % ", ".join(missing)
        )
    url_norm = normalize_workspace_url(values["remote_workspace_url"])
    cid = values["remote_client_id"]
    if len(cid) > 128 or any(ch.isspace() or ord(ch) < 32 for ch in cid):
        raise ConfigError("remote_client_id must be at most 128 characters without whitespace")
    for label in ("remote_secret_scope", "remote_secret_key"):
        if not _SECRET_NAME_RE.match(values[label]):
            raise ConfigError("%s must be 1-128 characters of letters, digits, '_', '-' or '.'" % label)
    if not _WAREHOUSE_ID_RE.match(values["remote_warehouse_id"]):
        raise ConfigError("remote_warehouse_id must be 8-64 letters/digits (the warehouse ID, not its name)")
    return {
        "workspace_url": url_norm,
        "client_id": cid,
        "secret_scope": values["remote_secret_scope"],
        "secret_key": values["remote_secret_key"],
        "warehouse_id": values["remote_warehouse_id"],
    }


def volume_root_of(path: str, label: str) -> str:
    """'/Volumes/<catalog>/<schema>/<volume>' that contains `path` (which must be a file in it)."""
    parts = [p for p in path.split("/") if p]
    if len(parts) < 5:
        raise ConfigError("%s must be /Volumes/<catalog>/<schema>/<volume>/<file>: %r" % (label, path))
    return "/" + "/".join(parts[:4])


def validate_config(
    csv_path,
    report_path,
    worker_notebook_path,
    compute_type="serverless",
    all_purpose_cluster_id="",
    remote_workspace_url="",
    remote_client_id="",
    remote_secret_scope="",
    remote_secret_key="",
    remote_warehouse_id="",
    now: Optional[datetime] = None,
    exists: Callable[[str], bool] = os.path.exists,
    isfile: Callable[[str], bool] = os.path.isfile,
    isdir: Callable[[str], bool] = os.path.isdir,
) -> Dict[str, object]:
    """Validate all widget values; returns a normalized config dict or raises ConfigError."""
    csv_norm = _normalize_volume_path(csv_path, "csv_path")
    report_dir = _normalize_volume_path(report_path, "report_path")

    # The volume is derived from csv_path (there is no volume_root widget); the report folder must be
    # in the same volume. The worker re-validates the paths against this root.
    root = volume_root_of(csv_norm, "csv_path")
    if not csv_norm.lower().endswith(".csv"):
        raise ConfigError(
            "csv_path must be the full path of a .csv file (e.g. %s/po_operations.csv), not a folder: %s"
            % (root, csv_norm)
        )
    if report_dir.lower().endswith((".xlsx", ".xls", ".csv")):
        raise ConfigError(
            "report_path must be a folder (e.g. %s), not a file: the report file name "
            "(%s<timestamp>.xlsx) is generated for you: %s" % (root, REPORT_FILE_PREFIX, report_dir)
        )
    if report_dir != root and not report_dir.startswith(root + "/"):
        raise ConfigError("report_path %r must be in the same volume as csv_path (%s)" % (report_dir, root))
    if not isfile(csv_norm):
        raise ConfigError("csv_path does not exist or is not readable: %s" % csv_norm)
    if not isdir(report_dir):
        raise ConfigError("report_path folder does not exist (it is not created for you): %s" % report_dir)

    report_norm = generate_report_path(report_dir, now)
    if exists(report_norm):  # reports are never overwritten (only possible within the same second)
        raise ConfigError("report already exists and reports are never overwritten: %s" % report_norm)

    worker = str(worker_notebook_path).strip()
    if not worker or not worker.startswith("/"):
        raise ConfigError("worker notebook path must be an absolute workspace path, got %r" % worker)

    compute = str(compute_type).strip().lower()
    if compute not in COMPUTE_TYPES:
        raise ConfigError("compute_type must be one of %s, got %r" % (COMPUTE_TYPES, compute_type))
    cluster = {
        "cluster_id": str(all_purpose_cluster_id).strip(),
        "spark_version": JOB_CLUSTER_SPARK_VERSION,
        "node_type_id": JOB_CLUSTER_NODE_TYPE_ID,
        "num_workers": 0,  # single node
    }
    if compute == "all_purpose":
        if not cluster["cluster_id"]:
            raise ConfigError("all_purpose_cluster_id is required when compute_type=all_purpose")
    elif cluster["cluster_id"]:
        raise ConfigError(
            "all_purpose_cluster_id was provided but compute_type is %r; set "
            "compute_type=all_purpose or clear it (no silent fallback either way)" % compute
        )

    remote = validate_remote(remote_workspace_url, remote_client_id, remote_secret_scope,
                             remote_secret_key, remote_warehouse_id)

    return {
        "compute_type": compute,
        "cluster": cluster,
        "remote": remote,
        "volume_root": root,
        "csv_path": csv_norm,
        "report_folder": report_dir,
        "report_path": report_norm,
        "worker_notebook_path": worker,
        "job_name": JOB_NAME,
        "max_wait_minutes": MAX_WAIT_MINUTES,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Jobs API (all payload construction is isolated in `build_job_spec`)

# COMMAND ----------


def _job_cluster_spec(cluster: Dict[str, object]) -> Dict[str, object]:
    spec = {
        "spark_version": cluster["spark_version"],
        "node_type_id": cluster["node_type_id"],
        "num_workers": cluster["num_workers"],
        "data_security_mode": JOB_CLUSTER_DATA_SECURITY_MODE,
    }
    if cluster["num_workers"] == 0:  # single-node cluster
        spec["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"}
        spec["custom_tags"] = {"ResourceClass": "SingleNode"}
    return spec


def _remote_parameters(remote: Optional[Dict[str, str]]) -> Dict[str, str]:
    """Worker parameters for the remote workspace (blank when local only). Never the secret value."""
    r = remote or {}
    return {
        "remote_workspace_url": r.get("workspace_url", ""),
        "remote_client_id": r.get("client_id", ""),
        "remote_secret_scope": r.get("secret_scope", ""),
        "remote_secret_key": r.get("secret_key", ""),
        "remote_warehouse_id": r.get("warehouse_id", ""),
    }


def build_job_spec(cfg: Dict[str, object]) -> Dict[str, object]:
    """Jobs API 2.1 payload for a one-task notebook job. Compute is chosen explicitly.

    This is the only place that knows the API shape. There is never an automatic fallback
    from one compute type to another.

    serverless:   the task declares NO cluster; `openpyxl` comes from a job `environment`.
    job_cluster:  a new job cluster (`job_cluster_key`); `openpyxl` is a task library.
    all_purpose:  an existing all-purpose cluster (`existing_cluster_id`); `openpyxl` is
                  installed on it as a task library.

    Retries (found by live testing): serverless AUTOMATICALLY RETRIES failed tasks unless
    `disable_auto_optimization` is set, which would silently re-run DDL (e.g. after a
    report-write failure). `max_retries: 0` is always set; the design forbids DDL retries.
    """
    compute = cfg.get("compute_type", "serverless")
    task = {
        "task_key": TASK_KEY,
        "max_retries": 0,
        "notebook_task": {
            "notebook_path": cfg["worker_notebook_path"],
            "source": "WORKSPACE",
            "base_parameters": {
                "csv_path": cfg["csv_path"],
                "report_path": cfg["report_path"],
                "volume_root": cfg["volume_root"],
                "overwrite_report": "false",  # reports are never overwritten
                "compute_type": compute,
                **_remote_parameters(cfg.get("remote")),
                "job_id": "{{job.id}}",
                "run_id": "{{job.run_id}}",
            },
        },
    }
    spec = {
        "name": cfg["job_name"],
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "tasks": [task],
    }
    if compute == "serverless":
        task["environment_key"] = ENVIRONMENT_KEY
        task["disable_auto_optimization"] = True
        spec["environments"] = [
            {
                "environment_key": ENVIRONMENT_KEY,
                "spec": {"environment_version": ENVIRONMENT_VERSION, "dependencies": ["openpyxl"]},
            }
        ]
    elif compute == "all_purpose":
        task["libraries"] = [{"pypi": {"package": "openpyxl"}}]
        task["existing_cluster_id"] = cfg["cluster"]["cluster_id"]
    elif compute == "job_cluster":
        cluster = cfg["cluster"]
        if not cluster["spark_version"] or not cluster["node_type_id"]:
            raise ConfigError("job cluster needs spark_version and node_type_id")
        task["libraries"] = [{"pypi": {"package": "openpyxl"}}]
        task["job_cluster_key"] = CLUSTER_KEY
        spec["job_clusters"] = [
            {"job_cluster_key": CLUSTER_KEY, "new_cluster": _job_cluster_spec(cluster)}
        ]
    else:
        raise ConfigError("unknown compute_type %r" % compute)
    return spec


class JobsClient:
    """Thin wrapper over the REST API through a databricks-sdk WorkspaceClient."""

    def __init__(self, workspace_client):
        self._api = workspace_client.api_client
        self._ws = workspace_client

    def _do(self, method, path, **kw):
        return self._api.do(method, path, **kw) or {}

    def notebook_exists(self, path: str) -> bool:
        try:
            self._ws.workspace.get_status(path)
            return True
        except Exception:  # noqa: BLE001 - any failure means not usable
            return False

    def spark_versions(self) -> List[Dict[str, str]]:
        return self._do("GET", "/api/2.1/clusters/spark-versions").get("versions", [])

    def node_types(self) -> List[Dict[str, object]]:
        return self._do("GET", "/api/2.1/clusters/list-node-types").get("node_types", [])

    def get_cluster(self, cluster_id: str) -> Optional[Dict[str, object]]:
        try:
            return self._do("GET", "/api/2.1/clusters/get", query={"cluster_id": cluster_id})
        except Exception:  # noqa: BLE001 - any failure means not usable
            return None

    def find_jobs_by_name(self, name: str) -> List[int]:
        ids, token = [], None
        while True:
            q = {"name": name, "limit": 100}
            if token:
                q["page_token"] = token
            resp = self._do("GET", "/api/2.1/jobs/list", query=q)
            for j in resp.get("jobs", []):
                if j.get("settings", {}).get("name") == name:
                    ids.append(j["job_id"])
            token = resp.get("next_page_token")
            if not token:
                return ids

    def active_runs(self, job_id: int) -> List[int]:
        resp = self._do(
            "GET", "/api/2.1/jobs/runs/list", query={"job_id": job_id, "active_only": "true"}
        )
        return [r["run_id"] for r in resp.get("runs", [])]

    def create(self, spec) -> int:
        return self._do("POST", "/api/2.1/jobs/create", body=spec)["job_id"]

    def reset(self, job_id: int, spec) -> None:
        self._do("POST", "/api/2.1/jobs/reset", body={"job_id": job_id, "new_settings": spec})

    def run_now(self, job_id: int) -> int:
        return self._do("POST", "/api/2.1/jobs/run-now", body={"job_id": job_id})["run_id"]

    def get_run(self, run_id: int) -> Dict[str, object]:
        return self._do("GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id})

    def get_run_output(self, task_run_id: int) -> Dict[str, object]:
        return self._do("GET", "/api/2.1/jobs/runs/get-output", query={"run_id": task_run_id})


def verify_job_cluster_available(client, spark_version: str, node_type_id: str) -> None:
    """Fail clearly if the fixed runtime / node type cannot be launched in this workspace.

    Found by live testing: a node type that is not enabled on the subscription (non-empty
    `node_info.status`) leaves the cluster in PENDING instead of failing, so check up front.
    """
    if spark_version not in {v.get("key") for v in client.spark_versions()}:
        raise ConfigError("Databricks Runtime %s is not available in this workspace" % spark_version)
    node = next((n for n in client.node_types() if n.get("node_type_id") == node_type_id), None)
    if node is None:
        raise ConfigError("node type %s is not available in this workspace" % node_type_id)
    status = (node.get("node_info") or {}).get("status")
    if status or node.get("is_deprecated"):
        raise ConfigError("node type %s cannot be launched here (status=%s)" % (node_type_id, status))


def resolve_compute(client, cfg: Dict[str, object], log=print) -> Dict[str, object]:
    """Verify the compute is usable. Never falls back to another compute type."""
    compute = cfg.get("compute_type")
    if compute == "serverless":
        return cfg
    cluster = dict(cfg["cluster"])
    if compute == "all_purpose":
        info = client.get_cluster(cluster["cluster_id"])
        if not info:
            raise ConfigError(
                "all_purpose_cluster_id not found or not accessible: %s" % cluster["cluster_id"]
            )
        mode = str(info.get("data_security_mode", "")).upper()
        if mode in NON_UC_ACCESS_MODES:
            raise ConfigError(
                "cluster %s has access mode %s which cannot use Unity Catalog/Volumes; use a "
                "single-user or shared (USER_ISOLATION) cluster" % (cluster["cluster_id"], mode)
            )
        log("all-purpose cluster %s (%s, state=%s, access mode=%s)" % (
            cluster["cluster_id"], info.get("cluster_name"), info.get("state"), mode))
    else:  # job_cluster
        verify_job_cluster_available(client, cluster["spark_version"], cluster["node_type_id"])
        log("job cluster: %s" % cluster)
    out = dict(cfg)
    out["cluster"] = cluster
    return out


def ensure_job(client, cfg) -> int:
    """Create the job named cfg["job_name"], or update (overwrite) it if it already exists.

    "Update" replaces the whole job definition with this utility's definition. An existing
    job with an active run is never modified.
    """
    spec = build_job_spec(cfg)
    ids = client.find_jobs_by_name(cfg["job_name"])
    if len(ids) > 1:
        raise JobApiError(
            "Multiple jobs are named %r (ids %s); resolve manually" % (cfg["job_name"], ids)
        )
    if not ids:
        return client.create(spec)
    job_id = ids[0]
    active = client.active_runs(job_id)
    if active:
        raise JobApiError(
            "Job %s already has active run(s) %s; refusing to modify it or start another "
            "(same output paths must not be used concurrently)" % (job_id, active),
            job_id=job_id,
        )
    client.reset(job_id, spec)
    return job_id


def summarize_state(run: Dict[str, object]) -> Dict[str, str]:
    st = run.get("state", {}) or {}
    return {
        "life_cycle_state": st.get("life_cycle_state", "UNKNOWN"),
        "result_state": st.get("result_state", ""),
        "state_message": st.get("state_message", ""),
    }


def poll_run(client, run_id, max_wait_minutes, sleep=time.sleep, monotonic=time.monotonic,
             interval=POLL_INTERVAL_SECONDS, log=print):
    """Poll until terminal or timeout. Returns (run_dict, timed_out). Never cancels."""
    deadline = monotonic() + max_wait_minutes * 60
    last = {}
    last_life = None
    while True:
        last = client.get_run(run_id)
        s = summarize_state(last)
        if s["life_cycle_state"] != last_life:
            log("run %s: %s %s" % (run_id, s["life_cycle_state"], s["result_state"]))
            last_life = s["life_cycle_state"]
        if s["life_cycle_state"] in TERMINAL_STATES:
            return last, False
        if monotonic() >= deadline:
            return last, True
        sleep(interval)


def run_wrapper(client, cfg, sleep=time.sleep, monotonic=time.monotonic, log=print) -> Dict[str, object]:
    """Create/update the job, start a run and wait for it. Returns a result dict."""
    if not client.notebook_exists(cfg["worker_notebook_path"]):
        raise ConfigError(
            "worker notebook not found or not accessible: %s" % cfg["worker_notebook_path"]
        )
    cfg = resolve_compute(client, cfg, log)
    existing_before = set(client.find_jobs_by_name(cfg["job_name"]))
    job_id = ensure_job(client, cfg)
    job_action = "updated" if job_id in existing_before else "created"
    log("job_id = %s (%s, compute_type=%s)" % (job_id, job_action, cfg["compute_type"]))
    try:
        run_id = client.run_now(job_id)
    except Exception as exc:  # noqa: BLE001
        raise JobApiError("run-now failed for job %s: %s" % (job_id, exc), job_id=job_id)
    log("run_id = %s" % run_id)

    result = {
        "job_id": job_id,
        "run_id": run_id,
        "compute_type": cfg["compute_type"],
        "job_action": job_action,
        "job_name": cfg["job_name"],
        "report_path": cfg["report_path"],
        "timed_out": False,
        "state": "SUBMITTED",
        "result_state": "",
        "run_page_url": "",
        "worker_summary": None,
    }
    run, timed_out = poll_run(client, run_id, cfg["max_wait_minutes"], sleep, monotonic, log=log)
    s = summarize_state(run)
    result.update(
        state=s["life_cycle_state"],
        result_state=s["result_state"],
        timed_out=timed_out,
        run_page_url=run.get("run_page_url", ""),
    )
    if timed_out:
        result["state"] = "%s (polling timed out after %d min; run NOT cancelled, final state unknown)" % (
            s["life_cycle_state"],
            cfg["max_wait_minutes"],
        )
        return result
    for t in run.get("tasks", []) or []:
        if t.get("task_key") == TASK_KEY:
            try:
                out = client.get_run_output(t["run_id"])
                result["worker_summary"] = out.get("notebook_output", {}).get("result")
                result["worker_error"] = out.get("error")
            except Exception as exc:  # noqa: BLE001 - output is informational
                log("Could not fetch worker output: %s" % exc)
    return result


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run - validates the widget values, creates/updates the job, runs it and waits

# COMMAND ----------


def main():  # pragma: no cover - runs only inside Databricks
    from databricks.sdk import WorkspaceClient

    g = dbutils.widgets.get
    wrapper_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

    cfg = validate_config(
        g("csv_path"), g("report_path"), sibling_worker_path(wrapper_path),
        g("compute_type"), g("all_purpose_cluster_id"),
        remote_workspace_url=g("remote_workspace_url"), remote_client_id=g("remote_client_id"),
        remote_secret_scope=g("remote_secret_scope"), remote_secret_key=g("remote_secret_key"),
        remote_warehouse_id=g("remote_warehouse_id"),
    )
    print("Validated configuration: %s" % cfg)
    client = JobsClient(WorkspaceClient())
    try:
        res = run_wrapper(client, cfg)
    except JobApiError as exc:
        print("JOB API ERROR: %s (job_id=%s run_id=%s)" % (exc, exc.job_id, exc.run_id))
        raise

    print("=" * 70)
    for k in ("job_id", "job_action", "run_id", "compute_type", "state", "result_state", "run_page_url", "report_path"):
        print("%-14s %s" % (k, res.get(k)))
    if res.get("worker_summary"):
        print("worker summary: %s" % res["worker_summary"])
    if res.get("worker_error"):
        print("worker error:   %s" % res["worker_error"])
    if not res["timed_out"] and res["result_state"] != "SUCCESS":
        raise RuntimeError(
            "Worker run %s did not succeed: state=%s result=%s"
            % (res["run_id"], res["state"], res["result_state"])
        )
    dbutils.notebook.exit(json.dumps(res, default=str))


# COMMAND ----------

if __name__ == "__main__":
    main()
