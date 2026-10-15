#!/usr/bin/env python3
"""Submit po_worker directly (bypassing the wrapper) with arbitrary parameters.
Usage: it_worker_direct.py <profile> key=value ...   -> prints task state + error."""
import json, sys, time, warnings
warnings.filterwarnings("ignore")
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(profile=sys.argv[1])
params = dict(kv.split("=", 1) for kv in sys.argv[2:])
body = {"run_name": "po_worker direct", "tasks": [{
    "task_key": "w", "environment_key": "e", "max_retries": 0, "disable_auto_optimization": True,
    "notebook_task": {"notebook_path": "/Shared/po_control/po_worker", "source": "WORKSPACE", "base_parameters": params}}],
    "environments": [{"environment_key": "e", "spec": {"environment_version": "3", "dependencies": ["openpyxl"]}}]}
rid = w.api_client.do("POST", "/api/2.1/jobs/runs/submit", body=body)["run_id"]
while True:
    r = w.api_client.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": rid})
    if r["state"]["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"): break
    time.sleep(6)
t = r["tasks"][0]
o = w.api_client.do("GET", "/api/2.1/jobs/runs/get-output", query={"run_id": t["run_id"]})
print("state:", r["state"]["life_cycle_state"], r["state"].get("result_state"), "| attempts:", len(r["tasks"]))
print("error:", o.get("error"))
print("result:", (o.get("notebook_output") or {}).get("result", "")[:300])
