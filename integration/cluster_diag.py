#!/usr/bin/env python3
"""Run a code snippet on an all-purpose cluster via the Command Execution API.
Usage: cluster_diag.py <profile> <cluster_id> <file.py>"""
import sys, warnings
warnings.filterwarnings("ignore")
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language
w = WorkspaceClient(profile=sys.argv[1])
cid = sys.argv[2]
ctx = w.command_execution.create_and_wait(cluster_id=cid, language=Language.PYTHON)
res = w.command_execution.execute_and_wait(cluster_id=cid, context_id=ctx.id, language=Language.PYTHON,
                                          command=open(sys.argv[3]).read())
print(res.results.data if res.results.result_type.value != "error" else res.results.cause)
w.command_execution.destroy(cluster_id=cid, context_id=ctx.id)
