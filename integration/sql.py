#!/usr/bin/env python3
"""Run SQL statements on a Databricks SQL warehouse and print results.

Usage: integration/sql.py <profile> <warehouse_id> "<sql>" ["<sql>" ...]
Used for preparing / inspecting integration-test objects. Stops on first error.
"""
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementState


def run(w, wh, sql):
    r = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=wh, wait_timeout="50s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
    )
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError("%s\n  -> %s: %s" % (sql, r.status.state, r.status.error.message if r.status.error else ""))
    cols = [c.name for c in (r.manifest.schema.columns if r.manifest and r.manifest.schema else [])]
    rows = r.result.data_array if r.result and r.result.data_array else []
    return cols, rows


if __name__ == "__main__":
    profile, wh, *stmts = sys.argv[1:]
    w = WorkspaceClient(profile=profile)
    for s in stmts:
        print(">>", s)
        cols, rows = run(w, wh, s)
        if cols:
            print("   ", " | ".join(cols))
        for row in rows:
            print("   ", " | ".join("" if v is None else str(v) for v in row))
