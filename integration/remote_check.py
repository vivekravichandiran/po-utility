#!/usr/bin/env python3
"""Live check of the remote-execution path against a real workspace's SQL warehouse.

Runs the REAL RemoteSqlClient and execute_records from notebooks/po_worker.py (preflight, statement
submit, polling, failure handling) against <profile>'s warehouse. The only difference from
production is the bearer token: it comes from the CLI profile (PAT) instead of the OAuth
client-credentials exchange, which needs a service principal secret (see README).

Usage: integration/remote_check.py <profile> <warehouse_id>
Touches only the po_ctl_* test objects; run `integration/po_state.py <profile> <wh> reset` afterwards.
"""
import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "notebooks"))

from databricks.sdk import WorkspaceClient  # noqa: E402

import po_worker as w  # noqa: E402


def main():
    profile, warehouse = sys.argv[1:3]
    ws = WorkspaceClient(profile=profile)
    host = ws.config.host.rstrip("/")
    token = ws.config.authenticate()["Authorization"].split(" ", 1)[1]

    client = w.RemoteSqlClient(host, "pat-profile", lambda: "unused", warehouse, token_provider=lambda: token)
    print("remote:", client.url, "warehouse", client.warehouse_id)

    t0 = time.time()
    client.preflight()  # SELECT 1; a stopped warehouse is started, so this can take a while
    print("preflight OK in %.1fs" % (time.time() - t0))

    rows = [
        (2, ["SCHEMA", "po_ctl_t1", "sales_stage", "", "ENABLE", "remote"]),
        (3, ["TABLE", "po_ctl_t1", "sales_curated", "orders", "DISABLE", "remote"]),
        (4, ["CATALOG", "po_ctl_t1", "", "", "ENABLE", "local"]),          # local: goes to the fake spark
        (5, ["TABLE", "po_ctl_t1", "sales_curated", "no_such_table", "ENABLE", "remote"]),  # real failure
        (6, ["TABLE", "po_ctl_t3", "locked", "secret_tbl", "ENABLE", "remote"]),  # real permission failure
        (7, ["SCHEMA", "po_ctl_t1", "sales_curated", "", "ENABLE", "remote"]),  # runs after the failures
    ]
    records, executable = w.plan_records(rows)
    local_calls = []
    w.execute_records(executable, lambda sql: local_calls.append(sql), remote_runner=client.execute)

    print("\nrow  workspace  status             statement_id                          error_class")
    for r in records:
        print("%-4d %-10s %-18s %-37s %s" % (r.source_row_number, r.workspace, r.status,
                                             r.remote_statement_id or "-", r.error_class or "-"))
        if r.error_message:
            print("       ", r.error_message[:160])
    print("local statements (fake spark):", local_calls)

    expect = {2: "SUCCESS", 3: "SUCCESS", 4: "SUCCESS", 5: "EXECUTION_FAILED", 6: "EXECUTION_FAILED", 7: "SUCCESS"}
    got = {r.source_row_number: r.status for r in records}
    ok = got == expect and not local_calls[1:] and len(local_calls) == 1
    print("\nRESULT:", "as expected" if ok else "UNEXPECTED %s" % got)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
