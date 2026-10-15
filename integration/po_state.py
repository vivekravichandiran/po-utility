#!/usr/bin/env python3
"""Print the explicit Predictive Optimization setting of the po_ctl_* test objects.

Independent of the utility under test: reads DESCRIBE ... EXTENDED via the SQL API.
Usage: integration/po_state.py <profile> <warehouse_id> [reset]
  reset -> set every test object back to INHERIT first.
"""
import os
import sys

from databricks.sdk import WorkspaceClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sql import run  # noqa: E402

CATALOGS = ["po_ctl_t1", "po_ctl_t2"]
SCHEMAS = ["po_ctl_t1.sales_stage", "po_ctl_t1.sales_curated", "po_ctl_t2.finance"]
TABLES = [
    "po_ctl_t1.sales_curated.orders",
    "po_ctl_t1.sales_curated.customers",
    "po_ctl_t1.sales_stage.raw_events",
    "po_ctl_t2.finance.ledger",
]


def po_of(w, wh, kind, name):
    _, rows = run(w, wh, "DESCRIBE %s EXTENDED %s" % (kind, name))
    for r in rows:
        if r[0] and r[0].strip() == "Predictive Optimization":
            return r[1]
    return "?"


if __name__ == "__main__":
    profile, wh = sys.argv[1:3]
    w = WorkspaceClient(profile=profile)
    if len(sys.argv) > 3 and sys.argv[3] == "reset":
        for kind, names in (("CATALOG", CATALOGS), ("SCHEMA", SCHEMAS), ("TABLE", TABLES)):
            for n in names:
                run(w, wh, "ALTER %s %s INHERIT PREDICTIVE OPTIMIZATION" % (kind, n))
        print("reset all to INHERIT")
    for kind, names in (("CATALOG", CATALOGS), ("SCHEMA", SCHEMAS), ("TABLE", TABLES)):
        for n in names:
            print("%-8s %-40s %s" % (kind, n, po_of(w, wh, kind, n)))
