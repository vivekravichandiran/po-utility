# Databricks notebook source
# MAGIC %md
# MAGIC # PO Control - Worker
# MAGIC Reads a CSV of Predictive Optimization (PO) operations from a Unity Catalog Volume,
# MAGIC validates it, executes one `ALTER ... {ENABLE|DISABLE} PREDICTIVE OPTIMIZATION` per
# MAGIC unique valid target (each in its own exception boundary) and writes an XLSX report
# MAGIC (`Summary` + `Details` sheets) to a Volume.
# MAGIC
# MAGIC **Task parameters (widgets):** `csv_path`, `report_path`, `volume_root`,
# MAGIC `overwrite_report`, `compute_type` (informational), `job_id`, `run_id`.
# MAGIC
# MAGIC Failure policy: row-level problems are recorded in the report and do **not** fail the
# MAGIC task. Bootstrap problems (bad paths / CSV structure) and report-writing problems **do**
# MAGIC fail the task. DDL is never executed before paths and CSV structure are validated and
# MAGIC the report location is proven writable.

# COMMAND ----------

import csv
import json
import os
import posixpath
import re
import shutil
import tempfile
import traceback
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants and allowlists

# COMMAND ----------

REQUIRED_HEADER = ["object_type", "catalog", "schema", "table", "action"]

# object_type -> (identifier fields that must be present, fields that must be blank)
SCOPE_RULES = {
    "CATALOG": (("catalog",), ("schema", "table")),
    "SCHEMA": (("catalog", "schema"), ("table",)),
    "TABLE": (("catalog", "schema", "table"), ()),
}
ALLOWED_ACTIONS = ("ENABLE", "DISABLE")

# Allowlisted SQL templates. Only quoted identifiers and the allowlisted action are
# ever substituted; no CSV text is ever used as a SQL fragment.
SQL_TEMPLATES = {
    "CATALOG": "ALTER CATALOG {target} {action} PREDICTIVE OPTIMIZATION",
    "SCHEMA": "ALTER SCHEMA {target} {action} PREDICTIVE OPTIMIZATION",
    "TABLE": "ALTER TABLE {target} {action} PREDICTIVE OPTIMIZATION",
}

STATUS_SUCCESS = "SUCCESS"
STATUS_VALIDATION_FAILED = "VALIDATION_FAILED"
STATUS_EXECUTION_FAILED = "EXECUTION_FAILED"
STATUS_SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
STATUS_SKIPPED_CONFLICT = "SKIPPED_CONFLICT"
ALL_STATUSES = (
    STATUS_SUCCESS,
    STATUS_VALIDATION_FAILED,
    STATUS_EXECUTION_FAILED,
    STATUS_SKIPPED_DUPLICATE,
    STATUS_SKIPPED_CONFLICT,
)

OUTCOME_ALL_SUCCEEDED = "ALL_SUCCEEDED"
OUTCOME_WITH_FAILURES = "COMPLETED_WITH_FAILURES"
OUTCOME_NO_ROWS = "NO_INPUT_ROWS"

DETAIL_COLUMNS = [
    "source_row_number",
    "object_type",
    "catalog",
    "schema",
    "table",
    "fully_qualified_target",
    "requested_action",
    "generated_sql",
    "status",
    "start_timestamp",
    "end_timestamp",
    "error_class",
    "error_message",
    "reference_row_numbers",
]

MAX_IDENTIFIER_LEN = 255
XLSX_MAX_CELL_CHARS = 32000  # Excel hard limit is 32767
TRUNCATION_MARKER = " ...[TRUNCATED - full text in task output]"
_ILLEGAL_XML_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


class InputError(Exception):
    """Bootstrap / file-level problem: no DDL may run, the task must fail."""


class ReportError(Exception):
    """Report generation or publishing problem: the task must fail."""


@dataclass
class ResultRecord:
    source_row_number: int
    object_type: str = ""
    catalog: str = ""
    schema: str = ""
    table: str = ""
    action: str = ""
    fully_qualified_target: str = ""
    generated_sql: str = ""
    status: str = ""
    start_timestamp: str = ""
    end_timestamp: str = ""
    error_class: str = ""
    error_message: str = ""
    references: List[int] = field(default_factory=list)
    target_key: Optional[Tuple] = None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Time helper

# COMMAND ----------


def utc_now_iso() -> str:
    """Timezone-aware ISO-8601 timestamp (UTC)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Path validation (Volume-root boundary)

# COMMAND ----------


def _clean_volume_path(path: str, label: str) -> str:
    """Normalize a /Volumes path, rejecting traversal and malformed values."""
    if path is None or not str(path).strip():
        raise InputError("%s must not be empty" % label)
    raw = str(path).strip()
    if "\x00" in raw or "\n" in raw or "\r" in raw:
        raise InputError("%s contains illegal characters" % label)
    if not raw.startswith("/Volumes/"):
        raise InputError("%s must be an absolute /Volumes/... path: %r" % (label, raw))
    if ".." in raw.split("/"):
        raise InputError("%s must not contain '..' path segments: %r" % (label, raw))
    norm = posixpath.normpath(raw)
    # normpath keeps a leading '//' untouched; we already required '/Volumes/'.
    return norm


def validate_volume_root(volume_root: str) -> str:
    root = _clean_volume_path(volume_root, "volume_root")
    parts = [p for p in root.split("/") if p]
    # /Volumes/<catalog>/<schema>/<volume>[/subdir...]
    if len(parts) < 4:
        raise InputError(
            "volume_root must be at least /Volumes/<catalog>/<schema>/<volume>: %r" % root
        )
    return root


def validate_path_in_root(path: str, volume_root: str, label: str) -> str:
    root = validate_volume_root(volume_root)
    norm = _clean_volume_path(path, label)
    if not norm.startswith(root + "/"):
        raise InputError("%s %r is outside volume_root %r" % (label, norm, root))
    return norm


def validate_paths(csv_path: str, report_path: str, volume_root: str) -> Tuple[str, str]:
    """Validate both paths; returns normalized (csv_path, report_path)."""
    csv_norm = validate_path_in_root(csv_path, volume_root, "csv_path")
    rep_norm = validate_path_in_root(report_path, volume_root, "report_path")
    if not rep_norm.lower().endswith(".xlsx"):
        raise InputError("report_path must end with .xlsx: %r" % rep_norm)
    if not csv_norm.lower().endswith(".csv"):
        raise InputError("csv_path must end with .csv: %r" % csv_norm)
    if csv_norm == rep_norm or csv_norm.lower() == rep_norm.lower():
        raise InputError("report_path must differ from csv_path")
    if os.path.exists(csv_norm) and os.path.exists(rep_norm):
        try:
            if os.path.samefile(csv_norm, rep_norm):
                raise InputError("report_path resolves to the same file as csv_path")
        except OSError:
            pass
    return csv_norm, rep_norm


# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL generation

# COMMAND ----------


def quote_identifier(name: str) -> str:
    """Backtick-quote a single identifier; embedded backticks are doubled."""
    return "`" + name.replace("`", "``") + "`"


def fully_qualified_target(object_type: str, catalog: str, schema: str, table: str) -> str:
    parts = {"CATALOG": [catalog], "SCHEMA": [catalog, schema], "TABLE": [catalog, schema, table]}[
        object_type
    ]
    return ".".join(quote_identifier(p) for p in parts)


def build_sql(object_type: str, action: str, catalog: str, schema: str, table: str) -> str:
    """Build the DDL only from allowlisted object_type/action and quoted identifiers."""
    if object_type not in SQL_TEMPLATES:
        raise ValueError("object_type not allowed: %r" % object_type)
    if action not in ALLOWED_ACTIONS:
        raise ValueError("action not allowed: %r" % action)
    target = fully_qualified_target(object_type, catalog, schema, table)
    return SQL_TEMPLATES[object_type].format(target=target, action=action)


# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV reading (file-level validation)

# COMMAND ----------


def read_csv_records(csv_path: str) -> List[Tuple[int, List[str]]]:
    """Read the CSV and return [(source_row_number, fields)] for non-blank data rows.

    The header is row 1. Raises InputError for any file-level problem (missing/unreadable
    file, empty file, header not exactly the required column set).
    """
    if not os.path.isfile(csv_path):
        raise InputError("CSV not found or not a file: %s" % csv_path)
    records = []  # type: List[Tuple[int, List[str]]]
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.reader(fh)
            header = None
            for idx, rec in enumerate(reader, start=1):
                if idx == 1:
                    header = [h.strip() for h in rec]
                    continue
                if not rec or all(c.strip() == "" for c in rec):
                    continue  # blank line: ignored, not counted
                records.append((idx, rec))
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise InputError("Cannot read CSV %s: %s: %s" % (csv_path, type(exc).__name__, exc))
    if header is None:
        raise InputError("CSV is empty (no header row): %s" % csv_path)
    _check_header(header)
    # Re-map well-formed rows to canonical column order (header may be in any order).
    # Rows whose field count differs from the header keep their raw shape; validate_row
    # flags them as MalformedRow because len != len(REQUIRED_HEADER).
    pos = {name: header.index(name) for name in REQUIRED_HEADER}
    mapped = []
    for idx, rec in records:
        if len(rec) == len(header):
            mapped.append((idx, [rec[pos[name]] for name in REQUIRED_HEADER]))
        else:
            mapped.append((idx, rec))
    return mapped


def _check_header(header: List[str]) -> None:
    if len(set(header)) != len(header):
        raise InputError("CSV header has duplicate columns: %s" % header)
    missing = [c for c in REQUIRED_HEADER if c not in header]
    extra = [c for c in header if c not in REQUIRED_HEADER]
    if missing or extra:
        raise InputError(
            "CSV header must be exactly %s; missing=%s extra=%s (found %s)"
            % (",".join(REQUIRED_HEADER), missing, extra, header)
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Row validation, duplicate and conflict policy

# COMMAND ----------


def _identifier_problem(label: str, value: str) -> Optional[str]:
    if len(value) > MAX_IDENTIFIER_LEN:
        return "%s is longer than %d characters" % (label, MAX_IDENTIFIER_LEN)
    if "." in value:
        return "%s must not contain '.'; put each name part in its own column" % label
    if "/" in value:
        return "%s must not contain '/'" % label
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in value):
        return "%s must not contain whitespace or control characters" % label
    return None


def validate_row(source_row_number: int, fields: List[str]) -> ResultRecord:
    """Validate a single row. Returns a ResultRecord (status VALIDATION_FAILED or '')."""
    rec = ResultRecord(source_row_number=source_row_number)
    if len(fields) != len(REQUIRED_HEADER):
        rec.status = STATUS_VALIDATION_FAILED
        rec.error_class = "MalformedRow"
        rec.error_message = "Row has %d fields; expected %d" % (len(fields), len(REQUIRED_HEADER))
        return rec

    vals = dict(zip(REQUIRED_HEADER, [f.strip() for f in fields]))
    rec.object_type = vals["object_type"].upper()
    rec.catalog, rec.schema, rec.table = vals["catalog"], vals["schema"], vals["table"]
    rec.action = vals["action"].upper()

    problems = []
    rules = SCOPE_RULES.get(rec.object_type)
    if rules is None:
        problems.append(
            "object_type %r not allowed (expected one of %s)"
            % (vals["object_type"], ", ".join(SCOPE_RULES))
        )
    if rec.action not in ALLOWED_ACTIONS:
        problems.append(
            "action %r not allowed (expected one of %s)"
            % (vals["action"], ", ".join(ALLOWED_ACTIONS))
        )
    if rules is not None:
        required, must_be_blank = rules
        for col in required:
            if not vals[col]:
                problems.append("%s is required for object_type %s" % (col, rec.object_type))
            else:
                p = _identifier_problem(col, vals[col])
                if p:
                    problems.append(p)
        for col in must_be_blank:
            if vals[col]:
                problems.append("%s must be blank for object_type %s" % (col, rec.object_type))

    if problems:
        rec.status = STATUS_VALIDATION_FAILED
        rec.error_class = "ValidationError"
        rec.error_message = "; ".join(problems)
        return rec

    # Valid: pre-compute target/key/sql. SQL is only *generated* here, never executed.
    rec.fully_qualified_target = fully_qualified_target(
        rec.object_type, rec.catalog, rec.schema, rec.table
    )
    rec.generated_sql = build_sql(rec.object_type, rec.action, rec.catalog, rec.schema, rec.table)
    # Unity Catalog identifiers are case-insensitive: normalize for duplicate detection.
    rec.target_key = (
        rec.object_type,
        rec.catalog.casefold(),
        rec.schema.casefold(),
        rec.table.casefold(),
    )
    return rec


def plan_records(raw_rows: List[Tuple[int, List[str]]]) -> Tuple[List[ResultRecord], List[ResultRecord]]:
    """Validate all rows and apply duplicate/conflict policy.

    Returns (all_records_in_row_order, executable_records_in_csv_order).
    Executable = the first row of each valid, non-conflicting target.
    """
    records = [validate_row(n, f) for n, f in raw_rows]

    groups = OrderedDict()  # type: OrderedDict
    for rec in records:
        if rec.status == STATUS_VALIDATION_FAILED:
            continue
        groups.setdefault(rec.target_key, []).append(rec)

    executable = []
    for key, members in groups.items():
        actions = {m.action for m in members}
        if len(actions) > 1:
            rows = [m.source_row_number for m in members]
            for m in members:
                m.status = STATUS_SKIPPED_CONFLICT
                m.error_class = "ConflictingActions"
                m.error_message = "Conflicting actions %s requested for the same target" % sorted(
                    actions
                )
                m.references = [r for r in rows if r != m.source_row_number]
                m.generated_sql = ""  # nothing will be executed for this target
            continue
        first = members[0]
        executable.append(first)
        for dup in members[1:]:
            dup.status = STATUS_SKIPPED_DUPLICATE
            dup.error_class = "DuplicateTarget"
            dup.error_message = "Identical request already handled at row %d" % first.source_row_number
            dup.references = [first.source_row_number]
    return records, executable


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution (one exception boundary per statement, no retries)

# COMMAND ----------


def _error_class_of(exc: BaseException) -> str:
    for attr in ("getCondition", "getErrorClass"):
        fn = getattr(exc, attr, None)
        if callable(fn):
            try:
                val = fn()
            except Exception:  # noqa: BLE001 - diagnostics only
                val = None
            if val:
                return "%s:%s" % (type(exc).__name__, val)
    return type(exc).__name__


def execute_records(
    executable: List[ResultRecord],
    sql_runner: Callable[[str], object],
    log: Callable[[str], None] = print,
    clock: Callable[[], str] = utc_now_iso,
) -> None:
    """Execute each record's SQL once. A failure never stops later records."""
    for rec in executable:
        rec.start_timestamp = clock()
        try:
            sql_runner(rec.generated_sql)  # exactly one attempt: no automatic retry
            rec.status = STATUS_SUCCESS
        except Exception as exc:  # noqa: BLE001 - row-level isolation is intentional
            rec.status = STATUS_EXECUTION_FAILED
            rec.error_class = _error_class_of(exc)
            rec.error_message = str(exc) or type(exc).__name__
            log("Row %d FAILED: %s -> %s" % (rec.source_row_number, rec.generated_sql, rec.error_class))
        else:
            log("Row %d SUCCESS: %s" % (rec.source_row_number, rec.generated_sql))
        finally:
            rec.end_timestamp = clock()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------


def count_statuses(records: List[ResultRecord]) -> Dict[str, int]:
    counts = {s: 0 for s in ALL_STATUSES}
    for r in records:
        counts[r.status] = counts.get(r.status, 0) + 1
    return counts


def build_summary(
    records: List[ResultRecord],
    csv_path: str,
    report_path: str,
    start_ts: str,
    end_ts: str,
    run_id: str = "",
    job_id: str = "",
    report_status: str = "WRITTEN",
    compute_type: str = "",
) -> "OrderedDict[str, object]":
    c = count_statuses(records)
    attempted = c[STATUS_SUCCESS] + c[STATUS_EXECUTION_FAILED]
    problems = (
        c[STATUS_VALIDATION_FAILED]
        + c[STATUS_EXECUTION_FAILED]
        + c[STATUS_SKIPPED_CONFLICT]
    )
    if not records:
        outcome = OUTCOME_NO_ROWS
    elif problems:
        outcome = OUTCOME_WITH_FAILURES
    else:
        outcome = OUTCOME_ALL_SUCCEEDED
    s = OrderedDict()
    s["run_id"] = run_id
    s["job_id"] = job_id
    s["compute_type"] = compute_type
    s["input_csv_path"] = csv_path
    s["report_path"] = report_path
    s["start_timestamp"] = start_ts
    s["end_timestamp"] = end_ts
    s["total_input_rows"] = len(records)
    s["valid_unique_targets_attempted"] = attempted
    s["successes"] = c[STATUS_SUCCESS]
    s["validation_failures"] = c[STATUS_VALIDATION_FAILED]
    s["execution_failures"] = c[STATUS_EXECUTION_FAILED]
    s["duplicates_skipped"] = c[STATUS_SKIPPED_DUPLICATE]
    s["conflicts_skipped"] = c[STATUS_SKIPPED_CONFLICT]
    s["report_status"] = report_status
    s["overall_outcome"] = outcome
    s["note"] = (
        "SUCCESS means the ALTER statement returned without error; it does not prove the "
        "effective PO behavior of every child object. Parent settings do not override "
        "explicitly configured children."
    )
    return s


# COMMAND ----------

# MAGIC %md
# MAGIC ## XLSX report

# COMMAND ----------


def sanitize_cell(value, log: Callable[[str], None] = print, where: str = ""):
    """Make a value safe for XLSX: neutralize formulas, drop illegal chars, cap length."""
    if not isinstance(value, str):
        return value
    text = _ILLEGAL_XML_CHARS.sub("\ufffd", value)
    if text.startswith(_FORMULA_PREFIXES):
        text = "'" + text  # prevents interpretation as a formula
    if len(text) > XLSX_MAX_CELL_CHARS:
        log("TRUNCATED cell (%s), full text follows:\n%s" % (where, value))
        text = text[: XLSX_MAX_CELL_CHARS - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER
    return text


def record_to_row(rec: ResultRecord) -> List[object]:
    return [
        rec.source_row_number,
        rec.object_type,
        rec.catalog,
        rec.schema,
        rec.table,
        rec.fully_qualified_target,
        rec.action,
        rec.generated_sql,
        rec.status,
        rec.start_timestamp,
        rec.end_timestamp,
        rec.error_class,
        rec.error_message,
        ", ".join(str(r) for r in rec.references),
    ]


def build_workbook(records: List[ResultRecord], summary, log: Callable[[str], None] = print):
    import openpyxl  # imported lazily so tests of pure logic do not need it

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Summary"
    ws.append(["metric", "value"])
    for k, v in summary.items():
        ws.append([k, sanitize_cell(v, log, "Summary/%s" % k)])
    ws.column_dimensions["A"].width = 34
    ws.column_dimensions["B"].width = 100

    wd = wb.create_sheet("Details")
    wd.append(DETAIL_COLUMNS)
    for rec in sorted(records, key=lambda r: r.source_row_number):
        row = record_to_row(rec)
        wd.append(
            [
                sanitize_cell(v, log, "Details/row %d/%s" % (rec.source_row_number, DETAIL_COLUMNS[i]))
                for i, v in enumerate(row)
            ]
        )
    wd.freeze_panes = "A2"
    for col, width in zip("ABCDEFGHIJKLMN", (10, 12, 16, 16, 16, 44, 12, 70, 20, 28, 28, 28, 80, 16)):
        wd.column_dimensions[col].width = width
    return wb


def verify_report(path: str, expected_detail_rows: int) -> None:
    """Re-open the published workbook and check structure and row counts."""
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        if wb.sheetnames != ["Summary", "Details"]:
            raise ReportError("Unexpected sheets in report: %s" % wb.sheetnames)
        n = sum(1 for _ in wb["Details"].iter_rows(min_row=2, values_only=True))
        if n != expected_detail_rows:
            raise ReportError("Details rows %d != expected %d" % (n, expected_detail_rows))
    finally:
        wb.close()


def preflight_report_location(report_path: str, overwrite: bool) -> None:
    """Prove the report can be written BEFORE any DDL runs."""
    if os.path.exists(report_path) and not overwrite:
        raise InputError(
            "report_path already exists and overwrite_report is false: %s" % report_path
        )
    if os.path.isdir(report_path):
        raise InputError("report_path is a directory: %s" % report_path)
    rdir = posixpath.dirname(report_path)
    try:
        os.makedirs(rdir, exist_ok=True)
        probe = posixpath.join(rdir, ".po_probe_%s.tmp" % uuid.uuid4().hex)
        with open(probe, "wb") as fh:
            fh.write(b"probe")
        os.remove(probe)
    except OSError as exc:
        raise InputError(
            "Report directory %s is not writable: %s. Check the run identity's WRITE VOLUME "
            "privilege. If this runs on classic compute (job cluster / all-purpose) and ALL "
            "Unity Catalog access fails, the cluster's egress IP may be blocked by the workspace "
            "IP access list (look for '403 ... blocked by Databricks IP ACL' in the cluster logs)."
            % (rdir, exc)
        )
    try:
        import openpyxl  # noqa: F401
    except ImportError as exc:
        raise InputError("openpyxl is required to write the XLSX report: %s" % exc)


def write_report(records, summary, report_path: str, overwrite: bool, log=print) -> None:
    """Build the workbook locally, stage it next to the target, publish, then verify."""
    try:
        wb = build_workbook(records, summary, log)
        with tempfile.TemporaryDirectory() as tmpdir:
            local = os.path.join(tmpdir, "report.xlsx")
            wb.save(local)  # local disk supports random access; Volumes may not
            rdir = posixpath.dirname(report_path)
            staged = posixpath.join(rdir, ".%s.%s.tmp" % (posixpath.basename(report_path), uuid.uuid4().hex))
            shutil.copyfile(local, staged)
            try:
                if os.path.exists(report_path) and not overwrite:
                    raise ReportError("report_path appeared during the run: %s" % report_path)
                try:
                    os.replace(staged, report_path)
                except OSError as exc:
                    log("os.replace unsupported here (%s); falling back to copy" % exc)
                    shutil.copyfile(staged, report_path)
            finally:
                if os.path.exists(staged):
                    try:
                        os.remove(staged)
                    except OSError as exc:
                        log("Could not remove staging file %s: %s" % (staged, exc))
        verify_report(report_path, len(records))
    except ReportError:
        raise
    except Exception as exc:  # noqa: BLE001 - re-raised as ReportError after logging
        log("REPORT WRITE FAILED:\n" + traceback.format_exc())
        raise ReportError("Failed to write report %s: %s: %s" % (report_path, type(exc).__name__, exc))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration

# COMMAND ----------


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in ("true", "1", "yes", "y")


def run_worker(
    csv_path: str,
    report_path: str,
    volume_root: str,
    sql_runner: Callable[[str], object],
    overwrite: bool = False,
    job_id: str = "",
    run_id: str = "",
    log: Callable[[str], None] = print,
    compute_type: str = "",
) -> "OrderedDict[str, object]":
    """Full worker flow. Raises InputError/ReportError for task-failing conditions."""
    start_ts = utc_now_iso()
    # ---- bootstrap: nothing below may execute DDL until it all passes ----
    csv_norm, report_norm = validate_paths(csv_path, report_path, volume_root)
    preflight_report_location(report_norm, overwrite)
    raw_rows = read_csv_records(csv_norm)
    records, executable = plan_records(raw_rows)
    log(
        "Parsed %d rows; %d valid unique targets to execute" % (len(records), len(executable))
    )
    # ---- execution ----
    execute_records(executable, sql_runner, log)
    # ---- report ----
    summary = build_summary(
        records, csv_norm, report_norm, start_ts, utc_now_iso(), run_id=run_id, job_id=job_id,
        compute_type=compute_type,
    )
    write_report(records, summary, report_norm, overwrite, log)
    return summary


def main():  # pragma: no cover - runs only inside Databricks
    dbutils.widgets.text("csv_path", "")
    dbutils.widgets.text("report_path", "")
    dbutils.widgets.text("volume_root", "")
    dbutils.widgets.text("overwrite_report", "false")
    dbutils.widgets.text("job_id", "")
    dbutils.widgets.text("run_id", "")
    dbutils.widgets.text("compute_type", "")
    get = dbutils.widgets.get

    try:
        summary = run_worker(
            csv_path=get("csv_path"),
            report_path=get("report_path"),
            volume_root=get("volume_root"),
            sql_runner=lambda sql: spark.sql(sql),  # noqa: F821 - provided by the runtime
            overwrite=parse_bool(get("overwrite_report")),
            job_id=get("job_id"),
            run_id=get("run_id"),
            compute_type=get("compute_type"),
        )
    except (InputError, ReportError) as exc:
        print("PO WORKER FAILED (%s): %s" % (type(exc).__name__, exc))
        raise  # surfaces as a failed task

    print("=" * 70)
    for k, v in summary.items():
        if k != "note":
            print("%-34s %s" % (k, v))
    print("Report written to: %s" % summary["report_path"])
    dbutils.notebook.exit(json.dumps(summary))


# COMMAND ----------

if __name__ == "__main__":
    main()
