"""Unit tests for notebooks/po_wrapper.py (Jobs API is mocked)."""
import inspect
import re
from datetime import datetime, timezone

import pytest

import po_wrapper as p

ROOT = "/Volumes/admin/ops/po_control"
NOW = datetime(2026, 10, 15, 6, 54, 1, tzinfo=timezone.utc)
REPORT_FILE = ROOT + "/po_report_20261015T065401Z.xlsx"  # what NOW generates for report_path=ROOT
WORKER = "/Shared/po_control/po_worker"


def cfg(**over):
    args = dict(
        csv_path=ROOT + "/po_operations.csv",
        report_path=ROOT,
        worker_notebook_path=WORKER,
        now=NOW,
        exists=lambda _p: False,
        isfile=lambda _p: True,
        isdir=lambda _p: True,
    )
    args.update(over)
    return p.validate_config(**args)


# ------------------------------------------------------------ the widget surface
REMOTE_WIDGETS = {"remote_workspace_url", "remote_client_id", "remote_secret_scope", "remote_secret_key",
                  "remote_warehouse_id"}
EXPECTED_WIDGETS = {"csv_path", "report_path", "compute_type", "all_purpose_cluster_id"} | REMOTE_WIDGETS
REMOVED_WIDGETS = (
    "job_cluster_spark_version", "job_cluster_node_type_id", "job_cluster_num_workers", "job_mode",
    "job_name_prefix", "max_wait_minutes", "overwrite_report", "volume_root", "wait_for_completion",
    "worker_notebook_path",
)


class FakeWidgets:
    def __init__(self):
        self.created = {}

    def text(self, name, default, label=None):
        self.created[name] = ("text", default, None)

    def dropdown(self, name, default, choices, label=None):
        assert default in choices, (name, default, choices)
        self.created[name] = ("dropdown", default, list(choices))

    def get(self, name):
        return self.created[name][1]


def make_widgets(monkeypatch):
    class FakeDbutils:
        widgets = FakeWidgets()

    monkeypatch.setattr(p, "dbutils", FakeDbutils, raising=False)
    p.create_widgets()
    return FakeDbutils.widgets.created


def test_only_the_needed_widgets_are_created(monkeypatch):
    created = make_widgets(monkeypatch)
    assert set(created) == EXPECTED_WIDGETS
    for gone in REMOVED_WIDGETS:
        assert gone not in created
    # the OAuth secret itself is never a widget (widget values become visible job parameters)
    assert not any(name.endswith(("client_secret", "secret_value", "password", "token")) for name in created)
    assert all(created[name][1] == "" for name in REMOTE_WIDGETS)  # blank default = local only


def test_create_widgets_creates_every_widget_main_reads(monkeypatch):
    """Regression: widgets were created inside main(), so they never showed up beforehand."""
    created = make_widgets(monkeypatch)
    main_src = inspect.getsource(p.main)
    read_by_main = set(re.findall(r'g\("(\w+)"\)', main_src))
    assert read_by_main == EXPECTED_WIDGETS
    # main() itself must not create widgets (it could not be edited before running)
    assert "dbutils.widgets.text" not in main_src and "dbutils.widgets.dropdown" not in main_src
    assert created["compute_type"][2] == list(p.COMPUTE_TYPES) and created["compute_type"][1] == "serverless"


def test_widget_defaults_pass_validate_config(monkeypatch):
    """The widget defaults must be acceptable inputs (only the paths need real files)."""
    d = {k: v[1] for k, v in make_widgets(monkeypatch).items()}
    c = p.validate_config(
        d["csv_path"], d["report_path"], WORKER, d["compute_type"], d["all_purpose_cluster_id"],
        exists=lambda _p: False, isfile=lambda _p: True, isdir=lambda _p: True,
    )
    assert c["compute_type"] == "serverless"


def test_main_derives_worker_path_from_its_own_notebook_path():
    src = inspect.getsource(p.main)
    assert "notebookPath()" in src and "sibling_worker_path(" in src


# ------------------------------------------------------------ fixed (former widget) values
def test_fixed_values():
    assert p.JOB_NAME == "po-control"
    assert p.MAX_WAIT_MINUTES == 180
    assert p.JOB_CLUSTER_SPARK_VERSION == "17.3.x-scala2.13"  # DBR 17.3 LTS
    assert p.JOB_CLUSTER_NODE_TYPE_ID == "Standard_D4s_v3"  # Dsv3 series
    assert re.fullmatch(r"Standard_D\d+s_v3", p.JOB_CLUSTER_NODE_TYPE_ID)
    assert p.WORKER_NOTEBOOK_NAME == "po_worker"
    assert p.COMPUTE_TYPES == ("serverless", "job_cluster", "all_purpose")


def test_config_carries_the_fixed_values():
    c = cfg()
    assert c["job_name"] == "po-control" and c["max_wait_minutes"] == 180
    assert c["cluster"]["spark_version"] == "17.3.x-scala2.13"
    assert c["cluster"]["node_type_id"] == "Standard_D4s_v3" and c["cluster"]["num_workers"] == 0


def test_validate_config_no_longer_accepts_removed_parameters():
    params = set(inspect.signature(p.validate_config).parameters)
    # worker_notebook_path stays a parameter, but is now filled from the notebook's own path (no widget)
    for gone in set(REMOVED_WIDGETS) - {"worker_notebook_path"}:
        assert gone not in params
    for name in ("job_mode", "overwrite_report", "wait_for_completion", "max_wait_minutes"):
        with pytest.raises(TypeError):
            cfg(**{name: "x"})


@pytest.mark.parametrize(
    "wrapper,worker",
    [
        ("/Shared/po_control/po_wrapper", "/Shared/po_control/po_worker"),
        ("/Users/a@b.com/utils/po_wrapper", "/Users/a@b.com/utils/po_worker"),
        ("/Workspace/Users/a@b.com/x/renamed_wrapper", "/Workspace/Users/a@b.com/x/po_worker"),
        ("/po_wrapper", "/po_worker"),
    ],
)
def test_worker_path_is_the_sibling_of_the_wrapper(wrapper, worker):
    assert p.sibling_worker_path(wrapper) == worker


@pytest.mark.parametrize("bad", ["", "po_wrapper", "   "])
def test_worker_path_needs_an_absolute_wrapper_path(bad):
    with pytest.raises(p.ConfigError):
        p.sibling_worker_path(bad)


# ------------------------------------------------------------ volume derived from csv_path
def test_volume_root_is_derived_from_csv_path():
    c = cfg(csv_path="/Volumes/c1/s1/v1/in/ops.csv", report_path="/Volumes/c1/s1/v1/out")
    assert c["volume_root"] == "/Volumes/c1/s1/v1"
    bp = p.build_job_spec(c)["tasks"][0]["notebook_task"]["base_parameters"]
    assert bp["volume_root"] == "/Volumes/c1/s1/v1"  # still handed to the worker, which re-validates


def test_report_folder_must_be_in_the_same_volume_as_the_csv():
    for rep in ("/Volumes/c1/s1/other", "/Volumes/c2/s1/v1", "/Volumes/c1/s1/v1x", "/Volumes/c1/s1",
                "/Volumes/c1/s1/other/v1"):
        with pytest.raises(p.ConfigError, match="same volume"):
            cfg(csv_path="/Volumes/c1/s1/v1/a.csv", report_path=rep)


@pytest.mark.parametrize("csv", ["/Volumes/c1/s1/a.csv", "/Volumes/c1/a.csv", "/Volumes/a.csv"])
def test_csv_must_be_a_file_inside_a_volume(csv):
    with pytest.raises(p.ConfigError, match="/Volumes/<catalog>/<schema>/<volume>/<file>"):
        cfg(csv_path=csv, report_path="/Volumes/c1/s1/v1")


# ------------------------------------------------------------ report_path is a FOLDER
VOL = "/Volumes/ril_uc_demo/gold/ops"


def test_users_exact_case_volume_folder_as_report_path():
    """The reported case: csv_path=<vol>/po_mapping.csv and report_path=<the volume folder>."""
    c = cfg(csv_path=VOL + "/po_mapping.csv", report_path=VOL)
    assert c["volume_root"] == VOL and c["report_folder"] == VOL
    assert c["report_path"] == VOL + "/po_report_20261015T065401Z.xlsx"


@pytest.mark.parametrize("folder", [ROOT, ROOT + "/", ROOT + "//", ROOT + "/reports", ROOT + "/reports/",
                                    ROOT + "/a/b/c", "  " + ROOT + "  "])
def test_report_path_folder_gets_a_generated_timestamped_file_name(folder):
    c = cfg(report_path=folder)
    parent, name = c["report_path"].rsplit("/", 1)
    assert parent == c["report_folder"] == folder.strip().rstrip("/").replace("//", "/")
    assert name == "po_report_20261015T065401Z.xlsx"


def test_generated_name_changes_with_time_so_runs_never_collide():
    later = datetime(2026, 10, 15, 6, 54, 2, tzinfo=timezone.utc)
    assert cfg()["report_path"] != cfg(now=later)["report_path"]
    assert cfg(now=later)["report_path"].endswith("po_report_20261015T065402Z.xlsx")


def test_generate_report_path_uses_current_utc_time_by_default():
    path = p.generate_report_path(ROOT)
    assert re.fullmatch(re.escape(ROOT) + r"/po_report_\d{8}T\d{6}Z\.xlsx", path)


@pytest.mark.parametrize("filepath", [ROOT + "/r.xlsx", ROOT + "/po_report_1.XLSX", ROOT + "/old.xls",
                                      ROOT + "/data.csv"])
def test_report_path_given_as_a_file_is_rejected_with_a_clear_message(filepath):
    with pytest.raises(p.ConfigError) as e:
        cfg(report_path=filepath)
    assert "must be a folder" in str(e.value) and "generated for you" in str(e.value)


def test_report_folder_must_exist_and_is_not_created():
    seen = []
    with pytest.raises(p.ConfigError, match="folder does not exist") as e:
        cfg(report_path=ROOT + "/nope", isdir=lambda d: seen.append(d) or False)
    assert ROOT + "/nope" in str(e.value) and seen == [ROOT + "/nope"]


def test_csv_path_that_is_a_folder_gets_a_clear_message():
    with pytest.raises(p.ConfigError, match="full path of a .csv file"):
        cfg(csv_path=ROOT + "/inputs")


def test_worker_receives_the_generated_full_report_path():
    bp = p.build_job_spec(cfg(report_path=VOL, csv_path=VOL + "/po_mapping.csv"))["tasks"][0][
        "notebook_task"]["base_parameters"]
    assert bp["report_path"] == VOL + "/po_report_20261015T065401Z.xlsx"
    assert bp["overwrite_report"] == "false"


def test_removed_timestamp_token_is_no_longer_special():
    assert not hasattr(p, "resolve_report_path")
    with pytest.raises(p.ConfigError):  # looks like a file path now, not a template
        cfg(report_path=ROOT + "/po_report_{timestamp}.xlsx")


# ------------------------------------------------------------ config validation
@pytest.mark.parametrize(
    "over,frag",
    [
        (dict(csv_path="/tmp/x/a.csv"), "/Volumes"),
        (dict(csv_path=""), "empty"),
        (dict(csv_path=ROOT + "/../evil/a.csv"), ".."),
        (dict(csv_path=ROOT + "/a\nb.csv"), "illegal"),
        (dict(csv_path=ROOT + "/a.txt"), ".csv"),
        (dict(report_path=""), "empty"),
        (dict(report_path="/tmp/reports"), "/Volumes"),
        (dict(report_path=ROOT + "/../reports"), ".."),
        (dict(report_path=ROOT + "/r\nx"), "illegal"),
        (dict(worker_notebook_path="po_worker"), "absolute"),
        (dict(worker_notebook_path=""), "absolute"),
    ],
)
def test_invalid_config(over, frag):
    with pytest.raises(p.ConfigError) as e:
        cfg(**over)
    assert frag in str(e.value)


def test_missing_csv_rejected():
    with pytest.raises(p.ConfigError, match="does not exist"):
        cfg(isfile=lambda _p: False)


def test_existing_report_is_never_overwritten():
    """Only reachable when two runs start in the same second."""
    seen = []
    with pytest.raises(p.ConfigError, match="never overwritten"):
        cfg(exists=lambda x: seen.append(x) or True)
    assert seen == [REPORT_FILE]


# ------------------------------------------------------------ job spec
def test_job_spec_is_single_task_serverless():
    spec = p.build_job_spec(cfg())
    assert spec["name"] == "po-control"
    assert spec["max_concurrent_runs"] == 1
    assert len(spec["tasks"]) == 1
    task = spec["tasks"][0]
    assert task["task_key"] == "execute_po_operations"
    assert task["notebook_task"]["notebook_path"] == WORKER
    bp = task["notebook_task"]["base_parameters"]
    assert bp["csv_path"].endswith("po_operations.csv") and bp["report_path"] == REPORT_FILE
    assert bp["volume_root"] == ROOT and bp["job_id"] == "{{job.id}}"
    # serverless: no classic cluster configuration of any kind
    for forbidden in ("new_cluster", "existing_cluster_id", "job_cluster_key"):
        assert forbidden not in task
    assert "job_clusters" not in spec
    assert not any(k in str(spec).lower() for k in ("token", "password", "client_secret", "secret_value"))


def test_job_spec_never_allows_automatic_task_retry():
    """Serverless auto-retries failed tasks unless disabled; DDL must never be re-run."""
    task = p.build_job_spec(cfg())["tasks"][0]
    assert task["max_retries"] == 0
    assert task["disable_auto_optimization"] is True


def test_job_spec_installs_openpyxl_in_serverless_environment():
    spec = p.build_job_spec(cfg())
    env = spec["environments"][0]
    assert spec["tasks"][0]["environment_key"] == env["environment_key"]
    assert "openpyxl" in env["spec"]["dependencies"]


# ------------------------------------------------------------ fake client
class FakeClient:
    def __init__(self, jobs=None, active=None, states=None, notebook_ok=True, run_now_exc=None):
        self.jobs = list(jobs or [])
        self.active = list(active or [])
        self.states = list(states or [("TERMINATED", "SUCCESS")])
        self.notebook_ok = notebook_ok
        self.run_now_exc = run_now_exc
        self.calls = []
        self.polls = 0
        self.notebook_paths = []

    def notebook_exists(self, path):
        self.notebook_paths.append(path)
        return self.notebook_ok

    def find_jobs_by_name(self, name):
        self.looked_up = name
        return self.jobs

    def active_runs(self, job_id):
        return self.active

    def create(self, spec):
        self.calls.append(("create", spec))
        return 111

    def reset(self, job_id, spec):
        self.calls.append(("reset", job_id))

    def run_now(self, job_id):
        if self.run_now_exc:
            raise self.run_now_exc
        self.calls.append(("run_now", job_id))
        return 222

    def get_run(self, run_id):
        i = min(self.polls, len(self.states) - 1)
        self.polls += 1
        life, res = self.states[i]
        return {"state": {"life_cycle_state": life, "result_state": res}, "run_page_url": "http://x",
                "tasks": [{"task_key": "execute_po_operations", "run_id": 333}]}

    def get_run_output(self, tid):
        return {"notebook_output": {"result": '{"successes": 1}'}}


def quiet(*_a, **_k):
    return None


def test_creates_job_when_absent_and_runs():
    c = FakeClient()
    res = p.run_wrapper(c, cfg(), sleep=quiet, log=quiet)
    assert [x[0] for x in c.calls] == ["create", "run_now"]
    assert (res["job_id"], res["run_id"]) == (111, 222)
    assert res["job_action"] == "created" and res["job_name"] == "po-control"
    assert res["state"] == "TERMINATED" and res["result_state"] == "SUCCESS"
    assert res["worker_summary"] == '{"successes": 1}'
    assert c.looked_up == "po-control"


def test_updates_existing_job_via_reset_and_reports_updated():
    c = FakeClient(jobs=[55])
    res = p.run_wrapper(c, cfg(), sleep=quiet, log=quiet)
    assert [x[0] for x in c.calls] == ["reset", "run_now"]
    assert res["job_id"] == 55 and res["job_action"] == "updated"


def test_update_overwrites_definition_incl_compute_switch():
    c = ClusterFake(jobs=[55])
    res = p.run_wrapper(c, cfg(compute_type="job_cluster"), sleep=quiet, log=quiet)
    assert [x[0] for x in c.calls] == ["reset", "run_now"] and res["job_action"] == "updated"


def test_refuses_when_job_has_active_run():
    c = FakeClient(jobs=[55], active=[999])
    with pytest.raises(p.JobApiError, match="active run") as e:
        p.run_wrapper(c, cfg(), sleep=quiet, log=quiet)
    assert e.value.job_id == 55
    assert c.calls == []  # neither modified nor started


def test_multiple_jobs_same_name_is_an_error():
    with pytest.raises(p.JobApiError, match="Multiple jobs"):
        p.run_wrapper(FakeClient(jobs=[1, 2]), cfg(), sleep=quiet, log=quiet)


def test_missing_worker_notebook():
    c = FakeClient(notebook_ok=False)
    with pytest.raises(p.ConfigError, match="worker notebook"):
        p.run_wrapper(c, cfg(), sleep=quiet, log=quiet)
    assert c.calls == []


def test_worker_notebook_checked_is_the_derived_sibling():
    c = FakeClient()
    conf = cfg(worker_notebook_path=p.sibling_worker_path("/Users/me/tools/po_wrapper"))
    p.run_wrapper(c, conf, sleep=quiet, log=quiet)
    assert c.notebook_paths == ["/Users/me/tools/po_worker"]
    assert c.calls[0][1]["tasks"][0]["notebook_task"]["notebook_path"] == "/Users/me/tools/po_worker"


def test_run_now_failure_reports_job_id():
    c = FakeClient(run_now_exc=RuntimeError("serverless not enabled"))
    with pytest.raises(p.JobApiError, match="serverless not enabled") as e:
        p.run_wrapper(c, cfg(), sleep=quiet, log=quiet)
    assert e.value.job_id == 111 and e.value.run_id is None


# ------------------------------------------------------------ waiting (always on, 180 min)
def test_always_waits_for_the_run():
    c = FakeClient(states=[("PENDING", ""), ("RUNNING", ""), ("TERMINATED", "FAILED")])
    res = p.run_wrapper(c, cfg(), sleep=quiet, log=quiet)
    assert c.polls == 3 and res["result_state"] == "FAILED" and not res["timed_out"]


def test_polling_timeout_is_180_minutes_and_does_not_cancel_or_infer_final_state():
    c = FakeClient(states=[("RUNNING", "")])
    t = {"now": 0.0, "slept": 0.0}

    def mono():
        return t["now"]

    def sleep(s):
        t["now"] += 600  # 10 minutes per poll
        t["slept"] += 600

    res = p.run_wrapper(c, cfg(), sleep=sleep, monotonic=mono, log=quiet)
    assert res["timed_out"] is True
    assert t["slept"] == 180 * 60  # gave up after exactly 180 minutes, not 60
    assert res["run_id"] == 222
    assert "RUNNING" in res["state"] and "NOT cancelled" in res["state"] and "180 min" in res["state"]
    assert res["result_state"] == ""  # no false success/failure
    assert all(call[0] != "cancel" for call in c.calls)


# ============================================================ compute type
def test_default_compute_is_serverless():
    c = cfg()
    assert c["compute_type"] == "serverless"
    spec = p.build_job_spec(c)
    task = spec["tasks"][0]
    assert task["environment_key"] and task["disable_auto_optimization"] is True
    assert "job_clusters" not in spec and "existing_cluster_id" not in task and "libraries" not in task
    assert task["notebook_task"]["base_parameters"]["compute_type"] == "serverless"


def test_invalid_compute_type():
    for bad in ("classic", "gpu", ""):
        with pytest.raises(p.ConfigError, match="compute_type"):
            cfg(compute_type=bad)


def test_compute_type_is_case_insensitive():
    assert cfg(compute_type=" Job_Cluster ")["compute_type"] == "job_cluster"


@pytest.mark.parametrize(
    "over,frag",
    [
        (dict(compute_type="all_purpose"), "all_purpose_cluster_id is required"),
        (dict(compute_type="all_purpose", all_purpose_cluster_id="  "), "is required"),
        (dict(compute_type="serverless", all_purpose_cluster_id="0101-abc"), "no silent fallback"),
        (dict(compute_type="job_cluster", all_purpose_cluster_id="0101-abc"), "no silent fallback"),
    ],
)
def test_invalid_compute_settings(over, frag):
    with pytest.raises(p.ConfigError, match=frag):
        cfg(**over)


def test_job_cluster_spec_is_single_node_dbr_17_3_on_dsv3():
    spec = p.build_job_spec(cfg(compute_type="job_cluster"))
    task = spec["tasks"][0]
    assert "environments" not in spec  # serverless-only
    assert "environment_key" not in task and "disable_auto_optimization" not in task
    assert task["max_retries"] == 0
    assert task["job_cluster_key"] == spec["job_clusters"][0]["job_cluster_key"]
    assert "existing_cluster_id" not in task
    assert {"pypi": {"package": "openpyxl"}} in task["libraries"]
    nc = spec["job_clusters"][0]["new_cluster"]
    assert nc["spark_version"] == "17.3.x-scala2.13" and nc["node_type_id"] == "Standard_D4s_v3"
    assert nc["num_workers"] == 0  # single node
    assert nc["spark_conf"]["spark.databricks.cluster.profile"] == "singleNode"
    assert nc["spark_conf"]["spark.master"] == "local[*]"
    assert nc["custom_tags"] == {"ResourceClass": "SingleNode"}
    assert nc["data_security_mode"] == p.JOB_CLUSTER_DATA_SECURITY_MODE
    assert task["notebook_task"]["base_parameters"]["compute_type"] == "job_cluster"


def test_all_purpose_spec_uses_existing_cluster_id():
    c = cfg(compute_type="all_purpose", all_purpose_cluster_id=" 0101-abc ")
    spec = p.build_job_spec(c)
    task = spec["tasks"][0]
    assert task["existing_cluster_id"] == "0101-abc"
    assert "job_clusters" not in spec and "job_cluster_key" not in task and "environments" not in spec
    assert task["max_retries"] == 0 and task["libraries"]
    assert task["notebook_task"]["base_parameters"]["compute_type"] == "all_purpose"


class ClusterFake(FakeClient):
    def __init__(self, cluster=None, versions=None, nodes=None, **kw):
        super().__init__(**kw)
        self.cluster = cluster
        self._versions = versions if versions is not None else [
            {"key": "17.3.x-scala2.13", "name": "17.3 LTS (Scala 2.13)"}]
        self._nodes = nodes if nodes is not None else [
            {"node_type_id": "Standard_D4s_v3", "category": "General Purpose", "num_cores": 4,
             "memory_mb": 16384, "node_info": {}}]

    def spark_versions(self):
        return self._versions

    def node_types(self):
        return self._nodes

    def get_cluster(self, cid):
        return self.cluster


GOOD_CLUSTER = {"cluster_name": "po", "state": "RUNNING", "data_security_mode": "SINGLE_USER"}


def test_wrapper_job_cluster_happy_path_uses_the_fixed_cluster():
    c = ClusterFake()
    res = p.run_wrapper(c, cfg(compute_type="job_cluster"), sleep=quiet, log=quiet)
    nc = c.calls[0][1]["job_clusters"][0]["new_cluster"]
    assert nc["spark_version"] == "17.3.x-scala2.13" and nc["node_type_id"] == "Standard_D4s_v3"
    assert nc["num_workers"] == 0
    assert res["compute_type"] == "job_cluster" and res["result_state"] == "SUCCESS"


def test_job_cluster_runtime_missing_in_workspace_is_a_clear_error_with_no_fallback():
    c = ClusterFake(versions=[{"key": "16.4.x-scala2.13", "name": "16.4 LTS"}])
    with pytest.raises(p.ConfigError, match="17.3.x-scala2.13 is not available"):
        p.run_wrapper(c, cfg(compute_type="job_cluster"), sleep=quiet, log=quiet)
    assert c.calls == []  # no job created, in particular no serverless one


def test_job_cluster_node_missing_in_workspace_is_a_clear_error_with_no_fallback():
    c = ClusterFake(nodes=[])
    with pytest.raises(p.ConfigError, match="Standard_D4s_v3 is not available"):
        p.run_wrapper(c, cfg(compute_type="job_cluster"), sleep=quiet, log=quiet)
    assert c.calls == []


@pytest.mark.parametrize("status", [["NotEnabledOnSubscription"], ["NotAvailableInRegion"]])
def test_job_cluster_unlaunchable_node_is_rejected_up_front(status):
    """Regression: a node not enabled on the subscription leaves the cluster PENDING forever."""
    node = {"node_type_id": "Standard_D4s_v3", "node_info": {"status": status}}
    c = ClusterFake(nodes=[node])
    with pytest.raises(p.ConfigError, match="cannot be launched"):
        p.run_wrapper(c, cfg(compute_type="job_cluster"), sleep=quiet, log=quiet)
    assert c.calls == []


def test_job_cluster_deprecated_node_is_rejected():
    c = ClusterFake(nodes=[{"node_type_id": "Standard_D4s_v3", "is_deprecated": True, "node_info": {}}])
    with pytest.raises(p.ConfigError, match="cannot be launched"):
        p.run_wrapper(c, cfg(compute_type="job_cluster"), sleep=quiet, log=quiet)


def test_serverless_does_not_query_cluster_catalogs():
    class Boom(FakeClient):
        def spark_versions(self):
            raise AssertionError("must not be called for serverless")

        node_types = spark_versions

    p.run_wrapper(Boom(), cfg(), sleep=quiet, log=quiet)


def test_wrapper_all_purpose_happy_path():
    c = ClusterFake(cluster=GOOD_CLUSTER)
    res = p.run_wrapper(c, cfg(compute_type="all_purpose", all_purpose_cluster_id="0101-abc"),
                        sleep=quiet, log=quiet)
    assert c.calls[0][1]["tasks"][0]["existing_cluster_id"] == "0101-abc"
    assert res["compute_type"] == "all_purpose"


def test_wrapper_all_purpose_cluster_must_exist():
    c = ClusterFake(cluster=None)
    with pytest.raises(p.ConfigError, match="all_purpose_cluster_id not found"):
        p.run_wrapper(c, cfg(compute_type="all_purpose", all_purpose_cluster_id="0101-abc"), sleep=quiet, log=quiet)
    assert c.calls == []  # nothing created or started


@pytest.mark.parametrize("mode", ["NONE", "LEGACY_TABLE_ACL", "LEGACY_SINGLE_USER"])
def test_wrapper_rejects_all_purpose_cluster_without_uc_access_mode(mode):
    c = ClusterFake(cluster={"data_security_mode": mode})
    with pytest.raises(p.ConfigError, match="Unity Catalog"):
        p.run_wrapper(c, cfg(compute_type="all_purpose", all_purpose_cluster_id="0101-abc"), sleep=quiet, log=quiet)
    assert c.calls == []


@pytest.mark.parametrize("mode", ["SINGLE_USER", "USER_ISOLATION", "DATA_SECURITY_MODE_AUTO"])
def test_wrapper_accepts_uc_capable_access_modes(mode):
    c = ClusterFake(cluster={"data_security_mode": mode})
    p.run_wrapper(c, cfg(compute_type="all_purpose", all_purpose_cluster_id="0101-abc"), sleep=quiet, log=quiet)
    assert [x[0] for x in c.calls] == ["create", "run_now"]


# ============================================================ remote workspace settings
URL = "https://adb-1234567890123456.7.azuredatabricks.net"
REMOTE = dict(remote_workspace_url=URL, remote_client_id="11111111-2222-3333-4444-555555555555",
              remote_secret_scope="po-scope", remote_secret_key="po-key", remote_warehouse_id="abcdef0123456789")


def test_remote_defaults_to_disabled():
    c = cfg()
    assert c["remote"] is None
    bp = p.build_job_spec(c)["tasks"][0]["notebook_task"]["base_parameters"]
    for k in REMOTE_WIDGETS:
        assert bp[k] == ""  # worker widgets always exist, blank = local only


def test_remote_settings_are_normalized_and_passed_to_the_worker():
    c = cfg(**dict(REMOTE, remote_workspace_url="  " + URL + "/?o=1  ", remote_warehouse_id=" abcdef0123456789 "))
    assert c["remote"] == {"workspace_url": URL, "client_id": REMOTE["remote_client_id"],
                           "secret_scope": "po-scope", "secret_key": "po-key",
                           "warehouse_id": "abcdef0123456789"}
    bp = p.build_job_spec(c)["tasks"][0]["notebook_task"]["base_parameters"]
    assert {k: bp[k] for k in REMOTE_WIDGETS} == REMOTE
    assert bp["csv_path"].endswith(".csv")  # other parameters untouched


def test_remote_settings_work_with_every_compute_type():
    for compute, extra in (("serverless", {}), ("job_cluster", {}),
                           ("all_purpose", {"all_purpose_cluster_id": "0101-abc"})):
        spec = p.build_job_spec(cfg(compute_type=compute, **extra, **REMOTE))
        bp = spec["tasks"][0]["notebook_task"]["base_parameters"]
        assert bp["remote_workspace_url"] == URL and bp["compute_type"] == compute


@pytest.mark.parametrize("blank", sorted(REMOTE_WIDGETS))
def test_partial_remote_settings_are_rejected_before_anything_is_created(blank):
    c = FakeClient()
    with pytest.raises(p.ConfigError, match="incomplete.*%s" % blank):
        p.run_wrapper(c, cfg(**dict(REMOTE, **{blank: ""})), sleep=quiet, log=quiet)
    assert c.calls == []


@pytest.mark.parametrize("over,frag", [
    (dict(remote_workspace_url="https://evil.example.com"), "Databricks workspace host"),
    (dict(remote_workspace_url="http://adb-1.2.azuredatabricks.net"), "https://"),
    (dict(remote_workspace_url="adb-1.2.azuredatabricks.net"), "https://"),
    (dict(remote_workspace_url="https://u:p@adb-1.2.azuredatabricks.net"), "credentials"),
    (dict(remote_secret_scope="bad scope"), "remote_secret_scope"),
    (dict(remote_secret_key="../x"), "remote_secret_key"),
    (dict(remote_warehouse_id="my warehouse"), "remote_warehouse_id"),
    (dict(remote_warehouse_id="abc"), "remote_warehouse_id"),
    (dict(remote_client_id="a b"), "remote_client_id"),
])
def test_invalid_remote_settings(over, frag):
    with pytest.raises(p.ConfigError, match=frag):
        cfg(**dict(REMOTE, **over))


def test_remote_settings_do_not_change_the_rest_of_the_job_spec():
    plain = p.build_job_spec(cfg())
    remote = p.build_job_spec(cfg(**REMOTE))
    strip = lambda sp: {k: v for k, v in sp["tasks"][0]["notebook_task"]["base_parameters"].items()
                        if k not in REMOTE_WIDGETS}
    assert strip(plain) == strip(remote)
    assert {k: v for k, v in plain.items() if k != "tasks"} == {k: v for k, v in remote.items() if k != "tasks"}
    assert plain["tasks"][0]["max_retries"] == remote["tasks"][0]["max_retries"] == 0


def test_run_with_remote_settings_creates_and_runs_the_job():
    c = FakeClient()
    res = p.run_wrapper(c, cfg(**REMOTE), sleep=quiet, log=quiet)
    assert [x[0] for x in c.calls] == ["create", "run_now"] and res["result_state"] == "SUCCESS"
    assert c.calls[0][1]["tasks"][0]["notebook_task"]["base_parameters"]["remote_warehouse_id"] == "abcdef0123456789"
