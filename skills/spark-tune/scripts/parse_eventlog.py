#!/usr/bin/env python3
# /// script
# requires-python = ">=3.6"
# ///
"""
Spark SQL Event Log QPL Parser.

Extracts query plan information from Spark event log files (JSON-per-line)
as a fallback when the Spark UI HTML is unavailable.

Produces JSON output with per-operator metrics embedded inline, matching
the same format as parse_qpl.py (no Excel output).

Usage:
    python3 parse_eventlog.py <eventlog_file_or_dir> --query <id> [--query <id2>] [--force]
    python3 parse_eventlog.py <eventlog_file_or_dir> --all [--force]

The --query flag can be repeated. --all extracts every execution found.
Multiple event log files (rollover segments) can be passed as a directory.
Zero external dependencies -- works with plain python3.
"""

import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_COMMAND_KEYWORDS = (
    "Execute", "MergeIntoCommandEdge", "DeleteCommandEdge",
    "WriteIntoDeltaCommand", "InsertIntoHadoopFsRelation",
)

_PATH_PATTERN = re.compile(
    r"((?:dbfs|s3|s3a|abfss|wasbs|gs)://?[^\s,\)\]]+)"
)
_TABLE_ID_PATTERN = re.compile(
    r"/tables/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)

_WANTED_EVENT_SUBSTRINGS = frozenset({
    "SQLExecutionStart", "SQLExecutionEnd",
    "SQLAdaptiveExecutionUpdate", "SQLAdaptiveSQLMetricUpdates",
    "DriverAccumUpdates",
    "SparkListenerJobStart", "SparkListenerJobEnd",
    "SparkListenerStageCompleted", "SparkListenerTaskEnd",
})

_EARLY_TERM_THRESHOLD = 5000

_TIME_WINDOW_BUFFER_MS = 5 * 60 * 1000


def _parse_time_arg(value: str) -> int:
    """Parse a time argument into epoch milliseconds.

    Accepts:
      - Epoch milliseconds: "1712345678000"
      - ISO 8601: "2024-04-10T14:30:00"
      - Spark UI format: "2024/04/10 14:30:00"
    """
    from datetime import datetime, timezone

    value = value.strip()
    if value.isdigit() and len(value) >= 12:
        return int(value)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(
        f"Cannot parse time '{value}'. "
        f"Use epoch ms (1712345678000), ISO (2024-04-10T14:30:00), "
        f"or Spark UI format (2024/04/10 14:30:00)."
    )


# ---------------------------------------------------------------------------
# Event log reading
# ---------------------------------------------------------------------------

def read_event_log_files(
    paths,                    # type: List[Path]
    wanted_substrings=None,   # type: Optional[frozenset]
    target_execution_ids=None, # type: Optional[Set[int]]
):  # type: (...) -> List[dict]
    """Read one or more event log files, return list of parsed JSON events.

    Two layers of filtering, applied in order:

    1. **String pre-filter** (*wanted_substrings*) -- each raw line is
       checked for at least one matching substring **before**
       ``json.loads()`` is called.  Lines that don't match are skipped
       entirely, avoiding the expensive JSON parse.

    2. **Execution-ID post-filter** (*target_execution_ids*) -- after
       parsing, events that carry an ``executionId`` field (all SQL-level
       events) are discarded if their ID is not in the target set.  Events
       without ``executionId`` (job / stage / task) are always kept.
    """
    events = []
    skipped_pre = 0
    skipped_eid = 0
    for p in sorted(paths):
        print(f"  Reading: {p.name}")
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if wanted_substrings and not any(w in line for w in wanted_substrings):
                    skipped_pre += 1
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if target_execution_ids is not None:
                    eid = ev.get("executionId")
                    if eid is not None and eid not in target_execution_ids:
                        skipped_eid += 1
                        continue

                events.append(ev)

    parts = [f"Events loaded: {len(events):,}"]
    if skipped_pre:
        parts.append(f"lines skipped (event-type): {skipped_pre:,}")
    if skipped_eid:
        parts.append(f"lines skipped (executionId): {skipped_eid:,}")
    print("  " + ", ".join(parts))
    return events


def find_eventlog_files(path):  # type: (Path) -> List[Path]
    """Find event log files in a path (file or directory)."""
    if path.is_file():
        return [path]
    results = []
    for f in sorted(path.rglob("*")):
        if f.is_file() and f.suffix in (".txt", ".json", ".log", ""):
            if f.stat().st_size > 1000:
                results.append(f)
    return results


# ---------------------------------------------------------------------------
# Event classification and grouping
# ---------------------------------------------------------------------------

def classify_events(events):  # type: (List[dict]) -> dict
    """Group events by type into a lookup structure."""
    classified = {
        "sql_start": [],
        "sql_end": [],
        "aqe_update": [],
        "metric_update": [],
        "driver_accum": [],
        "job_start": [],
        "job_end": [],
        "stage_completed": [],
        "task_end": [],
    }
    for ev in events:
        etype = ev.get("Event", "")
        if "SQLExecutionStart" in etype:
            classified["sql_start"].append(ev)
        elif "SQLExecutionEnd" in etype:
            classified["sql_end"].append(ev)
        elif "SQLAdaptiveExecutionUpdate" in etype:
            classified["aqe_update"].append(ev)
        elif "SQLAdaptiveSQLMetricUpdates" in etype:
            classified["metric_update"].append(ev)
        elif "DriverAccumUpdates" in etype:
            classified["driver_accum"].append(ev)
        elif etype == "SparkListenerJobStart":
            classified["job_start"].append(ev)
        elif etype == "SparkListenerJobEnd":
            classified["job_end"].append(ev)
        elif etype == "SparkListenerStageCompleted":
            classified["stage_completed"].append(ev)
        elif etype == "SparkListenerTaskEnd":
            classified["task_end"].append(ev)
    return classified


def stream_task_accum_values(
    paths,                  # type: List[Path]
    target_accum_ids,       # type: Set[int]
    target_stage_ids=None,  # type: Optional[Set[int]]
    time_window_ms=None,    # type: Optional[Tuple[int, int]]
):  # type: (...) -> Dict[int, list]
    """Stream through TaskEnd events collecting values for target accumulators.

    Memory-efficient second pass: only parses lines containing
    ``SparkListenerTaskEnd``, only stores values for accumulators we care
    about.  Never loads all TaskEnd events into memory.

    Filters (applied in order, cheapest first):

    1. **Stage ID** (*target_stage_ids*) -- cheap int check from the
       canonical ``JobStart -> Stage IDs`` mapping.
    2. **Time window** (*time_window_ms*) -- ``(start_ms, end_ms)`` tuple.
       Tasks whose ``Finish Time`` falls outside the window are skipped.
       When an end time is set, **early termination** kicks in: after
       ``_EARLY_TERM_THRESHOLD`` consecutive tasks past the end time the
       file is abandoned (event logs are chronological).
    3. **Accumulator ID** (*target_accum_ids*) -- only values for known
       accumulators are stored.
    """
    accum_values = defaultdict(list)  # type: Dict[int, list]
    tasks_scanned = 0
    tasks_skipped_stage = 0
    tasks_skipped_time = 0
    tasks_matched = 0
    early_terminated = False
    consecutive_past_end = 0

    tw_start = time_window_ms[0] if time_window_ms else None
    tw_end = time_window_ms[1] if time_window_ms else None

    for p in sorted(paths):
        if early_terminated:
            break
        consecutive_past_end = 0

        with open(p, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if "SparkListenerTaskEnd" not in line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue

                tasks_scanned += 1

                # Filter 1: time window + early termination.
                # Applied BEFORE stage filter so the early-termination
                # counter tracks ALL tasks chronologically, not just
                # those in our target stages.
                task_info = ev.get("Task Info", {})
                if tw_start is not None or tw_end is not None:
                    finish_time = task_info.get("Finish Time", 0)
                    if tw_end and finish_time > tw_end:
                        tasks_skipped_time += 1
                        consecutive_past_end += 1
                        if consecutive_past_end >= _EARLY_TERM_THRESHOLD:
                            early_terminated = True
                            break
                        continue
                    if tw_start and finish_time < tw_start:
                        tasks_skipped_time += 1
                        continue
                    consecutive_past_end = 0

                # Filter 2: stage ID (cheap int lookup)
                if target_stage_ids is not None:
                    if ev.get("Stage ID") not in target_stage_ids:
                        tasks_skipped_stage += 1
                        continue

                if task_info.get("Failed", False):
                    continue

                # Filter 3: accumulator ID
                matched = False
                for acc in task_info.get("Accumulables", []):
                    aid = acc.get("ID")
                    if aid is not None and aid in target_accum_ids:
                        update = acc.get("Update")
                        if update is not None:
                            matched = True
                            try:
                                accum_values[aid].append(int(update))
                            except (ValueError, TypeError):
                                try:
                                    accum_values[aid].append(float(update))
                                except (ValueError, TypeError):
                                    pass

                if matched:
                    tasks_matched += 1

    parts = [f"tasks scanned: {tasks_scanned:,}"]
    if tasks_skipped_stage:
        parts.append(f"skipped (stage): {tasks_skipped_stage:,}")
    if tasks_skipped_time:
        parts.append(f"skipped (time): {tasks_skipped_time:,}")
    parts.append(f"matched: {tasks_matched:,}")
    if early_terminated:
        parts.append("EARLY TERMINATION (past end of time window)")
    print(f"  Task streaming: {', '.join(parts)}")
    return dict(accum_values)


def list_executions(classified):  # type: (dict) -> List[dict]
    """List all SQL executions found in the event log."""
    execs = []
    start_map = {ev["executionId"]: ev for ev in classified["sql_start"]}
    end_map = {ev["executionId"]: ev for ev in classified["sql_end"]}
    aqe_eids = {ev["executionId"] for ev in classified["aqe_update"]}

    for eid, start_ev in sorted(start_map.items()):
        end_ev = end_map.get(eid)
        has_plan = (
            eid in aqe_eids
            or "sparkPlanInfo" in start_ev
            or "physicalPlanDescription" in start_ev
        )
        info = {
            "executionId": eid,
            "rootExecutionId": start_ev.get("rootExecutionId"),
            "has_plan": has_plan,
            "has_end": end_ev is not None,
            "error": end_ev.get("errorMessage", "") if end_ev else None,
        }
        execs.append(info)
    return execs


# ---------------------------------------------------------------------------
# Extraction for a single execution ID
# ---------------------------------------------------------------------------

def extract_execution(
    execution_id: int,
    classified: dict,
    precomputed_task_accums=None,  # type: Optional[Dict[int, list]]
):  # type: (...) -> Optional[dict]
    """Extract all QPL-equivalent data for a single execution ID.

    When *precomputed_task_accums* is provided (from
    ``stream_task_accum_values``), the expensive per-task scanning is
    skipped entirely -- accum values are looked up directly.
    """

    start_ev = None
    for ev in classified["sql_start"]:
        if ev["executionId"] == execution_id:
            start_ev = ev
            break

    aqe_events = [
        ev for ev in classified["aqe_update"]
        if ev["executionId"] == execution_id
    ]
    plan_ev = None
    if aqe_events:
        for ev in reversed(aqe_events):
            desc = ev.get("physicalPlanDescription", "")
            if "Final Plan" in desc:
                plan_ev = ev
                break
        if plan_ev is None:
            plan_ev = aqe_events[-1]
    elif start_ev and "sparkPlanInfo" in start_ev:
        plan_ev = start_ev

    if plan_ev is None:
        print(f"  WARNING: No physical plan found for execution {execution_id}")
        return None

    end_ev = None
    for ev in classified["sql_end"]:
        if ev["executionId"] == execution_id:
            end_ev = ev
            break

    spark_plan_info = plan_ev.get("sparkPlanInfo")
    physical_plan_text = plan_ev.get("physicalPlanDescription", "")

    if spark_plan_info is None and start_ev:
        spark_plan_info = start_ev.get("sparkPlanInfo")
    if not physical_plan_text and start_ev:
        physical_plan_text = start_ev.get("physicalPlanDescription", "")

    if spark_plan_info is None:
        print(f"  WARNING: No sparkPlanInfo found for execution {execution_id}")
        return None

    accum_map = {}
    _walk_plan_tree_for_accums(spark_plan_info, accum_map)

    driver_accum_values = {}
    for ev in classified["driver_accum"]:
        if ev["executionId"] == execution_id:
            for pair in ev.get("accumUpdates", []):
                if len(pair) == 2:
                    aid, val = pair
                    driver_accum_values[aid] = val

    for ev in classified["metric_update"]:
        if ev["executionId"] == execution_id:
            for m in ev.get("sqlPlanMetrics", []):
                aid = m.get("accumulatorId")
                if aid is not None and aid not in accum_map:
                    accum_map[aid] = {
                        "name": m["name"],
                        "metricType": m.get("metricType", ""),
                        "node": None,
                        "explainId": None,
                    }

    if precomputed_task_accums is not None:
        task_accum_values = precomputed_task_accums
    else:
        task_accum_values = {}

    operators, stages = _build_operators_from_plan_tree(spark_plan_info)

    _resolve_operator_metrics(
        operators, accum_map, driver_accum_values, task_accum_values
    )

    query_metadata = _build_query_metadata(
        execution_id, start_ev, end_ev, aqe_events
    )

    command_metrics = _extract_command_metrics(operators)
    scan_metrics = _extract_scan_metrics(operators)
    table_paths = _extract_table_paths(operators)
    sql_properties = _extract_sql_properties(start_ev)
    execution_summary = _build_execution_summary(operators)

    return {
        "query_metadata": query_metadata,
        "operators": operators,
        "stages": stages,
        "physical_plan": physical_plan_text,
        "command_metrics": command_metrics,
        "scan_metrics": scan_metrics,
        "table_paths": table_paths,
        "execution_summary": execution_summary,
        "sql_properties": sql_properties,
    }


# ---------------------------------------------------------------------------
# Plan tree walking
# ---------------------------------------------------------------------------

def _walk_plan_tree_for_accums(node: dict, accum_map: dict):
    """Recursively walk sparkPlanInfo to collect accumulator definitions."""
    explain_id = node.get("explainId")
    node_name = node.get("nodeName", "")

    for m in node.get("metrics", []):
        aid = m.get("accumulatorId")
        if aid is not None:
            accum_map[aid] = {
                "name": m["name"],
                "metricType": m.get("metricType", ""),
                "node_name": node_name,
                "explainId": explain_id,
            }

    for child in node.get("children", []):
        _walk_plan_tree_for_accums(child, accum_map)


def _build_operators_from_plan_tree(root):  # type: (dict) -> Tuple[List[dict], List[dict]]
    """
    Build flat operator list from sparkPlanInfo tree.
    Returns (operators, stages) matching parse_qpl.py format.
    """
    operators = []
    stages = []
    _node_counter = [0]

    def walk(node, parent_id):  # type: (dict, Optional[int]) -> int
        nid = _node_counter[0]
        _node_counter[0] += 1

        node_name_raw = node.get("nodeName", "Unknown")
        explain_id = node.get("explainId")
        simple_str = node.get("simpleString", "")

        if explain_id is not None:
            display_name = f"{node_name_raw} ({explain_id})"
        else:
            display_name = node_name_raw

        is_stage = node_name_raw in (
            "WholeStageCodegen", "PhotonShuffleMapStage",
            "PhotonResultStage", "PhotonBroadcastStage",
        )

        metrics_defs = []
        for m in node.get("metrics", []):
            metrics_defs.append({
                "accumulatorId": m["accumulatorId"],
                "name": m["name"],
                "metricType": m.get("metricType", ""),
            })

        op = {
            "node_id": nid,
            "name": display_name,
            "stage": None,
            "children": [],
            "parents": [parent_id] if parent_id is not None else [],
            "edge_rows_to_parent": None,
            "metadata": simple_str,
            "_metrics_defs": metrics_defs,
            "_explain_id": explain_id,
        }
        operators.append(op)

        if is_stage:
            stages.append({
                "cluster_id": nid,
                "name": display_name,
                "info": simple_str,
                "node_ids": [],
            })

        child_ids = []
        for child_node in node.get("children", []):
            child_id = walk(child_node, nid)
            child_ids.append(child_id)
            if is_stage:
                stages[-1]["node_ids"].append(child_id)

        op["children"] = child_ids
        return nid

    walk(root, None)
    return operators, stages


# ---------------------------------------------------------------------------
# Stage/job mapping
# ---------------------------------------------------------------------------

_SQL_EXEC_ID_PROP = "spark.sql.execution.id"


def _build_execution_stage_map(classified):  # type: (dict) -> Dict[int, Set[int]]
    """Build mapping: execution_id -> set of stage IDs.

    Uses the official Spark mechanism: ``SparkListenerJobStart`` events
    carry the SQL execution ID in ``Properties["spark.sql.execution.id"]``
    (set by ``SQLExecution.withNewExecutionId()`` as a local property that
    propagates to submitted jobs).  The ``Stage IDs`` field of the same
    event lists all stages for that job.

    See ``SQLAppStatusListener.onJobStart()`` in the runtime for the
    authoritative implementation.
    """
    exec_stages = defaultdict(set)  # type: Dict[int, Set[int]]

    for ev in classified["job_start"]:
        props = ev.get("Properties", {})
        if not props:
            continue
        eid_str = props.get(_SQL_EXEC_ID_PROP)
        if eid_str is None:
            continue
        try:
            eid = int(eid_str)
        except (ValueError, TypeError):
            continue

        for sid in ev.get("Stage IDs", []):
            exec_stages[eid].add(sid)

    return dict(exec_stages)


def _find_stages_for_execution(execution_id, classified):  # type: (int, dict) -> Set[int]
    """Find all stage IDs that belong to an execution.

    Primary: reads ``SparkListenerJobStart`` Properties for the canonical
    ``spark.sql.execution.id`` link (same mechanism used by
    ``SQLAppStatusListener.onJobStart()`` in the Spark UI).

    Fallback: time-window match from ``StageCompleted`` events (for
    incomplete or truncated logs where ``JobStart`` events are missing).
    """
    stage_ids = set()

    for ev in classified["job_start"]:
        props = ev.get("Properties", {})
        if not props:
            continue
        eid_str = props.get(_SQL_EXEC_ID_PROP)
        if eid_str is None:
            continue
        try:
            eid = int(eid_str)
        except (ValueError, TypeError):
            continue
        if eid == execution_id:
            for sid in ev.get("Stage IDs", []):
                stage_ids.add(sid)

    if stage_ids:
        return stage_ids

    # Fallback: time-window heuristic (for incomplete logs).
    start_time = None
    end_time = None
    for ev in classified["sql_start"]:
        if ev["executionId"] == execution_id:
            start_time = ev.get("time")
            break
    for ev in classified["sql_end"]:
        if ev["executionId"] == execution_id:
            end_time = ev.get("time")
            break

    if start_time and end_time:
        for ev in classified["stage_completed"]:
            si = ev.get("Stage Info", {})
            sub_time = si.get("Submission Time", 0)
            if sub_time and start_time <= sub_time <= end_time + 60000:
                stage_ids.add(si["Stage ID"])

    return stage_ids


def _collect_accum_ids(node: dict, accum_ids: set):
    """Recursively collect all accumulator IDs from a sparkPlanInfo tree."""
    for m in node.get("metrics", []):
        aid = m.get("accumulatorId")
        if aid is not None:
            accum_ids.add(aid)
    for child in node.get("children", []):
        _collect_accum_ids(child, accum_ids)


def _get_accum_ids_for_execution(execution_id, classified):  # type: (int, dict) -> Set[int]
    """Lightweight pass: return all accumulator IDs for an execution.

    Walks the plan tree and metric-update events without building the full
    operator list -- just enough to know which accumulators to look for in
    the TaskEnd streaming pass.
    """
    start_ev = None
    for ev in classified["sql_start"]:
        if ev["executionId"] == execution_id:
            start_ev = ev
            break

    aqe_events = [
        ev for ev in classified["aqe_update"]
        if ev["executionId"] == execution_id
    ]
    plan_ev = None
    if aqe_events:
        for ev in reversed(aqe_events):
            if "Final Plan" in ev.get("physicalPlanDescription", ""):
                plan_ev = ev
                break
        if plan_ev is None:
            plan_ev = aqe_events[-1]
    elif start_ev and "sparkPlanInfo" in start_ev:
        plan_ev = start_ev

    if plan_ev is None:
        return set()

    spark_plan_info = plan_ev.get("sparkPlanInfo")
    if spark_plan_info is None and start_ev:
        spark_plan_info = start_ev.get("sparkPlanInfo")
    if spark_plan_info is None:
        return set()

    accum_ids = set()  # type: Set[int]
    _collect_accum_ids(spark_plan_info, accum_ids)

    for ev in classified["metric_update"]:
        if ev["executionId"] == execution_id:
            for m in ev.get("sqlPlanMetrics", []):
                aid = m.get("accumulatorId")
                if aid is not None:
                    accum_ids.add(aid)

    return accum_ids


# ---------------------------------------------------------------------------
# Metric resolution
# ---------------------------------------------------------------------------

def _format_size(nbytes: float) -> str:
    """Format bytes into human-readable size."""
    if nbytes == 0:
        return "0.0 B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    if nbytes < 0:
        nbytes = abs(nbytes)
    exp = min(int(math.log(nbytes, 1024)), len(units) - 1) if nbytes > 0 else 0
    val = nbytes / (1024 ** exp)
    return f"{val:.1f} {units[exp]}"


def _format_timing(nanos: float) -> str:
    """Format nanoseconds into human-readable time."""
    if nanos == 0:
        return "0 ms"
    ms = nanos / 1_000_000
    if ms < 1000:
        return f"{ms:.0f} ms"
    s = ms / 1000
    if s < 60:
        return f"{s:.1f} s"
    m = s / 60
    if m < 60:
        return f"{m:.1f} min"
    h = m / 60
    return f"{h:.1f} h"


def _format_ns_timing(nanos: float) -> str:
    """Format nsTiming metrics (already in nanoseconds)."""
    return _format_timing(nanos)


def _format_value(value: float, metric_type: str) -> str:
    """Format a metric value based on its type."""
    if metric_type == "size":
        return _format_size(value)
    elif metric_type in ("timing", "nsTiming"):
        return _format_ns_timing(value)
    elif metric_type == "average":
        return f"{value:.2f}"
    else:
        try:
            if value == int(value) and math.isfinite(value):
                return f"{int(value):,}"
        except (OverflowError, ValueError):
            pass
        return f"{value:,.2f}"


def _format_metric_with_breakdown(
    total, values, metric_type  # type: (float, List[float], str)
) -> str:
    """Format as 'total (min, med, max)' matching HTML QPL format."""
    formatted_total = _format_value(total, metric_type)
    if len(values) <= 1:
        return formatted_total
    sorted_vals = sorted(values)
    min_v = sorted_vals[0]
    max_v = sorted_vals[-1]
    med_v = median(sorted_vals)
    f_min = _format_value(min_v, metric_type)
    f_med = _format_value(med_v, metric_type)
    f_max = _format_value(max_v, metric_type)
    return f"{formatted_total} ({f_min}, {f_med}, {f_max})"


def _resolve_operator_metrics(
    operators,            # type: List[dict]
    accum_map,            # type: dict
    driver_accum_values,  # type: dict
    task_accum_values,    # type: dict
):
    """Resolve accumulator values into human-readable metrics for each operator."""
    for op in operators:
        resolved = []
        for mdef in op.get("_metrics_defs", []):
            aid = mdef["accumulatorId"]
            name = mdef["name"]
            mtype = mdef["metricType"]

            task_vals = task_accum_values.get(aid, [])
            driver_val = driver_accum_values.get(aid)

            if task_vals:
                total = sum(task_vals)
                formatted = _format_metric_with_breakdown(total, task_vals, mtype)
            elif driver_val is not None:
                formatted = _format_value(driver_val, mtype)
            else:
                formatted = "0" if mtype == "sum" else "0.0 B" if mtype == "size" else "0 ms" if mtype in ("timing", "nsTiming") else "0"

            resolved.append({
                "metric": name,
                "value": formatted,
            })

        if resolved:
            op["metrics"] = resolved
        # Clean up internal fields
        del op["_metrics_defs"]
        del op["_explain_id"]


# ---------------------------------------------------------------------------
# Query metadata
# ---------------------------------------------------------------------------

def _build_query_metadata(
    execution_id: int,
    start_ev,     # type: Optional[dict]
    end_ev,       # type: Optional[dict]
    aqe_events,   # type: List[dict]
):  # type: (...) -> dict
    meta = {"title": f"Details for Query {execution_id}"}
    if start_ev:
        t = start_ev.get("time")
        if t:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
            meta["submitted_time"] = dt.strftime("%Y/%m/%d %H:%M:%S")
    if end_ev:
        end_t = end_ev.get("time")
        start_t = start_ev.get("time") if start_ev else None
        if end_t and start_t:
            dur_ms = end_t - start_t
            meta["duration"] = _format_timing(dur_ms * 1_000_000)
        err = end_ev.get("errorMessage", "")
        if err:
            meta["error_message"] = err
    if aqe_events:
        meta["aqe_plan_versions"] = list(range(len(aqe_events)))
    meta["execution_id"] = execution_id
    if start_ev:
        meta["root_execution_id"] = start_ev.get("rootExecutionId")
    return meta


# ---------------------------------------------------------------------------
# Command / scan / table path extraction (from resolved operators)
# ---------------------------------------------------------------------------

def _detect_command_type(name: str) -> str:
    nl = name.lower()
    if "delete" in nl:
        return "delete"
    elif "merge" in nl:
        return "merge"
    elif "write" in nl or "insert" in nl:
        return "write"
    return "unknown"


def _extract_command_metrics(operators):  # type: (List[dict]) -> Optional[dict]
    for op in operators:
        name = op.get("name", "")
        if not any(kw in name for kw in _COMMAND_KEYWORDS):
            continue
        metrics = op.get("metrics", [])
        if not metrics:
            continue
        return {
            "command_type": _detect_command_type(name),
            "operator_name": name,
            "node_id": op["node_id"],
            "metrics": {m["metric"]: m["value"] for m in metrics},
        }
    return None


def _extract_scan_metrics(operators):  # type: (List[dict]) -> List[dict]
    scans = []
    for op in operators:
        name = op.get("name", "")
        if "Scan" not in name:
            continue
        metrics = op.get("metrics", [])
        if not metrics:
            continue
        nl = name.lower()
        if "parquet" in nl:
            scan_type = "parquet"
        elif "jdbc" in nl:
            scan_type = "jdbc"
        elif "orc" in nl:
            scan_type = "orc"
        elif "csv" in nl:
            scan_type = "csv"
        elif "json" in nl:
            scan_type = "json"
        else:
            scan_type = "other"
        scans.append({
            "operator_name": name,
            "node_id": op["node_id"],
            "scan_type": scan_type,
            "metrics": {m["metric"]: m["value"] for m in metrics},
        })
    return scans


def _extract_table_paths(operators):  # type: (List[dict]) -> List[dict]
    paths_seen = set()  # type: Set[str]
    results = []
    for op in operators:
        meta = op.get("metadata") or ""
        found = _PATH_PATTERN.findall(meta)
        if not found:
            continue
        name = op.get("name", "")
        is_write = any(kw in name for kw in _COMMAND_KEYWORDS)
        access_type = "write" if is_write else "read"
        for p in found:
            p = p.rstrip(",).;")
            if p in paths_seen:
                continue
            paths_seen.add(p)
            tid_m = _TABLE_ID_PATTERN.search(p)
            results.append({
                "path": p,
                "table_id": tid_m.group(1) if tid_m else None,
                "access_type": access_type,
                "operator_name": name,
            })
    return results


def _extract_sql_properties(start_ev):  # type: (Optional[dict]) -> dict
    if not start_ev:
        return {}
    return start_ev.get("modifiedConfigs", {})


# ---------------------------------------------------------------------------
# Execution summary
# ---------------------------------------------------------------------------

def _build_execution_summary(operators):  # type: (List[dict]) -> List[dict]
    """Build execution summary from resolved metrics."""
    summary = []
    for op in operators:
        metrics = op.get("metrics", [])
        if not metrics:
            continue
        m_dict = {m["metric"]: m["value"] for m in metrics}
        summary.append({
            "node": op["name"],
            "rows_output": m_dict.get("number of output rows", m_dict.get("rows output", "-")),
        })
    return summary


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def write_json(path: str, data: dict):
    out = {
        "query_metadata": data["query_metadata"],
        "operators": data["operators"],
        "stages": data["stages"],
        "physical_plan": data["physical_plan"],
    }
    if data.get("execution_summary"):
        out["execution_summary"] = data["execution_summary"]
    if data.get("sql_properties"):
        out["sql_properties"] = data["sql_properties"]
    if data.get("command_metrics"):
        out["command_metrics"] = data["command_metrics"]
    if data.get("scan_metrics"):
        out["scan_metrics"] = data["scan_metrics"]
    if data.get("table_paths"):
        out["table_paths"] = data["table_paths"]

    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"  JSON written: {path}")


# ---------------------------------------------------------------------------
# README generation
# ---------------------------------------------------------------------------

def write_readme(path: str, query_name: str, data: dict):
    meta = data["query_metadata"]
    operators = data["operators"]
    ops_with_metrics = sum(1 for op in operators if op.get("metrics"))

    metrics_list = "\n".join(
        f"  - **node {op['node_id']}** -- {op['name']} ({len(op['metrics'])} metrics)"
        for op in operators if op.get("metrics")
    )

    content = f"""# {query_name} -- Parsed from Event Log

## Source
- **Query**: {meta.get('title', query_name)}
- **Execution ID**: {meta.get('execution_id', 'unknown')}
- **Root Execution ID**: {meta.get('root_execution_id', 'unknown')}
- **Duration**: {meta.get('duration', 'unknown')}
- **Submitted**: {meta.get('submitted_time', 'unknown')}
- **Error**: {meta.get('error_message', 'none')}

## Data Source
Parsed from Spark event log files (not HTML). Uses the same JSON output format as
the HTML QPL parser for compatibility with the QPL analysis skill.

## Files

### `{query_name}.json` -- Full Query Plan with Metrics
The JSON contains the complete query plan structure with per-operator metrics inline:
- **`query_metadata`**: execution ID, timing, error info
- **`operators`**: {len(operators)} operators with topology and inline `metrics` arrays
- **`stages`**: stage groupings
- **`physical_plan`**: full physical plan text
- **`execution_summary`**: per-operator rows output summary
- **`sql_properties`**: Spark SQL configuration at execution time (from modifiedConfigs)
- **`command_metrics`**: write/merge/delete command summary (if applicable)
- **`scan_metrics`**: scan operator summaries
- **`table_paths`**: cloud storage paths referenced

## Operators with Metrics ({ops_with_metrics} of {len(operators)})
{metrics_list}

## How to Read the Metrics

### Metric format: "total (min, med, max)"
Most metrics follow this pattern: `5.6 s (381 ms, 634 ms, 761 ms)`
- **total**: sum across ALL tasks in the stage
- **min**: smallest value among individual tasks
- **med**: median value among individual tasks
- **max**: largest value among individual tasks

### Key metrics
- **number of output rows**: row count produced
- **duration total**: wall-clock time across all tasks
- **spill size total**: data spilled to disk (memory pressure)
- **shuffle bytes written**: shuffle data volume
- **fetch wait time total**: time waiting for shuffle reads

### Event log limitations
- `edge_rows_to_parent` is always null (DOT graph not available in event logs)
- Metric values may be zero if the query never reached `isFinalPlan=true`
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  README written: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    force = "--force" in sys.argv
    all_mode = "--all" in sys.argv
    query_ids = []  # type: List[int]
    user_start_time: Optional[int] = None
    user_end_time: Optional[int] = None

    args = [a for a in sys.argv[1:] if a not in ("--force", "--all")]

    i = 0
    paths_args = []
    while i < len(args):
        if args[i] == "--query" and i + 1 < len(args):
            try:
                query_ids.append(int(args[i + 1]))
            except ValueError:
                print(f"Error: --query expects an integer, got '{args[i + 1]}'")
                sys.exit(1)
            i += 2
        elif args[i] == "--start-time" and i + 1 < len(args):
            try:
                user_start_time = _parse_time_arg(args[i + 1])
            except ValueError as e:
                print(f"Error: {e}")
                sys.exit(1)
            i += 2
        elif args[i] == "--end-time" and i + 1 < len(args):
            try:
                user_end_time = _parse_time_arg(args[i + 1])
            except ValueError as e:
                print(f"Error: {e}")
                sys.exit(1)
            i += 2
        else:
            paths_args.append(args[i])
            i += 1

    if not paths_args:
        print("Usage: python3 parse_eventlog.py <eventlog_file_or_dir> --query <id> [--query <id2>] [--force]")
        print("       python3 parse_eventlog.py <eventlog_file_or_dir> --all [--force]")
        print()
        print("  --query <id>       Extract a specific execution/query ID (repeatable)")
        print("  --all              Extract all executions that have physical plans")
        print("  --force            Overwrite existing output")
        print("  --start-time <t>   Only process tasks after this time (epoch ms or ISO)")
        print("  --end-time <t>     Only process tasks before this time; enables early termination")
        print()
        print("  Time formats: epoch ms (1712345678000), ISO (2024-04-10T14:30:00),")
        print("                Spark UI (2024/04/10 14:30:00). All UTC.")
        print()
        print("  If --start-time / --end-time are omitted, the time window is auto-derived")
        print("  from the SQL execution start/end events with a 5-minute buffer.")
        sys.exit(1)

    if not query_ids and not all_mode:
        print("Error: specify --query <id> or --all")
        sys.exit(1)

    all_files = []
    input_base = None
    for arg in paths_args:
        p = Path(arg)
        if p.is_dir():
            input_base = p
            all_files.extend(find_eventlog_files(p))
        elif p.is_file():
            if input_base is None:
                input_base = p.parent
            all_files.append(p)
        else:
            print(f"Warning: '{arg}' not found, skipping")

    if not all_files:
        print("No event log files found.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Phase 1: Read all events EXCEPT TaskEnd (fast, low memory).
    # TaskEnd events are typically 90-95% of lines in large event logs.
    # When specific --query IDs are given, also filter SQL events by
    # executionId so we don't store data for unrelated queries.
    # ------------------------------------------------------------------
    phase1_substrings = _WANTED_EVENT_SUBSTRINGS - {"SparkListenerTaskEnd"}
    eid_filter = set(query_ids) if query_ids else None
    print(f"Found {len(all_files)} event log file(s):")
    filter_desc = f", executionId filter: {eid_filter}" if eid_filter else ""
    print(f"\nPhase 1: Reading events (skipping TaskEnd{filter_desc})...")
    events = read_event_log_files(
        all_files,
        wanted_substrings=phase1_substrings,
        target_execution_ids=eid_filter,
    )
    classified = classify_events(events)
    del events  # free memory

    print(f"\nEvent summary:")
    print(f"  SQL executions (start): {len(classified['sql_start'])}")
    print(f"  SQL executions (end):   {len(classified['sql_end'])}")
    print(f"  AQE plan updates:       {len(classified['aqe_update'])}")
    print(f"  Metric updates:         {len(classified['metric_update'])}")
    print(f"  Driver accum updates:   {len(classified['driver_accum'])}")
    print(f"  Jobs:                   {len(classified['job_start'])}")
    print(f"  Stages completed:       {len(classified['stage_completed'])}")
    print(f"  Task metrics:           (deferred to Phase 3 streaming)")

    all_execs = list_executions(classified) if (all_mode or not query_ids) else None

    if all_mode and all_execs is not None:
        query_ids = [e["executionId"] for e in all_execs if e["has_plan"]]
        print(f"\nFound {len(all_execs)} executions, {len(query_ids)} with plans")

    if not query_ids:
        print("\nNo executions to process.")
        if all_execs:
            print("\nAvailable executions:")
            for e in all_execs[:20]:
                status = "with plan" if e["has_plan"] else "no plan"
                err = f" ERROR: {e['error'][:60]}..." if e.get("error") else ""
                print(f"  Query {e['executionId']} ({status}){err}")
            if len(all_execs) > 20:
                print(f"  ... and {len(all_execs) - 20} more")
        sys.exit(0)

    output_base = input_base or Path(".")

    # Filter out queries whose output already exists (unless --force).
    to_extract = []  # type: List[int]
    skipped = []     # type: List[str]
    for eid in query_ids:
        query_name = f"Query_{eid}"
        out_dir = output_base / "parsed_output" / query_name
        if out_dir.exists() and not force:
            print(f"\n  Skipping {query_name} (already exists, use --force to overwrite)")
            skipped.append(query_name)
        else:
            to_extract.append(eid)

    if not to_extract:
        print(f"\nAll queries already extracted. Use --force to re-extract.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Phase 2: Build execution -> stage mapping, collect accum IDs,
    #          and derive time window.
    #
    # Stage mapping uses the canonical Spark mechanism:
    #   JobStart.Properties["spark.sql.execution.id"] -> Stage IDs
    # (same as SQLAppStatusListener.onJobStart in the runtime).
    #
    # Accum IDs come from a lightweight walk of each plan tree.
    #
    # Time window is auto-derived from SQLExecutionStart/End times
    # (with a buffer) unless the user provided --start-time/--end-time.
    # ------------------------------------------------------------------
    print(f"\nPhase 2: Building execution->stage map, accum IDs, and time window...")
    exec_stage_map = _build_execution_stage_map(classified)

    all_accum_ids = set()    # type: Set[int]
    all_stage_ids = set()    # type: Set[int]
    valid_eids = []          # type: List[int]
    auto_start_times = []    # type: List[int]
    auto_end_times = []      # type: List[int]

    start_map = {ev["executionId"]: ev for ev in classified["sql_start"]}
    end_map = {ev["executionId"]: ev for ev in classified["sql_end"]}

    for eid in to_extract:
        aids = _get_accum_ids_for_execution(eid, classified)
        if aids:
            all_accum_ids.update(aids)
            stages = exec_stage_map.get(eid, set())
            all_stage_ids.update(stages)
            valid_eids.append(eid)
            s_ev = start_map.get(eid)
            e_ev = end_map.get(eid)
            if s_ev and s_ev.get("time"):
                auto_start_times.append(s_ev["time"])
            if e_ev and e_ev.get("time"):
                auto_end_times.append(e_ev["time"])
        else:
            print(f"  WARNING: No plan found for execution {eid}")

    print(f"  Target accumulator IDs: {len(all_accum_ids):,}")
    print(f"  Target stage IDs:       {len(all_stage_ids):,} (from JobStart properties)")

    # Resolve time window: user override > auto-derived > None.
    time_window = None  # type: Optional[Tuple[int, int]]
    if user_start_time is not None or user_end_time is not None:
        tw_start = user_start_time or 0
        tw_end = user_end_time or (2**63 - 1)
        time_window = (tw_start, tw_end)
        print(f"  Time window (user):     [{tw_start}, {tw_end}]")
    elif auto_start_times and auto_end_times:
        tw_start = min(auto_start_times) - _TIME_WINDOW_BUFFER_MS
        tw_end = max(auto_end_times) + _TIME_WINDOW_BUFFER_MS
        time_window = (tw_start, tw_end)
        from datetime import datetime, timezone
        t0 = datetime.fromtimestamp(tw_start / 1000, tz=timezone.utc).strftime("%H:%M:%S")
        t1 = datetime.fromtimestamp(tw_end / 1000, tz=timezone.utc).strftime("%H:%M:%S")
        print(f"  Time window (auto):     [{t0} .. {t1}] UTC (+/-5 min buffer)")
    else:
        print(f"  Time window:            none (execution times not available)")

    # ------------------------------------------------------------------
    # Phase 3: Single streaming pass through TaskEnd events.
    # Three-level filter: stage ID -> time window -> accumulator ID.
    # Early termination when events pass the end of the time window.
    # ------------------------------------------------------------------
    print(f"\nPhase 3: Streaming task accumulators...")
    task_accum_values = stream_task_accum_values(
        all_files,
        all_accum_ids,
        target_stage_ids=all_stage_ids if all_stage_ids else None,
        time_window_ms=time_window,
    )

    # ------------------------------------------------------------------
    # Phase 4: Build full output for each query.
    # ------------------------------------------------------------------
    parsed = []  # type: List[str]

    for eid in valid_eids:
        query_name = f"Query_{eid}"
        out_dir = output_base / "parsed_output" / query_name

        print(f"\n{'='*60}")
        print(f"Extracting execution {eid}...")

        result = extract_execution(eid, classified, precomputed_task_accums=task_accum_values)
        if result is None:
            print(f"  FAILED: Could not extract execution {eid}")
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = str(out_dir / f"{query_name}.json")
        readme_path = str(out_dir / "README.md")

        write_json(json_path, result)
        write_readme(readme_path, query_name, result)

        ops_count = len(result["operators"])
        metrics_count = sum(1 for op in result["operators"] if op.get("metrics"))
        print(f"  Done: {query_name} ({ops_count} operators, {metrics_count} with metrics)")
        parsed.append(query_name)

    print(f"\n{'='*60}")
    print(f"Summary: {len(parsed)} parsed, {len(skipped)} skipped")
    if parsed:
        print(f"  Parsed: {', '.join(parsed)}")
    if skipped:
        print(f"  Skipped: {', '.join(skipped)}")
    print(f"  Output: {output_base.resolve()}/parsed_output/")


if __name__ == "__main__":
    main()
