---
name: spark-tune
description: Analyze Spark execution logs, extract query plans, and generate a performance tuning report. Use when debugging slow Spark jobs, reviewing shuffle/spill metrics, diagnosing data skew, optimizing join strategies, or producing a tuning recommendations document from Spark event logs or EXPLAIN output. Accepts job ID, run ID, event log path, or 'current' as input.
argument-hint: "--job-id <id> --run-id <id> [--profile <name>] [--catalog <name>] [--focus <areas>]"
disable-model-invocation: true
---

# /spark-tune - Spark Plan & Log Performance Tuner

Extracts execution plans, stage metrics, and task-level statistics from Spark logs or live SparkSessions, then produces a structured **Performance Tuning Report** (Markdown) with prioritized, actionable recommendations.

## Inputs

The skill accepts the following inputs. At least one of `--job-id`, `--run-id`, or a positional path must be provided.

| Input | Flag | Required | Description |
|-------|------|----------|-------------|
| **Job ID** | `--job-id <id>` | No | Databricks job ID. If provided without `--run-id`, analyzes the **most recent completed run** of this job. |
| **Run ID** | `--run-id <id>` | No | Databricks job run ID. Analyzes this specific run. The `job_id` is **auto-resolved** from the run details API (`GET /api/2.1/jobs/runs/get` returns `job_id`), so `--job-id` is not required when `--run-id` is provided. |
| **Event Log Path** | positional | No | Local or DBFS path to a Spark event log file (JSON lines). |
| **Profile** | `--profile <name>` | No | Databricks CLI profile name (default: `DEFAULT`). Used to authenticate REST API calls. |
| **Catalog** | `--catalog <name>` | No | Unity Catalog name. Used to locate cluster log volumes at `/Volumes/<catalog>/<schema>/cluster_logs/`. |
| **Schema** | `--schema <name>` | No | Schema within the catalog for log volume lookup (default: inferred from job tags or `default`). |
| **Focus** | `--focus <areas>` | No | Comma-separated analysis focus areas: `joins`, `shuffles`, `spill`, `skew`, `scans`, `writes`, `all` (default: `all`). |
| **Current** | `current` | No | Analyze the active SparkSession in the current notebook. |

### Input Resolution Order

When multiple inputs are provided, they are resolved in this order:

```
1. --run-id         → Fetch run details (which includes job_id), extract cluster ID, download logs
2. --job-id (alone) → Fetch latest completed run for this job, then proceed as (1)
3. positional path  → Parse event log file directly (local or DBFS)
4. current          → Extract plans from the live SparkSession

Note: --run-id auto-resolves job_id from the API response, so --job-id is optional when --run-id is given.
```

### Input Parsing Logic

```python
def parse_inputs(args: str) -> dict:
    """Parse skill invocation arguments into structured inputs."""
    import re

    inputs = {
        "job_id": None,
        "run_id": None,
        "event_log_path": None,
        "profile": "DEFAULT",
        "catalog": None,
        "schema": None,
        "focus": "all",
        "mode": None,  # "job", "run", "file", "current"
    }

    # Extract named flags
    job_match = re.search(r'--job-id\s+(\d+)', args)
    run_match = re.search(r'--run-id\s+(\d+)', args)
    profile_match = re.search(r'--profile\s+(\S+)', args)
    catalog_match = re.search(r'--catalog\s+(\S+)', args)
    schema_match = re.search(r'--schema\s+(\S+)', args)
    focus_match = re.search(r'--focus\s+(\S+)', args)

    if job_match:
        inputs["job_id"] = int(job_match.group(1))
        inputs["mode"] = "job"
    if run_match:
        inputs["run_id"] = int(run_match.group(1))
        inputs["mode"] = "run"
    if profile_match:
        inputs["profile"] = profile_match.group(1)
    if catalog_match:
        inputs["catalog"] = catalog_match.group(1)
    if schema_match:
        inputs["schema"] = schema_match.group(1)
    if focus_match:
        inputs["focus"] = focus_match.group(1)

    # Check for positional path or "current"
    if "current" in args.lower() and not inputs["mode"]:
        inputs["mode"] = "current"
    elif not inputs["mode"]:
        # Remaining non-flag token is treated as event log path
        remaining = re.sub(r'--\S+\s+\S+', '', args).strip()
        if remaining:
            inputs["event_log_path"] = remaining
            inputs["mode"] = "file"

    # If job_id provided without run_id, resolve to latest run
    if inputs["mode"] == "job" and not inputs["run_id"]:
        inputs["resolve_latest_run"] = True

    return inputs
```

## Usage

```
/spark-tune --job-id 583122712958557
/spark-tune --job-id 583122712958557 --run-id 282634152109589
/spark-tune --run-id 282634152109589 --profile myworkspace
/spark-tune --run-id 282634152109589 --catalog kishoremannava --schema tower_kpis
/spark-tune /dbfs/cluster-logs/app-20260422/eventlog
/spark-tune current
/spark-tune --job-id 583122712958557 --focus shuffles,skew
```

---

## Workflow

### 1. Resolve Inputs and Locate Logs

Parse the user's input using the Input Resolution Order above, then locate the Spark execution logs.

#### Mode: `--job-id` (resolve latest run)

```bash
# 1a. Get the latest completed run for this job
TOKEN=$(databricks auth token -p <profile> | jq -r '.access_token')
HOST=$(grep "host" ~/.databrickscfg | head -1 | awk '{print $3}')

# List runs, filter for SUCCESS, take the most recent
curl -s "$HOST/api/2.1/jobs/runs/list?job_id=<job_id>&limit=5" \
  -H "Authorization: Bearer $TOKEN" \
| jq '[.runs[] | select(.state.result_state == "SUCCESS")] | sort_by(.end_time) | last'
# Extract run_id from the result, then proceed as --run-id mode
```

#### Mode: `--run-id` (fetch run details and download logs)

```bash
# 1b. Get run details — extract cluster ID, task run IDs, timing
RUN_DETAILS=$(curl -s "$HOST/api/2.1/jobs/runs/get?run_id=<run_id>" \
  -H "Authorization: Bearer $TOKEN")

# Extract key fields — job_id is always returned in run details, even if not provided as input
JOB_ID=$(echo "$RUN_DETAILS" | jq -r '.job_id')
CLUSTER_ID=$(echo "$RUN_DETAILS" | jq -r '.tasks[0].cluster_instance.cluster_id')
TASK_RUN_ID=$(echo "$RUN_DETAILS" | jq -r '.tasks[0].run_id')
EXEC_DURATION=$(echo "$RUN_DETAILS" | jq -r '.execution_duration')
SETUP_DURATION=$(echo "$RUN_DETAILS" | jq -r '.setup_duration')
# JOB_ID is needed in Steps 8-9 to locate the source notebook and redeploy

# 1c. Locate logs in cluster log volume
# If --catalog provided, look in /Volumes/<catalog>/<schema>/cluster_logs/<cluster_id>/
# Otherwise, check the cluster's log_conf for the configured destination
CLUSTER_INFO=$(curl -s "$HOST/api/2.0/clusters/get?cluster_id=$CLUSTER_ID" \
  -H "Authorization: Bearer $TOKEN")
LOG_DEST=$(echo "$CLUSTER_INFO" | jq -r '.cluster_log_conf.volumes.destination // empty')

# 1d. Download logs from volume via Files API
LOG_BASE="$LOG_DEST/$CLUSTER_ID"
LOGDIR="/tmp/spark_logs/$CLUSTER_ID"
mkdir -p "$LOGDIR"

# stdout (print output)
curl -s "$HOST/api/2.0/fs/files${LOG_BASE}/driver/stdout" \
  -H "Authorization: Bearer $TOKEN" -o "$LOGDIR/stdout"

# stderr (JVM logs)
curl -s "$HOST/api/2.0/fs/files${LOG_BASE}/driver/stderr" \
  -H "Authorization: Bearer $TOKEN" -o "$LOGDIR/stderr"

# Event log (JSON lines — main source for metrics)
EVTDIR=$(curl -s "$HOST/api/2.0/fs/directories${LOG_BASE}/eventlog/" \
  -H "Authorization: Bearer $TOKEN" | jq -r '.contents[0].name')
EVTSUBDIR=$(curl -s "$HOST/api/2.0/fs/directories${LOG_BASE}/eventlog/$EVTDIR/" \
  -H "Authorization: Bearer $TOKEN" | jq -r '.contents[0].name')
curl -s "$HOST/api/2.0/fs/files${LOG_BASE}/eventlog/$EVTDIR/$EVTSUBDIR/eventlog" \
  -H "Authorization: Bearer $TOKEN" -o "$LOGDIR/eventlog"
```

#### Mode: Event log file path (local or DBFS)

```bash
# 1e. If DBFS path, download via Files API
# If local path, use directly
LOGDIR=$(dirname "<event_log_path>")
```

#### Mode: `current` (live SparkSession)

```python
# 1f. No log download needed — extract plans directly from active session
# Proceed to Step 2 using DataFrame .explain() methods
```

**After this step, you should have:** `LOGDIR` containing `eventlog`, `stdout`, and `stderr` files, plus `CLUSTER_ID`, `EXEC_DURATION`, and `SETUP_DURATION` metadata.

### 2. Extract All Execution Plans

For every SQL execution / DataFrame action found in the log, capture **all four plan phases**:

```python
# --- Plan extraction helper ---
def extract_plans(df_or_sql, spark):
    """Capture all plan phases for a query."""
    plans = {}

    # Method A: DataFrame
    if hasattr(df_or_sql, 'explain'):
        plans["simple"]    = df_or_sql._jdf.queryExecution().simpleString()
        plans["parsed"]    = df_or_sql._jdf.queryExecution().logical().toString()
        plans["analyzed"]  = df_or_sql._jdf.queryExecution().analyzed().toString()
        plans["optimized"] = df_or_sql._jdf.queryExecution().optimizedPlan().toString()
        plans["physical"]  = df_or_sql._jdf.queryExecution().executedPlan().toString()

    # Method B: SQL string
    else:
        explain_df = spark.sql(f"EXPLAIN EXTENDED {df_or_sql}")
        plans["extended"] = explain_df.collect()[0][0]

    return plans
```

For **event log files**, parse JSON lines and collect:
```python
import json

def parse_event_log(log_path):
    """Extract execution metadata from Spark event log."""
    jobs, stages, sql_executions, tasks = [], [], [], []

    with open(log_path) as f:
        for line in f:
            event = json.loads(line)
            etype = event.get("Event", "")

            if etype == "SparkListenerJobStart":
                jobs.append({
                    "job_id":    event["Job ID"],
                    "stage_ids": [s["Stage ID"] for s in event.get("Stage Infos", [])],
                    "timestamp": event.get("Submission Time"),
                    "properties": event.get("Properties", {}),
                })

            elif etype == "SparkListenerStageCompleted":
                info = event["Stage Info"]
                metrics = info.get("Accumulables", [])
                stages.append({
                    "stage_id":        info["Stage ID"],
                    "stage_name":      info.get("Stage Name", ""),
                    "num_tasks":       info["Number of Tasks"],
                    "submission_time": info.get("Submission Time"),
                    "completion_time": info.get("Completion Time"),
                    "metrics":         {m["Name"]: m["Value"] for m in metrics},
                })

            elif etype == "SparkListenerSQLExecutionStart":
                sql_executions.append({
                    "execution_id": event["executionId"],
                    "description":  event.get("description", ""),
                    "details":      event.get("details", ""),
                    "physical_plan": event.get("physicalPlanDescription", ""),
                })

            elif etype == "SparkListenerTaskEnd":
                tm = event.get("Task Metrics", {})
                tasks.append({
                    "stage_id":          event["Stage ID"],
                    "task_id":           event["Task Info"]["Task ID"],
                    "duration_ms":       event["Task Info"].get("Finish Time", 0)
                                         - event["Task Info"].get("Launch Time", 0),
                    "bytes_read":        tm.get("Input Metrics", {}).get("Bytes Read", 0),
                    "shuffle_write":     tm.get("Shuffle Write Metrics", {}).get("Bytes Written", 0),
                    "shuffle_read":      tm.get("Shuffle Read Metrics", {}).get("Total Bytes Read", 0),
                    "memory_spill":      tm.get("Memory Bytes Spilled", 0),
                    "disk_spill":        tm.get("Disk Bytes Spilled", 0),
                    "records_read":      tm.get("Input Metrics", {}).get("Records Read", 0),
                    "records_written":   tm.get("Output Metrics", {}).get("Records Written", 0),
                })

    return {"jobs": jobs, "stages": stages, "sql_executions": sql_executions, "tasks": tasks}
```

### 3. Analyze Plans for Anti-Patterns

Scan every physical plan and stage metric for these **10 anti-patterns**, ordered by typical severity:

#### 3a. Critical Severity

| # | Anti-Pattern | Detection Rule | Plan/Log Signal |
|---|-------------|---------------|-----------------|
| 1 | **Cartesian Join** | `CartesianProduct` in physical plan | Explosive row count; N x M output |
| 2 | **Full Table Scan on Large Table** | `Scan` with no `PushedFilters` + `bytesRead > 1GB` | Missing partition pruning or predicate pushdown |
| 3 | **Severe Data Skew** | Task duration max/median ratio > 5x | One task processes 10x+ more data than peers |

#### 3b. High Severity

| # | Anti-Pattern | Detection Rule | Plan/Log Signal |
|---|-------------|---------------|-----------------|
| 4 | **Disk Spill** | `diskBytesSpilled > 0` | Executor memory exhausted; shuffle buffers overflow to disk |
| 5 | **Wrong Join Strategy** | `SortMergeJoin` when one side < 1GB | Should be `BroadcastHashJoin`; missing AQE or hint |
| 6 | **Excessive Shuffles** | More than 2 `Exchange` nodes in a single query plan | Redundant repartitions or groupBy chains |

#### 3c. Medium Severity

| # | Anti-Pattern | Detection Rule | Plan/Log Signal |
|---|-------------|---------------|-----------------|
| 7 | **Missing Predicate Pushdown** | `Filter` appears above `Scan` in plan (not inside `PushedFilters`) | Data source reads unnecessary rows/files |
| 8 | **Small File Problem** | `numFiles > 1000` with avg file size < 32MB | Metadata overhead; slow listing |
| 9 | **Partition Count Mismatch** | `spark.sql.shuffle.partitions` >> or << data size | 200 default for 100MB data = too many; for 500GB = too few |
| 10 | **Collect/toPandas on Large Data** | `collect()` or `toPandas()` on > 1M rows | Driver OOM risk |

**Detection code:**
```python
def detect_anti_patterns(physical_plan: str, stage_metrics: dict, task_metrics: list):
    """Scan plan text and metrics for known anti-patterns."""
    findings = []

    # --- Plan-based detection ---
    plan_upper = physical_plan.upper()

    if "CARTESIANPRODUCT" in plan_upper:
        findings.append(("CRITICAL", "Cartesian Join",
            "CartesianProduct detected — verify join condition exists"))

    # Count Exchange nodes (shuffles)
    exchange_count = plan_upper.count("EXCHANGE")
    if exchange_count > 2:
        findings.append(("HIGH", "Excessive Shuffles",
            f"{exchange_count} Exchange operations in single query"))

    # SortMergeJoin that could be Broadcast
    if "SORTMERGEJOIN" in plan_upper and "BROADCASTHASHJOIN" not in plan_upper:
        findings.append(("HIGH", "Potential Wrong Join Strategy",
            "SortMergeJoin used — check if one side is small enough for broadcast"))

    # Missing predicate pushdown
    if "FILTER" in plan_upper and "PUSHEDFILTERS: []" in plan_upper:
        findings.append(("MEDIUM", "Missing Predicate Pushdown",
            "Filter not pushed to data source — check column types and expressions"))

    # --- Metric-based detection ---
    if task_metrics:
        durations   = [t["duration_ms"] for t in task_metrics if t["duration_ms"] > 0]
        spill_mem   = sum(t["memory_spill"] for t in task_metrics)
        spill_disk  = sum(t["disk_spill"] for t in task_metrics)
        total_read  = sum(t["bytes_read"] for t in task_metrics)

        # Data skew
        if durations:
            import statistics
            median_d = statistics.median(durations)
            max_d    = max(durations)
            if median_d > 0 and max_d / median_d > 5:
                findings.append(("CRITICAL", "Data Skew",
                    f"Max task duration {max_d}ms is {max_d/median_d:.1f}x the median {median_d:.0f}ms"))

        # Spill
        if spill_disk > 0:
            findings.append(("HIGH", "Disk Spill",
                f"Total disk spill: {spill_disk / (1024**2):.1f} MB — "
                f"memory spill: {spill_mem / (1024**2):.1f} MB"))

        # Large full scan
        if total_read > 1_073_741_824:  # > 1 GB
            findings.append(("MEDIUM", "Large Data Scan",
                f"Total bytes read: {total_read / (1024**3):.2f} GB — verify partition pruning"))

    return sorted(findings, key=lambda x: {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}.get(x[0], 3))
```

### 4. Compute Tuning Metrics

For each job/stage, compute these summary metrics:

```python
def compute_tuning_metrics(stages, tasks, spark_conf):
    """Compute aggregate tuning metrics across all stages."""
    metrics = {}

    # -- Shuffle efficiency --
    total_shuffle_write = sum(t["shuffle_write"] for t in tasks)
    total_shuffle_read  = sum(t["shuffle_read"] for t in tasks)
    total_input         = sum(t["bytes_read"] for t in tasks)
    metrics["shuffle_ratio"] = (
        total_shuffle_write / total_input if total_input > 0 else 0
    )
    metrics["total_shuffle_gb"] = total_shuffle_write / (1024**3)

    # -- Spill ratio --
    total_spill = sum(t["disk_spill"] for t in tasks)
    metrics["spill_ratio"] = total_spill / total_input if total_input > 0 else 0
    metrics["total_spill_gb"] = total_spill / (1024**3)

    # -- Task skew factor (per stage) --
    from collections import defaultdict
    import statistics
    stage_tasks = defaultdict(list)
    for t in tasks:
        stage_tasks[t["stage_id"]].append(t["duration_ms"])

    skew_factors = {}
    for sid, durations in stage_tasks.items():
        if len(durations) > 1 and statistics.median(durations) > 0:
            skew_factors[sid] = max(durations) / statistics.median(durations)
    metrics["max_skew_factor"] = max(skew_factors.values()) if skew_factors else 1.0
    metrics["skew_by_stage"] = skew_factors

    # -- Partition sizing --
    shuffle_partitions = int(spark_conf.get("spark.sql.shuffle.partitions", "200"))
    target_partition_mb = 128  # ideal partition size
    if total_shuffle_write > 0:
        actual_partition_mb = (total_shuffle_write / shuffle_partitions) / (1024**2)
        ideal_partitions = max(1, int(total_shuffle_write / (target_partition_mb * 1024**2)))
        metrics["current_shuffle_partitions"] = shuffle_partitions
        metrics["actual_partition_size_mb"] = round(actual_partition_mb, 1)
        metrics["recommended_shuffle_partitions"] = ideal_partitions

    # -- Stage duration breakdown --
    stage_durations = {}
    for s in stages:
        if s.get("completion_time") and s.get("submission_time"):
            stage_durations[s["stage_id"]] = s["completion_time"] - s["submission_time"]
    metrics["stage_durations_ms"] = stage_durations
    if stage_durations:
        metrics["slowest_stage"] = max(stage_durations, key=stage_durations.get)
        metrics["total_wall_time_s"] = sum(stage_durations.values()) / 1000

    return metrics
```

### 5. Generate Spark Configuration Recommendations

Based on detected anti-patterns and computed metrics, generate specific config changes:

```python
RECOMMENDATION_RULES = {
    "Data Skew": {
        "configs": [
            ("spark.sql.adaptive.enabled", "true", "Enable AQE for runtime skew handling"),
            ("spark.sql.adaptive.skewJoin.enabled", "true", "Auto-split skewed partitions"),
            ("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb",
             "Threshold to detect skewed partitions"),
        ],
        "code_fix": "Consider salting the skewed key:\n"
                     "  df = df.withColumn('salt', F.concat(F.col('key'), F.lit('_'), (F.rand()*N).cast('int')))\n"
                     "  # Join on salted key, then drop salt column",
    },
    "Disk Spill": {
        "configs": [
            ("spark.sql.adaptive.enabled", "true", "AQE auto-coalesces partitions"),
            ("spark.sql.adaptive.coalescePartitions.enabled", "true", "Merge small partitions"),
            ("spark.executor.memory", "<increase by 2x>", "More memory for shuffle buffers"),
            ("spark.memory.fraction", "0.8", "Increase memory fraction for execution (default 0.6)"),
        ],
        "code_fix": "Repartition before heavy aggregations to right-size partitions:\n"
                     "  df = df.repartition(optimal_partitions, 'key_col')",
    },
    "Potential Wrong Join Strategy": {
        "configs": [
            ("spark.sql.autoBroadcastJoinThreshold", "100mb",
             "Increase broadcast threshold if small side < 100MB"),
            ("spark.sql.adaptive.enabled", "true",
             "AQE converts SortMergeJoin to BroadcastHashJoin at runtime"),
        ],
        "code_fix": "Use broadcast hint for known small tables:\n"
                     "  from pyspark.sql.functions import broadcast\n"
                     "  result = big_df.join(broadcast(small_df), 'key')",
    },
    "Excessive Shuffles": {
        "configs": [
            ("spark.sql.adaptive.enabled", "true", "AQE coalesces redundant shuffles"),
        ],
        "code_fix": "Consolidate multiple groupBy/join operations:\n"
                     "  # Instead of: df.groupBy('a').agg(...).join(df.groupBy('b').agg(...))\n"
                     "  # Use: df.groupBy('a','b').agg(...) if possible\n"
                     "  # Or cache intermediate results: df.cache()",
    },
    "Missing Predicate Pushdown": {
        "configs": [
            ("spark.sql.parquet.filterPushdown", "true", "Enable Parquet pushdown (default true)"),
            ("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true", "Enable DPP"),
        ],
        "code_fix": "Move filters before joins and use partition columns:\n"
                     "  # Push filters early in the chain\n"
                     "  df = df.filter(F.col('date') >= '2026-01-01').join(...)",
    },
    "Cartesian Join": {
        "configs": [],
        "code_fix": "Add an explicit join condition:\n"
                     "  # BAD:  df1.crossJoin(df2)\n"
                     "  # GOOD: df1.join(df2, df1.key == df2.key)",
    },
    "Small File Problem": {
        "configs": [
            ("spark.sql.shuffle.partitions", "<reduce to match data size>",
             "Fewer partitions = fewer output files"),
            ("spark.databricks.delta.optimizeWrite.enabled", "true",
             "Databricks auto-optimizes write file sizes"),
            ("spark.databricks.delta.autoCompact.enabled", "true",
             "Auto-compact small files after writes"),
        ],
        "code_fix": "Coalesce before writing:\n"
                     "  df.coalesce(target_files).write.parquet(...)\n"
                     "  # Or run OPTIMIZE on Delta tables:\n"
                     "  spark.sql('OPTIMIZE schema.table')",
    },
}
```

### 6. Generate the Performance Tuning Report

Assemble all findings into a structured Markdown document:

```
## Spark Performance Tuning Report
**Job**: <job name or ID>
**Cluster**: <cluster ID / type>
**Date**: <execution date>
**Total Wall Time**: <X seconds>

---

### Executive Summary
- **Overall Health**: [GOOD | NEEDS ATTENTION | CRITICAL]
- **Anti-Patterns Found**: X (Y critical, Z high, W medium)
- **Estimated Improvement**: <potential speedup range>

---

### Job Topology
| Job ID | Stages | Tasks | Duration | Shuffle Write | Spill |
|--------|--------|-------|----------|--------------|-------|
| ...    | ...    | ...   | ...      | ...          | ...   |

---

### Plan Analysis

#### Query 1: <description>
**Physical Plan:**
```
<physical plan text>
```

**Findings:**
| Severity | Issue | Details | Recommendation |
|----------|-------|---------|----------------|
| CRITICAL | ...   | ...     | ...            |

---

### Tuning Metrics Dashboard

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Shuffle Ratio (shuffle/input) | X% | < 30% | ... |
| Spill Ratio (spill/input) | X% | 0% | ... |
| Max Skew Factor | Xx | < 3x | ... |
| Partition Size (avg) | X MB | 128 MB | ... |
| Shuffle Partitions | X | Y (recommended) | ... |

---

### Configuration Recommendations

| Priority | Config Key | Current | Recommended | Reason |
|----------|-----------|---------|-------------|--------|
| 1        | ...       | ...     | ...         | ...    |

---

### Code-Level Fixes

#### Fix 1: <title>
**Severity**: CRITICAL
**Location**: <file:line or query>
**Before:**
```python
<current code>
```
**After:**
```python
<recommended code>
```
**Expected Impact**: <description>

---

### Databricks-Specific Optimizations

| Feature | Status | Recommendation |
|---------|--------|----------------|
| Adaptive Query Execution (AQE) | enabled/disabled | ... |
| Photon Engine | enabled/disabled | ... |
| Dynamic Partition Pruning | enabled/disabled | ... |
| Delta Auto-Optimize Write | enabled/disabled | ... |
| Delta Auto-Compact | enabled/disabled | ... |
| Predictive I/O | enabled/disabled | ... |

---

### Next Steps
1. [ ] Apply critical configuration changes
2. [ ] Refactor code-level fixes
3. [ ] Re-run job and compare metrics
4. [ ] Review Spark UI for validation
```

### 7. Write Report to File

Save the generated report as `<job-name>_tuning_report.md` in the working directory, or to a specified output path. If a Databricks catalog is specified, also persist metrics to a Delta table for historical tracking:

```python
# Optional: persist to Delta for trend analysis
tuning_df.write.mode("append").saveAsTable(f"{catalog}.{schema}.spark_tuning_history")
```

### 8. Prompt: Apply Tuning Code Changes

After presenting the report, **ask the user**:

> **Would you like to apply the tuning code changes?**
>
> The following fixes were identified:
> 1. `[Fix title]` — `[severity]` — `[expected impact]`
> 2. `[Fix title]` — `[severity]` — `[expected impact]`
> ...
>
> Options:
> - **all** — Apply all recommended code fixes
> - **1,2,...** — Apply specific fixes by number
> - **none** — Skip code changes

If the user selects fixes to apply:

1. **Read the target source file** (notebook or `.py` file) that was analyzed
2. For each selected fix, apply the code change using the Edit tool:
   - Show the exact before/after diff for each change
   - Preserve all existing functionality
   - Add a comment `# TUNING: <fix description>` above each change
3. **Re-upload** the modified file to the Databricks workspace if it was sourced from there
4. Present a summary of all changes made with file paths and line numbers

**Code change patterns to apply automatically:**

| Fix Type | Action |
|----------|--------|
| Cache intermediate results | Add `.cache()` + `.count()` after DataFrame creation |
| Replace countDistinct with approx | Swap `F.countDistinct(col)` with `F.approx_count_distinct(col, rsd=0.05)` |
| GROUPING SETS refactor | Replace 3 separate groupBy queries with single SQL GROUPING SETS query |
| Consolidate drill-down queries | Replace per-country loop with single filtered display |
| Add broadcast hint | Wrap small table with `F.broadcast()` in join |
| Reduce shuffle partitions | Add `spark.conf.set("spark.sql.shuffle.partitions", N)` |
| Enable missing configs | Add `spark.conf.set(key, value)` for each missing optimization |

### 9. Prompt: Deploy and Re-Evaluate

After applying code changes (or if the user skips Step 8 but wants config-only changes), **ask the user**:

> **Would you like to deploy the updated job and re-evaluate performance?**
>
> This will:
> 1. Upload the modified notebook/script to Databricks workspace
> 2. Update the job definition with any new Spark configs
> 3. Trigger a new job run
> 4. Wait for completion
> 5. Pull logs from the cluster log volume
> 6. Re-run `/spark-tune` and generate a comparison report
>
> Options:
> - **yes** — Deploy, run, and re-evaluate
> - **deploy-only** — Deploy changes but don't run yet
> - **no** — Skip deployment

If the user selects **yes**:

1. **Upload** the modified source file to the Databricks workspace:
   ```bash
   databricks workspace import <remote-path> --file <local-path> --format SOURCE --language PYTHON --overwrite
   ```

2. **Update job definition** if Spark configs changed:
   ```bash
   curl -X POST "$HOST/api/2.1/jobs/reset" -d '{
     "job_id": <job_id>,
     "new_settings": { ...updated_settings... }
   }'
   ```

3. **Trigger the job run**:
   ```bash
   curl -X POST "$HOST/api/2.1/jobs/run-now" -d '{"job_id": <job_id>}'
   ```

4. **Poll for completion** (check every 10 seconds):
   ```bash
   curl "$HOST/api/2.1/jobs/runs/get?run_id=<run_id>"
   ```

5. **Download new logs** from the cluster log volume:
   - Event log, stdout, stderr from `/Volumes/<catalog>/<schema>/<volume>/<cluster-id>/`

6. **Re-run the full analysis** (Steps 2-7) on the new logs

7. **Generate a comparison report** showing before vs. after:

   ```
   ## Performance Comparison: Before vs. After Tuning

   | Metric | Before | After | Change |
   |--------|--------|-------|--------|
   | Total Execution Time | Xs | Ys | -Z% |
   | Shuffle Volume | X KB | Y KB | -Z% |
   | Disk Spill | X MB | Y MB | -Z% |
   | Max Skew Factor | Xx | Yx | -Z% |
   | SQL Executions | N | M | -K |
   | Anti-Patterns | N | M | -K |
   | Health Score | X/100 | Y/100 | +Z |
   ```

8. **Persist comparison** to the tuning history Delta table if configured

If the user selects **deploy-only**:
- Execute steps 1-2 only
- Print the job URL for manual triggering
- Remind the user to run `/spark-tune <new-run-id>` after the job completes

---

## Anti-Pattern Reference

### Severity Definitions

| Level | Meaning | Impact |
|-------|---------|--------|
| **CRITICAL** | Job will fail or run 10x+ slower than optimal | Immediate action required |
| **HIGH** | Significant waste — 2-5x slower or excessive resource use | Fix before next scheduled run |
| **MEDIUM** | Suboptimal but functional — 10-50% improvement possible | Address in next tuning cycle |

### Complete Anti-Pattern Catalog

<details>
<summary><strong>1. Cartesian Join</strong> (CRITICAL)</summary>

**What**: Two tables joined without a condition, producing N x M rows.
**Plan signal**: `CartesianProduct` node in physical plan.
**Metric signal**: Output row count >> input row count.
**Fix**: Add explicit join condition. If intentional, use `.crossJoin()` explicitly.
</details>

<details>
<summary><strong>2. Full Table Scan</strong> (CRITICAL)</summary>

**What**: Reading entire dataset without partition pruning or predicate pushdown.
**Plan signal**: `Scan` with `PushedFilters: []` and large `bytesRead`.
**Metric signal**: `bytesRead` >> expected data for the query.
**Fix**: Filter on partition columns first. Use `EXPLAIN` to verify pushdown.
</details>

<details>
<summary><strong>3. Severe Data Skew</strong> (CRITICAL)</summary>

**What**: One partition holds disproportionate data, creating a straggler task.
**Plan signal**: Not visible in plan; detected via task metrics.
**Metric signal**: `max(task_duration) / median(task_duration) > 5`.
**Fix**: Enable AQE skew join handling. Salt keys for extreme cases.
</details>

<details>
<summary><strong>4. Disk Spill</strong> (HIGH)</summary>

**What**: Executor runs out of memory during shuffle, writes to disk.
**Plan signal**: Not visible in plan; detected in task metrics.
**Metric signal**: `diskBytesSpilled > 0`.
**Fix**: Increase executor memory, increase partitions, or enable AQE coalesce.
</details>

<details>
<summary><strong>5. Wrong Join Strategy</strong> (HIGH)</summary>

**What**: SortMergeJoin used when BroadcastHashJoin would be faster.
**Plan signal**: `SortMergeJoin` with one small input.
**Metric signal**: Small shuffle read on one join side (< 100MB).
**Fix**: Increase `autoBroadcastJoinThreshold` or use `broadcast()` hint.
</details>

<details>
<summary><strong>6. Excessive Shuffles</strong> (HIGH)</summary>

**What**: Multiple unnecessary Exchange operations in a single query.
**Plan signal**: More than 2 `Exchange` nodes in physical plan.
**Metric signal**: High cumulative `shuffleBytesWritten`.
**Fix**: Consolidate operations, cache intermediates, or pre-partition data.
</details>

<details>
<summary><strong>7. Missing Predicate Pushdown</strong> (MEDIUM)</summary>

**What**: Filters applied after data is read instead of at the source.
**Plan signal**: `Filter` above `Scan` with `PushedFilters: []`.
**Metric signal**: More bytes read than necessary for filtered result.
**Fix**: Ensure filters use partition columns and supported types.
</details>

<details>
<summary><strong>8. Small File Problem</strong> (MEDIUM)</summary>

**What**: Many tiny files causing metadata overhead and slow listing.
**Plan signal**: `numFiles` >> 1000 with small individual sizes.
**Metric signal**: Listing time dominates scan time.
**Fix**: OPTIMIZE / ZORDER, reduce shuffle partitions, enable auto-compact.
</details>

<details>
<summary><strong>9. Partition Count Mismatch</strong> (MEDIUM)</summary>

**What**: Shuffle partitions too high (tiny tasks) or too low (OOM/spill).
**Plan signal**: Visible in `Exchange` partition count.
**Metric signal**: Avg partition size << 50MB or >> 500MB.
**Fix**: Set `spark.sql.shuffle.partitions` to `total_shuffle_bytes / 128MB`.
</details>

<details>
<summary><strong>10. Collect on Large Data</strong> (MEDIUM)</summary>

**What**: `collect()` or `toPandas()` pulls massive data to driver.
**Plan signal**: `CollectLimit` or full `Collect` on large scan.
**Metric signal**: Driver memory spike or OOM.
**Fix**: Use `take(N)`, `.limit()`, or write results to table instead.
</details>

---

## Databricks-Specific Checks

When running on Databricks, also evaluate:

| Feature | Config Key | Expected Value | Why |
|---------|-----------|----------------|-----|
| AQE | `spark.sql.adaptive.enabled` | `true` | Runtime plan optimization |
| AQE Skew Join | `spark.sql.adaptive.skewJoin.enabled` | `true` | Auto-handles skew |
| AQE Coalesce | `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merges small partitions |
| Photon | `spark.databricks.photon.enabled` | `true` | 2-8x vectorized speedup |
| DPP | `spark.sql.optimizer.dynamicPartitionPruning.enabled` | `true` | Runtime partition pruning |
| Optimize Write | `spark.databricks.delta.optimizeWrite.enabled` | `true` | Right-sizes output files |
| Auto Compact | `spark.databricks.delta.autoCompact.enabled` | `true` | Compacts small files |
| Predictive I/O | `spark.databricks.io.skipping.enabled` | `true` | Stats-based file skipping |

---

## Examples

```
# Analyze the latest run of a specific job
/spark-tune --job-id 583122712958557 --profile myworkspace
```

```
# Analyze a specific run by run ID
/spark-tune --run-id 282634152109589 --profile myworkspace
```

```
# Analyze with job ID + run ID + catalog (for volume log lookup)
/spark-tune --job-id 583122712958557 --run-id 282634152109589 --catalog kishoremannava --schema tower_kpis
```

```
# Analyze from a local event log file
/spark-tune /tmp/spark_logs/0422-190328-5zvr75wi/eventlog
```

```
# Analyze the active SparkSession in a notebook
/spark-tune current
```

```
# Focus analysis on specific areas only
/spark-tune --job-id 583122712958557 --focus shuffles,skew,joins
```

```
# Analyze latest run with all defaults
/spark-tune --job-id 583122712958557
```

---

## Tips

- **Start with the physical plan** — it shows what Spark actually executed, not what it planned to do.
- **Compare before/after** — re-run the analyzer after applying fixes to measure improvement.
- **AQE is your friend** — enabling it alone resolves ~60% of common anti-patterns at runtime.
- **Photon + AQE together** yield ~2.9x median speedup on Databricks.
- **Target 128MB partitions** — this balances parallelism with per-task overhead.
- **Check the slowest stage first** — 80% of wall time is usually in one stage.
