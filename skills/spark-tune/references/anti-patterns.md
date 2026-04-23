# Spark Anti-Pattern Detection Reference

## Detection Matrix

This reference maps each anti-pattern to its detection signals across three sources:
physical plan text, stage-level metrics, and task-level metrics.

---

## Plan Text Patterns (regex-ready)

```python
PLAN_PATTERNS = {
    # Pattern name: (regex, severity, description)
    "cartesian_join": (
        r"CartesianProduct",
        "CRITICAL",
        "Cross join without condition — output = N x M rows",
    ),
    "sort_merge_join": (
        r"SortMergeJoin",
        "HIGH",
        "SortMergeJoin detected — verify both sides are large; otherwise use broadcast",
    ),
    "broadcast_hash_join": (
        r"BroadcastHashJoin",
        "INFO",
        "BroadcastHashJoin — good, small table is broadcast",
    ),
    "exchange_shuffle": (
        r"Exchange\s+(hashpartitioning|rangepartitioning|SinglePartition)",
        "INFO",
        "Shuffle exchange — count occurrences; >2 per query is HIGH",
    ),
    "empty_pushed_filters": (
        r"PushedFilters:\s*\[\]",
        "MEDIUM",
        "No filters pushed to data source — check column types",
    ),
    "filter_above_scan": (
        r"Filter.*\n.*Scan",
        "MEDIUM",
        "Filter applied after scan — predicate not pushed down",
    ),
    "collect_limit": (
        r"CollectLimit\s+(\d+)",
        "INFO",
        "CollectLimit — check limit value; unlimited collect is dangerous",
    ),
    "whole_stage_codegen": (
        r"WholeStageCodegen",
        "INFO",
        "Whole-stage codegen active — good for CPU-bound operations",
    ),
    "in_memory_scan": (
        r"InMemoryTableScan",
        "INFO",
        "Reading from cached table — verify cache is still valid",
    ),
    "file_scan": (
        r"FileScan\s+(parquet|delta|csv|json|orc)\s+\[([^\]]*)\]",
        "INFO",
        "File scan — capture format and pushed columns",
    ),
    "subquery": (
        r"Subquery\s+subquery",
        "MEDIUM",
        "Correlated subquery — may execute once per row in outer query",
    ),
    "window": (
        r"Window\s+\[",
        "INFO",
        "Window function — check partition key for potential skew",
    ),
    "expand": (
        r"Expand\s+\[",
        "MEDIUM",
        "Expand (from CUBE/ROLLUP/GROUPING SETS) — row multiplication",
    ),
    "broadcast_nested_loop": (
        r"BroadcastNestedLoopJoin",
        "HIGH",
        "BroadcastNestedLoopJoin — O(N*M) comparison, very slow for large data",
    ),
}
```

---

## Stage Metric Thresholds

```python
STAGE_THRESHOLDS = {
    "shuffle_write_bytes": {
        "warning":  1 * 1024**3,   # 1 GB
        "critical": 10 * 1024**3,  # 10 GB
        "message":  "High shuffle write — consider broadcast join or pre-partitioning",
    },
    "shuffle_read_bytes": {
        "warning":  1 * 1024**3,
        "critical": 10 * 1024**3,
        "message":  "High shuffle read — reduce partition count or use broadcast",
    },
    "disk_bytes_spilled": {
        "warning":  1,             # any spill is concerning
        "critical": 100 * 1024**2, # 100 MB
        "message":  "Disk spill detected — increase executor memory or partitions",
    },
    "memory_bytes_spilled": {
        "warning":  100 * 1024**2,
        "critical": 1 * 1024**3,
        "message":  "Memory spill — data overflows execution memory to storage memory",
    },
    "stage_duration_s": {
        "warning":  300,           # 5 min
        "critical": 1800,         # 30 min
        "message":  "Long-running stage — investigate tasks for skew or spill",
    },
}
```

---

## Task Metric Skew Detection

```python
def compute_skew_report(tasks_by_stage: dict) -> list:
    """
    Compute skew metrics per stage.

    Returns list of:
      (stage_id, skew_factor, p50_ms, p99_ms, max_ms, recommendation)
    """
    import statistics

    report = []
    for stage_id, tasks in tasks_by_stage.items():
        durations = [t["duration_ms"] for t in tasks if t["duration_ms"] > 0]
        if len(durations) < 2:
            continue

        p50 = statistics.median(durations)
        p99 = sorted(durations)[int(len(durations) * 0.99)]
        max_d = max(durations)
        skew = max_d / p50 if p50 > 0 else 0

        if skew > 10:
            rec = "CRITICAL: Extreme skew — salt join/group keys or enable AQE skew handling"
        elif skew > 5:
            rec = "HIGH: Significant skew — enable spark.sql.adaptive.skewJoin.enabled=true"
        elif skew > 3:
            rec = "MEDIUM: Moderate skew — monitor; AQE should handle this"
        else:
            rec = "OK: No significant skew"

        report.append((stage_id, round(skew, 2), round(p50), round(p99), round(max_d), rec))

    return sorted(report, key=lambda x: -x[1])  # worst skew first
```

---

## Shuffle Partition Sizing Formula

```python
def recommend_partitions(total_shuffle_bytes: int, target_mb: int = 128) -> dict:
    """
    Compute recommended shuffle partition count.

    Rule of thumb:
      - Target partition size: 128 MB (good balance of parallelism and overhead)
      - Min partitions: 2x number of cores
      - Max partitions: 8000 (Spark internal limit for efficient scheduling)
    """
    target_bytes = target_mb * 1024 * 1024
    recommended = max(1, int(total_shuffle_bytes / target_bytes))

    return {
        "total_shuffle_bytes": total_shuffle_bytes,
        "total_shuffle_gb": round(total_shuffle_bytes / (1024**3), 2),
        "target_partition_mb": target_mb,
        "recommended_partitions": min(recommended, 8000),
        "formula": f"ceil({total_shuffle_bytes} / {target_bytes}) = {recommended}",
    }
```

---

## Join Strategy Decision Tree

```
Is one side < autoBroadcastJoinThreshold (default 10MB)?
├── YES → BroadcastHashJoin ✅ (fastest)
└── NO
    ├── Are both sides sorted on join key?
    │   ├── YES → SortMergeJoin (no extra sort needed)
    │   └── NO
    │       ├── Is one side small enough to build hash table in memory?
    │       │   ├── YES → ShuffledHashJoin
    │       │   └── NO → SortMergeJoin (with sort)
    └── Is there no equi-join condition?
        ├── One side small → BroadcastNestedLoopJoin
        └── Both sides large → CartesianProduct ⚠️ CRITICAL
```

**AQE overrides** (when `spark.sql.adaptive.enabled=true`):
- After shuffle, if actual data size of one join side < `autoBroadcastJoinThreshold`:
  → Dynamically converts `SortMergeJoin` to `BroadcastHashJoin`
- If partition skew detected:
  → Splits skewed partition into multiple tasks
