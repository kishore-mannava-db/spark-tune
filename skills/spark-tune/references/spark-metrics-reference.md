# Spark QPL Metrics Reference

Concise reference for interpreting Spark SQL query plan metrics from the Spark UI.

## Metric Format

Most metrics follow: `total (min, med, max)`

Example: `5.6 s (381 ms, 634 ms, 761 ms)`
- **total** = 5.6 s: sum across ALL tasks in the stage
- **min** = 381 ms: fastest individual task
- **med** = 634 ms: median task (typical task)
- **max** = 761 ms: slowest task (straggler indicator)

When max >> med (e.g. 10x), there is likely **data skew** or a **straggler task**.

## Units

| Abbreviation | Meaning |
|---|---|
| ms | milliseconds |
| s | seconds |
| m | minutes |
| B | bytes |
| KiB, MiB, GiB, TiB | binary units (1 KiB = 1024 B) |

## Operator-Level Metrics

### Universal (appear on most operators)

| Metric | What it means | Red flag |
|---|---|---|
| **rows output** | Number of rows produced | Unexpected increase = join fanout or missing filter |
| **duration total** | Wall-clock time across all tasks | High value = bottleneck operator |
| **peak memory total** | Maximum memory used | Close to executor memory limit = OOM risk |
| **spill size total** | Data spilled from memory to disk | Any non-zero value = memory pressure |

### Scan Operators (PhotonScan, Scan parquet, etc.)

| Metric | What it means | Red flag |
|---|---|---|
| **scan time total** | Time reading data from storage | High = slow storage or large scan |
| **metadata time** | Time reading file metadata/footers | High = too many small files |
| **number of files read** | Files opened for reading | Very high = small file problem |
| **size of files read total** | Total bytes read from storage | Compare with rows output for selectivity |
| **rows output** | Rows after pushdown filters | Compare with "number of output rows" pre-filter |
| **dynamic partition pruning** | Whether DPP was applied | 0 when expected = missing optimization |
| **IO cache hit ratio** | % of reads served from cache | Low = cold cache or large scan |

### Shuffle / Exchange Operators

| Metric | What it means | Red flag |
|---|---|---|
| **data size total** | Total shuffle data volume | Large = expensive shuffle |
| **shuffle bytes written total** | Bytes written by map-side shuffle | Large max vs med = write skew |
| **shuffle write time total** | Time writing shuffle data | High = disk I/O bottleneck |
| **fetch wait time total** | Time waiting for shuffle reads | High = network bottleneck or slow upstream |
| **remote bytes read total** | Bytes fetched from other executors | Large = cross-node data movement |
| **local bytes read total** | Bytes read from same executor | Higher is better (data locality) |
| **records read** | Number of shuffle records read | Compare with upstream rows output |
| **number of partitions** | Shuffle partition count | Too few = large tasks; too many = scheduling overhead |
| **corrupt merged block chunks** | Corrupted push-based shuffle blocks | Non-zero = shuffle corruption |

### AQEShuffleRead (Adaptive Query Execution)

| Metric | What it means | Red flag |
|---|---|---|
| **number of partitions** | Partitions after AQE coalescing | Very few = potential large partitions |
| **number of coalesced partitions** | How many partitions were merged | High = many empty/small original partitions |
| **partition data size (min, med, max)** | Data per partition after coalescing | max >> med = skew surviving coalesce |
| **number of empty partitions** | Partitions with no data | Many = poor hash distribution |

### Join Operators (PhotonShuffledHashJoin, BroadcastHashJoin, SortMergeJoin)

| Metric | What it means | Red flag |
|---|---|---|
| **rows output** | Rows after join | >> sum of inputs = fanout/cartesian |
| **build time total** | Time building hash table (build side) | High = large build side |
| **stream time total** | Time probing hash table (stream side) | High = large stream side |
| **build data size** | Size of build-side hash table | Large = memory pressure, potential spill |
| **build rows** | Rows in build side | Compare with stream rows for join ratio |
| **stream rows** | Rows in stream side | Large = expected for fact table side |
| **condition evaluation time** | Time evaluating join conditions | High = complex join predicate |

### Aggregation Operators (PhotonGroupingAgg, ObjectHashAggregate, HashAggregate)

| Metric | What it means | Red flag |
|---|---|---|
| **time in aggregation build total** | Time building aggregation hash map | High = many groups or complex agg |
| **spill size total** | Aggregation data spilled to disk | Non-zero = too many groups for memory |
| **number of sort fallback tasks** | Tasks that fell back to sort-based agg | Non-zero = hash table too large |
| **rows output** | Number of groups produced | Compare with input rows for reduction ratio |

### Write Operators (PhotonParquetWriter, etc.)

| Metric | What it means | Red flag |
|---|---|---|
| **rows output** | Rows written | Should match expected output |
| **written output rows** | Same as above (alternate name) | |
| **written output bytes** | Total bytes written | Large = check compression settings |
| **number of written files** | Output file count | Very high = small file problem |
| **duration total** | Time writing | High = storage bottleneck |

### Stage-Level Metrics (in cluster labels)

| Metric | What it means |
|---|---|
| **stage duration: total (min, med, max)** | Wall-clock time for the stage across all tasks |
| **duration** (WholeStageCodegen) | Total codegen execution time |

## Diagnostic Patterns

| Symptom | Likely Cause | Where to Look |
|---|---|---|
| High duration + high spill | Memory pressure | Increase executor memory or reduce data per task |
| High fetch wait time | Shuffle bottleneck | Check upstream shuffle sizes, network config |
| rows output >> input rows at join | Join fanout | Check join keys, missing filters, data quality |
| max >> median in partition size | Data skew | Consider salting, repartitioning, or AQE skew join |
| Many empty partitions | Poor hash distribution | Check partition key cardinality |
| High scan time + many files | Small file problem | Run OPTIMIZE/compaction on Delta table |
| High metadata time | Too many files | Compact files, check partition pruning |
| Sort fallback tasks > 0 | Aggregation memory overflow | Increase memory or reduce group cardinality |
| Large build data size at join | Build side too large | Swap join sides, use broadcast if small enough |
| Duration increase between runs | Regression | Compare row counts first, then shuffle/scan metrics |
