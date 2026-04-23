# Databricks-Specific Optimizations Reference

## Feature Configuration Matrix

### Adaptive Query Execution (AQE)

| Config | Default | Recommended | Effect |
|--------|---------|-------------|--------|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | `true` | Master switch for all AQE features |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | `true` | Merge small post-shuffle partitions |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1mb` | `1mb` | Min size after coalescing |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | `true` | Auto-split skewed join partitions |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256mb` | `256mb` | Size threshold to detect skew |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | `5` | Skew factor vs median |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64mb` | `128mb` | Target post-coalesce partition size |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | `true` | Read local shuffle data when possible |

**What AQE does at runtime:**
1. Collects accurate statistics after each shuffle
2. Converts SortMergeJoin → BroadcastHashJoin if one side is small enough
3. Splits skewed partitions into multiple tasks
4. Coalesces small partitions to reduce task overhead
5. Optimizes skewed aggregations

**Median speedup**: 1.38x (benchmarked across mixed workloads)

---

### Photon Engine

| Config | Default | Recommended | Effect |
|--------|---------|-------------|--------|
| `spark.databricks.photon.enabled` | varies | `true` | Enable Photon vectorized C++ engine |
| `spark.databricks.photon.allDataSources.enabled` | `true` | `true` | Photon for all data sources (not just Delta) |

**What Photon accelerates:**
- Scan operations (columnar reads)
- Filter/Project operations
- Hash aggregations
- Hash joins
- String operations
- Sort operations

**Typical speedup**: 2-8x for scan-heavy and aggregation workloads
**Combined AQE + Photon**: ~2.87x median speedup

**When Photon falls back to Spark JVM:**
- UDFs (Python/Scala)
- Complex nested types in some operations
- Some legacy data formats
- Unsupported expression types

---

### Dynamic Partition Pruning (DPP)

| Config | Default | Recommended | Effect |
|--------|---------|-------------|--------|
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | `true` | `true` | Runtime partition elimination |
| `spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly` | `true` | `true` | Reuse broadcast for pruning |

**Example**: When joining `fact_table` with a filtered `dim_table` on `date`, DPP uses the filter result to prune `fact_table` partitions at runtime — even if the fact table filter wasn't explicit.

---

### Delta Lake Optimizations

| Config | Default | Recommended | Effect |
|--------|---------|-------------|--------|
| `spark.databricks.delta.optimizeWrite.enabled` | `false` | `true` | Coalesces small writes into optimal file sizes |
| `spark.databricks.delta.autoCompact.enabled` | `false` | `true` | Auto-compacts small files after writes |
| `spark.databricks.delta.autoCompact.minNumFiles` | `50` | `50` | Min files to trigger auto-compact |
| `spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite` | `false` | `true` | Table-level default |
| `spark.databricks.delta.retentionDurationCheck.enabled` | `true` | `true` | Safety check for VACUUM |

**Delta-specific tuning commands:**
```sql
-- Compact small files
OPTIMIZE schema.table;

-- Compact + Z-ORDER for query patterns
OPTIMIZE schema.table ZORDER BY (country, state);

-- Liquid clustering (replaces Z-ORDER, auto-maintained)
ALTER TABLE schema.table CLUSTER BY (country, state);

-- Check file statistics
DESCRIBE DETAIL schema.table;

-- Show file-level info
DESCRIBE HISTORY schema.table;
```

---

### Predictive I/O and Data Skipping

| Config | Default | Recommended | Effect |
|--------|---------|-------------|--------|
| `spark.databricks.io.skipping.enabled` | `true` | `true` | Stats-based file skipping |
| `spark.databricks.io.skipping.stringPrefixLength` | `32` | `32` | String prefix for min/max stats |

**How it works:**
- Delta stores per-file min/max statistics for first 32 columns
- Queries with filters skip files where min > filter_value or max < filter_value
- Z-ORDER and Liquid Clustering improve skipping effectiveness by co-locating data

---

### Cluster Sizing Quick Reference

| Workload Type | Workers | Worker Type | Driver | Key Config |
|--------------|---------|-------------|--------|------------|
| Light ETL (< 10GB) | 1-2 | Standard_D4ds_v5 | Same | `shuffle.partitions=8` |
| Medium ETL (10-100GB) | 2-8 | Standard_D8ds_v5 | Standard_D4ds_v5 | `shuffle.partitions=200` |
| Heavy ETL (100GB-1TB) | 8-32 | Standard_D16ds_v5 | Standard_D8ds_v5 | `shuffle.partitions=auto (AQE)` |
| ML Training | 2-16 | Standard_NC6s_v3 (GPU) | Standard_D8ds_v5 | GPU-specific configs |
| Interactive/SQL | 1-4 (autoscale) | Standard_D4ds_v5 | Standard_D4ds_v5 | Photon enabled |

---

### Spark Config Audit Checklist

When analyzing a Databricks job, check these configs and flag deviations:

```python
DATABRICKS_CONFIG_AUDIT = {
    # (config_key, expected_value, severity_if_wrong, reason)
    ("spark.sql.adaptive.enabled", "true", "HIGH",
     "AQE provides ~1.4x speedup with zero code changes"),
    ("spark.sql.adaptive.skewJoin.enabled", "true", "HIGH",
     "Handles data skew automatically at runtime"),
    ("spark.sql.adaptive.coalescePartitions.enabled", "true", "MEDIUM",
     "Prevents too-small partitions after shuffle"),
    ("spark.databricks.photon.enabled", "true", "MEDIUM",
     "Photon provides 2-8x speedup for supported operations"),
    ("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true", "MEDIUM",
     "Prunes fact table partitions based on dimension filters"),
    ("spark.databricks.delta.optimizeWrite.enabled", "true", "MEDIUM",
     "Prevents small file accumulation on writes"),
    ("spark.databricks.delta.autoCompact.enabled", "true", "LOW",
     "Auto-compacts small files; reduces future scan overhead"),
    ("spark.sql.shuffle.partitions", "auto or tuned", "MEDIUM",
     "Default 200 is wrong for most workloads; use AQE or tune explicitly"),
    ("spark.sql.autoBroadcastJoinThreshold", "10485760", "LOW",
     "Default 10MB; increase to 100MB+ if small dimension tables are shuffled"),
}
```
