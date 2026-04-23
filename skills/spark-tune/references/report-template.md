# Spark Performance Tuning Report Template

Use this template to generate the final output report. Replace all `{{placeholders}}`.

---

```markdown
# Spark Performance Tuning Report

| Field | Value |
|-------|-------|
| **Job Name** | {{job_name}} |
| **Run ID** | {{run_id}} |
| **Cluster** | {{cluster_id}} ({{cluster_type}}) |
| **Spark Version** | {{spark_version}} |
| **Date** | {{execution_date}} |
| **Total Wall Time** | {{wall_time_s}} seconds |
| **Total Stages** | {{num_stages}} |
| **Total Tasks** | {{num_tasks}} |

---

## Executive Summary

| Metric | Value | Status |
|--------|-------|--------|
| **Overall Health** | {{health_status}} | {{health_emoji}} |
| **Anti-Patterns Found** | {{total_findings}} ({{critical_count}} critical, {{high_count}} high, {{medium_count}} medium) | |
| **Shuffle Volume** | {{total_shuffle_gb}} GB | {{shuffle_status}} |
| **Disk Spill** | {{total_spill_gb}} GB | {{spill_status}} |
| **Max Skew Factor** | {{max_skew}}x (Stage {{skew_stage}}) | {{skew_status}} |
| **Estimated Improvement** | {{improvement_range}} | |

### Health Score Calculation
- Start at 100 points
- CRITICAL finding: -25 points each
- HIGH finding: -10 points each
- MEDIUM finding: -5 points each
- Score >= 80: GOOD | 50-79: NEEDS ATTENTION | < 50: CRITICAL

---

## Job Topology

| Job ID | Stages | Tasks | Duration (s) | Shuffle Write | Shuffle Read | Spill |
|--------|--------|-------|-------------|--------------|-------------|-------|
{{#each jobs}}
| {{job_id}} | {{stage_count}} | {{task_count}} | {{duration_s}} | {{shuffle_write_mb}} MB | {{shuffle_read_mb}} MB | {{spill_mb}} MB |
{{/each}}

---

## Stage Breakdown

| Stage | Name | Tasks | Duration | Shuffle W | Shuffle R | Spill | Skew |
|-------|------|-------|----------|-----------|-----------|-------|------|
{{#each stages}}
| {{stage_id}} | {{stage_name}} | {{num_tasks}} | {{duration_s}}s | {{shuffle_write_mb}} MB | {{shuffle_read_mb}} MB | {{spill_mb}} MB | {{skew_factor}}x |
{{/each}}

**Slowest Stage**: Stage {{slowest_stage_id}} — {{slowest_stage_name}} ({{slowest_stage_duration}}s, {{slowest_stage_pct}}% of total)

---

## Plan Analysis

{{#each sql_executions}}
### Query {{execution_id}}: {{description}}

**Physical Plan:**
\`\`\`
{{physical_plan}}
\`\`\`

**Findings:**

| # | Severity | Issue | Details | Fix |
|---|----------|-------|---------|-----|
{{#each findings}}
| {{@index}} | {{severity}} | {{issue}} | {{details}} | {{recommendation}} |
{{/each}}

---
{{/each}}

## Tuning Metrics

| Metric | Current | Target | Status | Action |
|--------|---------|--------|--------|--------|
| Shuffle Ratio | {{shuffle_ratio}}% | < 30% | {{shuffle_ratio_status}} | {{shuffle_ratio_action}} |
| Spill Ratio | {{spill_ratio}}% | 0% | {{spill_ratio_status}} | {{spill_ratio_action}} |
| Max Skew Factor | {{max_skew}}x | < 3x | {{skew_status}} | {{skew_action}} |
| Avg Partition Size | {{avg_partition_mb}} MB | 128 MB | {{partition_status}} | {{partition_action}} |
| Shuffle Partitions | {{current_partitions}} | {{recommended_partitions}} | {{partition_count_status}} | {{partition_count_action}} |
| Total Input Read | {{total_input_gb}} GB | — | — | — |
| Total Shuffle Volume | {{total_shuffle_gb}} GB | — | — | — |

---

## Configuration Recommendations

| Priority | Config Key | Current | Recommended | Reason |
|----------|-----------|---------|-------------|--------|
{{#each config_recommendations}}
| {{priority}} | `{{key}}` | `{{current}}` | `{{recommended}}` | {{reason}} |
{{/each}}

**Apply these configs:**
\`\`\`python
spark.conf.set("{{key}}", "{{recommended}}")
\`\`\`

Or in cluster Spark Config:
\`\`\`
{{#each config_recommendations}}
{{key}} {{recommended}}
{{/each}}
\`\`\`

---

## Code-Level Fixes

{{#each code_fixes}}
### Fix {{@index}}: {{title}}

| Field | Value |
|-------|-------|
| **Severity** | {{severity}} |
| **Location** | {{location}} |
| **Impact** | {{expected_impact}} |

**Before:**
\`\`\`python
{{before_code}}
\`\`\`

**After:**
\`\`\`python
{{after_code}}
\`\`\`

---
{{/each}}

## Databricks Feature Audit

| Feature | Config | Current | Recommended | Impact |
|---------|--------|---------|-------------|--------|
| AQE | `spark.sql.adaptive.enabled` | {{aqe_status}} | `true` | ~1.4x speedup |
| AQE Skew Join | `spark.sql.adaptive.skewJoin.enabled` | {{aqe_skew_status}} | `true` | Handles data skew |
| AQE Coalesce | `spark.sql.adaptive.coalescePartitions.enabled` | {{aqe_coalesce_status}} | `true` | Right-sizes partitions |
| Photon | `spark.databricks.photon.enabled` | {{photon_status}} | `true` | 2-8x for supported ops |
| DPP | `spark.sql.optimizer.dynamicPartitionPruning.enabled` | {{dpp_status}} | `true` | Runtime partition skip |
| Optimize Write | `spark.databricks.delta.optimizeWrite.enabled` | {{opt_write_status}} | `true` | Prevents small files |
| Auto Compact | `spark.databricks.delta.autoCompact.enabled` | {{auto_compact_status}} | `true` | Auto file compaction |

---

## Next Steps

1. [ ] Apply **critical** configuration changes (Priority 1 items above)
2. [ ] Implement code-level fixes for CRITICAL/HIGH findings
3. [ ] Re-run the job with new configs
4. [ ] Run `/spark-log-analyzer` again on the new run to compare metrics
5. [ ] Set up recurring monitoring if shuffle/spill/skew trends appear

---

## Appendix: Raw Metrics

<details>
<summary>Stage Metrics JSON</summary>

\`\`\`json
{{stage_metrics_json}}
\`\`\`
</details>

<details>
<summary>Task Skew Report</summary>

| Stage | Skew Factor | P50 (ms) | P99 (ms) | Max (ms) | Assessment |
|-------|-------------|----------|----------|----------|------------|
{{#each skew_report}}
| {{stage_id}} | {{skew_factor}}x | {{p50}} | {{p99}} | {{max}} | {{assessment}} |
{{/each}}
</details>

<details>
<summary>Spark Configuration Dump</summary>

\`\`\`
{{spark_conf_dump}}
\`\`\`
</details>

---

*Report generated by `/spark-tune` on {{report_date}}*
```
