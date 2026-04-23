# spark-tune

A Claude Code plugin that analyzes Spark execution logs from Databricks jobs and generates prioritized performance tuning recommendations.

## What It Does

The `/spark-tune` skill extracts query plans, stage metrics, and task-level statistics from Spark event logs, then produces a structured **Performance Tuning Report** with actionable fixes for:

- Shuffle spill and excessive partitioning
- Data skew across stages and tasks
- Suboptimal join strategies (SortMergeJoin vs BroadcastHashJoin)
- Missing or misconfigured Adaptive Query Execution (AQE)
- Inefficient serialization, caching, and I/O patterns

## Prerequisites

- **Databricks CLI** configured with a workspace profile (`~/.databrickscfg`)
- **curl** and **jq** available on PATH
- A personal access token with View, Attach, and Manage permissions (see [CONNECTORS.md](CONNECTORS.md))

## Getting Started

1. **Install the plugin** by adding this repository to your Claude Code plugins.

2. **Run the skill** with a Databricks job ID:
   ```
   /spark-tune --job-id 123456
   ```

3. **Or specify a specific run:**
   ```
   /spark-tune --run-id 789012
   ```

4. **Or point to a local event log:**
   ```
   /spark-tune /path/to/eventlog.json
   ```

The skill will fetch logs, analyze execution plans, and generate a Markdown tuning report with before/after comparisons.

## Skills

| Skill | Description |
|-------|-------------|
| [/spark-tune](skills/spark-tune/SKILL.md) | Analyze Spark logs and generate a performance tuning report |

## Project Structure

```
spark-tuning/
├── .claude-plugin/plugin.json   Plugin manifest
├── README.md                    This file
├── CONNECTORS.md                API authentication and endpoints
└── skills/spark-tune/
    ├── SKILL.md                 Skill definition (893 lines)
    ├── test_inputs.py           Input validation test suite
    └── references/
        ├── anti-patterns.md     Spark anti-pattern detection reference
        ├── databricks-optimizations.md  Databricks-specific tuning checklist
        └── report-template.md   Output report template
```

## License

MIT
