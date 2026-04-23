"""
Validates all input parameter scenarios for the /spark-tune skill.
Extracted from SKILL.md parse_inputs function and tested against
every documented usage pattern.
"""

import re
import json

def parse_inputs(args: str) -> dict:
    """Parse skill invocation arguments into structured inputs."""
    inputs = {
        "job_id": None,
        "run_id": None,
        "event_log_path": None,
        "profile": "DEFAULT",
        "catalog": None,
        "schema": None,
        "focus": "all",
        "mode": None,
    }

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

    if "current" in args.lower() and not inputs["mode"]:
        inputs["mode"] = "current"
    elif not inputs["mode"]:
        remaining = re.sub(r'--\S+\s+\S+', '', args).strip()
        if remaining:
            inputs["event_log_path"] = remaining
            inputs["mode"] = "file"

    if inputs["mode"] == "job" and not inputs["run_id"]:
        inputs["resolve_latest_run"] = True

    return inputs


# -----------------------------------------------------------------------
# TEST SCENARIOS
# -----------------------------------------------------------------------

TESTS = [
    # --- Scenario 1: --job-id only (resolve latest run) ---
    {
        "name": "job-id only",
        "input": "--job-id 583122712958557",
        "expect": {
            "mode": "job",
            "job_id": 583122712958557,
            "run_id": None,
            "profile": "DEFAULT",
            "catalog": None,
            "focus": "all",
            "resolve_latest_run": True,
        },
    },
    # --- Scenario 2: --run-id only (auto-resolves job_id from API) ---
    {
        "name": "run-id only",
        "input": "--run-id 282634152109589",
        "expect": {
            "mode": "run",
            "run_id": 282634152109589,
            "job_id": None,  # resolved at runtime from API
            "profile": "DEFAULT",
        },
    },
    # --- Scenario 3: --job-id + --run-id (explicit both) ---
    {
        "name": "job-id + run-id",
        "input": "--job-id 583122712958557 --run-id 282634152109589",
        "expect": {
            "mode": "run",  # run-id takes precedence
            "job_id": 583122712958557,
            "run_id": 282634152109589,
        },
    },
    # --- Scenario 4: --run-id + --profile ---
    {
        "name": "run-id + profile",
        "input": "--run-id 282634152109589 --profile myworkspace",
        "expect": {
            "mode": "run",
            "run_id": 282634152109589,
            "profile": "myworkspace",
        },
    },
    # --- Scenario 5: --job-id + --profile ---
    {
        "name": "job-id + profile",
        "input": "--job-id 583122712958557 --profile myworkspace",
        "expect": {
            "mode": "job",
            "job_id": 583122712958557,
            "profile": "myworkspace",
            "resolve_latest_run": True,
        },
    },
    # --- Scenario 6: --run-id + --catalog + --schema ---
    {
        "name": "run-id + catalog + schema",
        "input": "--run-id 282634152109589 --catalog kishoremannava --schema tower_kpis",
        "expect": {
            "mode": "run",
            "run_id": 282634152109589,
            "catalog": "kishoremannava",
            "schema": "tower_kpis",
        },
    },
    # --- Scenario 7: --job-id + --catalog + --schema + --profile ---
    {
        "name": "all flags (job-id)",
        "input": "--job-id 583122712958557 --catalog kishoremannava --schema tower_kpis --profile myworkspace --focus shuffles,skew",
        "expect": {
            "mode": "job",
            "job_id": 583122712958557,
            "catalog": "kishoremannava",
            "schema": "tower_kpis",
            "profile": "myworkspace",
            "focus": "shuffles,skew",
            "resolve_latest_run": True,
        },
    },
    # --- Scenario 8: --run-id + all optional flags ---
    {
        "name": "all flags (run-id)",
        "input": "--run-id 282634152109589 --catalog kishoremannava --schema tower_kpis --profile myworkspace --focus joins",
        "expect": {
            "mode": "run",
            "run_id": 282634152109589,
            "catalog": "kishoremannava",
            "schema": "tower_kpis",
            "profile": "myworkspace",
            "focus": "joins",
        },
    },
    # --- Scenario 9: event log file path (local) ---
    {
        "name": "local event log path",
        "input": "/tmp/spark_logs/0422-190328-5zvr75wi/eventlog",
        "expect": {
            "mode": "file",
            "event_log_path": "/tmp/spark_logs/0422-190328-5zvr75wi/eventlog",
            "job_id": None,
            "run_id": None,
        },
    },
    # --- Scenario 10: DBFS event log path ---
    {
        "name": "DBFS event log path",
        "input": "/dbfs/cluster-logs/app-20260422/eventlog",
        "expect": {
            "mode": "file",
            "event_log_path": "/dbfs/cluster-logs/app-20260422/eventlog",
        },
    },
    # --- Scenario 11: current (live session) ---
    {
        "name": "current session",
        "input": "current",
        "expect": {
            "mode": "current",
            "job_id": None,
            "run_id": None,
        },
    },
    # --- Scenario 12: current + focus ---
    {
        "name": "current + focus",
        "input": "current --focus joins,shuffles",
        "expect": {
            "mode": "current",
            "focus": "joins,shuffles",
        },
    },
    # --- Scenario 13: --focus only (should have no mode — edge case) ---
    {
        "name": "focus only (no source — edge case)",
        "input": "--focus skew",
        "expect": {
            "mode": None,
            "focus": "skew",
        },
    },
    # --- Scenario 14: empty string ---
    {
        "name": "empty input",
        "input": "",
        "expect": {
            "mode": None,
            "job_id": None,
            "run_id": None,
            "profile": "DEFAULT",
            "focus": "all",
        },
    },
    # --- Scenario 15: --job-id + --run-id + --catalog + --schema + --profile + --focus ---
    {
        "name": "kitchen sink",
        "input": "--job-id 583122712958557 --run-id 580297051311764 --profile myworkspace --catalog kishoremannava --schema tower_kpis --focus shuffles,spill,skew",
        "expect": {
            "mode": "run",
            "job_id": 583122712958557,
            "run_id": 580297051311764,
            "profile": "myworkspace",
            "catalog": "kishoremannava",
            "schema": "tower_kpis",
            "focus": "shuffles,spill,skew",
        },
    },
    # --- Scenario 16: run-id with different ordering ---
    {
        "name": "flags in reverse order",
        "input": "--focus joins --schema tower_kpis --catalog kishoremannava --profile myworkspace --run-id 123456789",
        "expect": {
            "mode": "run",
            "run_id": 123456789,
            "profile": "myworkspace",
            "catalog": "kishoremannava",
            "schema": "tower_kpis",
            "focus": "joins",
        },
    },
]


# -----------------------------------------------------------------------
# RUN TESTS
# -----------------------------------------------------------------------

passed = 0
failed = 0
total = len(TESTS)

print(f"{'=' * 80}")
print(f"SPARK-TUNE INPUT PARAMETER VALIDATION — {total} scenarios")
print(f"{'=' * 80}\n")

for i, test in enumerate(TESTS, 1):
    result = parse_inputs(test["input"])
    errors = []

    for key, expected_val in test["expect"].items():
        actual_val = result.get(key)
        if actual_val != expected_val:
            errors.append(f"    {key}: expected={expected_val!r}, got={actual_val!r}")

    if errors:
        failed += 1
        print(f"  FAIL  #{i:2d}  {test['name']}")
        print(f"         Input: {test['input']!r}")
        for err in errors:
            print(err)
        print()
    else:
        passed += 1
        print(f"  PASS  #{i:2d}  {test['name']}")

print(f"\n{'=' * 80}")
print(f"RESULTS: {passed}/{total} passed, {failed}/{total} failed")
print(f"{'=' * 80}")

if failed > 0:
    exit(1)
