#!/usr/bin/env python3
# /// script
# dependencies = [
#   "beautifulsoup4>=4.12",
# ]
# requires-python = ">=3.6"
# ///
"""
Spark SQL Query Plan (QPL) HTML Parser.

Parses a downloaded Spark UI "Details for Query N" HTML page and produces
a JSON file with the DAG topology, operators with per-operator metrics,
stages, physical plan, execution summary, sql_properties, io_cache, and more.

Usage:
    python parse_qpl.py <path_to_html_file_or_directory> [--force]

Output files are written to parsed_output/{QueryName}/ next to the input.
"""

import json
import re
import sys
import html as html_module
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# HTML Parsing helpers
# ---------------------------------------------------------------------------

def load_html(path: str) -> BeautifulSoup:
    with open(path, "r", encoding="utf-8") as f:
        return BeautifulSoup(f.read(), "html.parser")


# ---------------------------------------------------------------------------
# Query metadata (title, time, duration, jobs, AQE versions)
# ---------------------------------------------------------------------------

def extract_query_metadata(soup: BeautifulSoup) -> dict:
    meta = {}

    title_tag = soup.find("title")
    if title_tag:
        full_title = title_tag.get_text(strip=True)
        m = re.search(r"Details for Query \d+", full_title)
        meta["title"] = m.group(0) if m else full_title

    ul = soup.find("ul", class_="list-unstyled")
    if ul:
        for li in ul.find_all("li"):
            text = li.get_text(" ", strip=True)
            if "Submitted Time:" in text:
                meta["submitted_time"] = text.split("Submitted Time:")[-1].strip()
            elif "Duration:" in text:
                meta["duration"] = text.split("Duration:")[-1].strip()
            elif "Succeeded Jobs:" in text:
                meta["succeeded_jobs"] = [
                    int(a.get_text(strip=True))
                    for a in li.find_all("a")
                    if a.get_text(strip=True).isdigit()
                ]
            elif "AQE Plan Versions:" in text:
                meta["aqe_plan_versions"] = [
                    int(a.get_text(strip=True))
                    for a in li.find_all("a")
                    if a.get_text(strip=True).isdigit()
                ]
    return meta


# ---------------------------------------------------------------------------
# DOT file parsing (graph topology, node labels, clusters)
# ---------------------------------------------------------------------------

def extract_dot_content(soup: BeautifulSoup) -> str:
    dot_div = soup.find("div", class_="dot-file")
    if dot_div:
        return dot_div.get_text()
    return ""


def _unescape_dot_label(raw: str) -> str:
    """Unescape HTML entities that appear inside DOT node labels."""
    return html_module.unescape(raw)


def _extract_operator_name(label_html: str) -> str:
    """Pull the bold operator name from a node label HTML snippet."""
    s = BeautifulSoup(label_html, "html.parser")
    b = s.find("b")
    if b:
        return b.get_text(strip=True)
    return label_html.strip()[:80]


def _extract_metrics_from_label(label_html: str) -> List[dict]:
    """Extract Metric/Value rows from the sql-metrics-table inside a label."""
    s = BeautifulSoup(label_html, "html.parser")
    table = s.find("table", class_=re.compile(r"sql-metrics-table"))
    if not table:
        return []
    metrics = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            is_experimental = "hideable-cell" in (tr.get("class") or [])
            metrics.append({
                "metric": tds[0].get_text(strip=True),
                "value": tds[1].get_text(strip=True),
                "is_experimental": is_experimental,
            })
    return metrics


def parse_dot_graph(dot_text: str) -> Tuple[dict, list, dict]:
    """
    Parse the DOT digraph text.

    Returns:
        nodes: {node_id: {"name": str, "metrics": list[dict]}}
        edges: [(child_id, parent_id, row_label_or_None)]
        clusters: {cluster_id: {"label_html": str, "node_ids": [int]}}
    """
    nodes = {}   # type: Dict[int, dict]
    edges = []   # type: List[tuple]
    clusters = {}  # type: Dict[int, dict]

    # --- Parse node definitions ---
    # Nodes look like:  7 [labelType="html" label="..."];
    # Use a word boundary (\b) instead of ^ to handle single-line DOT content
    node_pattern = re.compile(
        r'(?:^|\s|;)(\d+)\s*\[labelType="html"\s+label="(.*?)"\s*\]\s*;',
        re.MULTILINE | re.DOTALL,
    )
    for m in node_pattern.finditer(dot_text):
        nid = int(m.group(1))
        raw_label = m.group(2)
        label_html = _unescape_dot_label(raw_label)
        name = _extract_operator_name(label_html)
        metrics = _extract_metrics_from_label(label_html)
        nodes[nid] = {"name": name, "metrics": metrics}

    # --- Parse edges ---
    edge_pattern = re.compile(
        r'(\d+)\s*->\s*(\d+)\s*(?:\[label="([^"]*)"\])?\s*;'
    )
    for m in edge_pattern.finditer(dot_text):
        child = int(m.group(1))
        parent = int(m.group(2))
        label = m.group(3)
        edges.append((child, parent, label))

    # --- Parse subgraph clusters ---
    cluster_pattern = re.compile(
        r'subgraph\s+cluster(\d+)\s*\{(.*?)\}',
        re.DOTALL,
    )
    for m in cluster_pattern.finditer(dot_text):
        cid = int(m.group(1))
        body = m.group(2)
        lbl_m = re.search(r'label="(.*?)"', body, re.DOTALL)
        label_html = _unescape_dot_label(lbl_m.group(1)) if lbl_m else ""
        inner_nodes = [int(x) for x in re.findall(r'(?:^|\s|;)(\d+)\s*\[', body, re.MULTILINE)]
        clusters[cid] = {"label_html": label_html, "node_ids": inner_nodes}

    return nodes, edges, clusters


def _parse_cluster_info(label_html: str) -> Tuple[str, str]:
    """Extract cluster/stage name and extra info (e.g. duration) from label HTML."""
    s = BeautifulSoup(label_html, "html.parser")
    b = s.find("b")
    name = b.get_text(strip=True) if b else ""
    spans = s.find_all("span")
    info_parts = []
    for sp in spans:
        if sp.find("b"):
            continue
        t = sp.get_text("\n", strip=True)
        if t:
            info_parts.append(t)
    return name, "\n".join(info_parts)


# ---------------------------------------------------------------------------
# Node metadata (plan-meta-data-N divs)
# ---------------------------------------------------------------------------

def extract_node_metadata(soup: BeautifulSoup, max_id: int) -> Dict[int, str]:
    metadata = {}
    for nid in range(max_id + 1):
        div = soup.find("div", id=f"plan-meta-data-{nid}")
        if div:
            metadata[nid] = div.get_text(strip=True)
    return metadata


# ---------------------------------------------------------------------------
# Execution summary
# ---------------------------------------------------------------------------

def extract_execution_summary(soup: BeautifulSoup) -> List[dict]:
    div = soup.find("div", id="text-execution-summary")
    if not div:
        return []
    pre = div.find("pre")
    if not pre:
        return []
    text = pre.get_text()
    lines = text.strip().splitlines()
    rows = []
    for line in lines:
        if line.startswith("Completed in") or line.startswith("Node") or line.startswith("---") or set(line.strip()) <= set("-+| "):
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 6:
            rows.append({
                "node": parts[0],
                "tasks": parts[1],
                "duration": parts[2],
                "rows": parts[3],
                "est_rows": parts[4],
                "peak_mem": parts[5],
            })
    return rows


def extract_completed_time(soup: BeautifulSoup) -> Optional[str]:
    div = soup.find("div", id="text-execution-summary")
    if not div:
        return None
    pre = div.find("pre")
    if not pre:
        return None
    text = pre.get_text()
    m = re.search(r"Completed in (\d+ ms)", text)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# IO Cache stats
# ---------------------------------------------------------------------------

def extract_io_cache(soup: BeautifulSoup) -> dict:
    table = soup.find("table", id="io-cache-execution-stats")
    if not table:
        return {}
    result = {}
    hits_td = table.find("td", id="cache-hits-cell")
    misses_td = table.find("td", id="cache-misses-cell")
    ratio_td = table.find("td", id="cache-hit-ratio-cell")
    if hits_td:
        result["cache_hits"] = hits_td.get_text(strip=True)
    if misses_td:
        result["cache_misses"] = misses_td.get_text(strip=True)
    if ratio_td:
        result["cache_hit_ratio"] = ratio_td.get_text(strip=True)
    return result


# ---------------------------------------------------------------------------
# Physical plan text
# ---------------------------------------------------------------------------

def extract_physical_plan(soup: BeautifulSoup) -> str:
    div = soup.find("div", id="physical-plan-details")
    if not div:
        return ""
    pre = div.find("pre")
    return pre.get_text() if pre else ""


# ---------------------------------------------------------------------------
# SQL / DataFrame properties
# ---------------------------------------------------------------------------

def extract_sql_properties(soup: BeautifulSoup) -> Dict[str, str]:
    div = soup.find("div", class_="sql-properties")
    if not div:
        return {}
    table = div.find("table")
    if not table:
        return {}
    props = {}
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            props[tds[0].get_text(strip=True)] = tds[1].get_text(strip=True)
    return props


# ---------------------------------------------------------------------------
# Build the unified operator list with topology
# ---------------------------------------------------------------------------

def build_operator_list(
    nodes: dict,
    edges: list,
    clusters: dict,
    node_metadata: dict,
) -> Tuple[List[dict], List[dict]]:
    """
    Build the final operators list and stages list.

    Returns (operators, stages).
    """
    node_to_cluster = {}  # type: Dict[int, int]
    for cid, cinfo in clusters.items():
        for nid in cinfo["node_ids"]:
            node_to_cluster[nid] = cid

    children_of = {}   # type: Dict[int, List[int]]
    parents_of = {}    # type: Dict[int, List[int]]
    edge_labels = {}   # type: Dict[Tuple[int, int], str]

    for child, parent, label in edges:
        children_of.setdefault(parent, []).append(child)
        parents_of.setdefault(child, []).append(parent)
        if label:
            edge_labels[(child, parent)] = label

    operators = []
    for nid in sorted(nodes.keys()):
        ninfo = nodes[nid]
        parents = parents_of.get(nid, [])
        children = children_of.get(nid, [])

        edge_rows = {}
        for pid in parents:
            lbl = edge_labels.get((nid, pid))
            if lbl:
                edge_rows[str(pid)] = lbl

        cid = node_to_cluster.get(nid)
        stage_name = None
        if cid is not None:
            stage_name, _ = _parse_cluster_info(clusters[cid]["label_html"])

        op = {
            "node_id": nid,
            "name": ninfo["name"],
            "stage": stage_name,
            "children": sorted(children),
            "parents": sorted(parents),
            "edge_rows_to_parent": edge_rows if edge_rows else None,
            "metadata": node_metadata.get(nid),
        }
        if ninfo["metrics"]:
            op["metrics"] = ninfo["metrics"]

        operators.append(op)

    stages = []
    for cid in sorted(clusters.keys()):
        cinfo = clusters[cid]
        name, info = _parse_cluster_info(cinfo["label_html"])
        stages.append({
            "cluster_id": cid,
            "name": name,
            "info": info,
            "node_ids": sorted(cinfo["node_ids"]),
        })

    return operators, stages


# ---------------------------------------------------------------------------
# Command metrics, scan metrics, and table paths extraction
# ---------------------------------------------------------------------------

_COMMAND_TYPE_MAP = {
    "merge": "merge",
    "delete": "delete",
    "write": "write",
    "update": "update",
    "insert": "insert",
}


def _classify_command(operator_name: str) -> str:
    lower = operator_name.lower()
    for keyword, ctype in _COMMAND_TYPE_MAP.items():
        if keyword in lower:
            return ctype
    return "unknown"


def extract_command_metrics(operators: List[dict]) -> List[dict]:
    """Extract metrics from Execute-command operators into structured dicts."""
    results = []
    for op in operators:
        name = op.get("name", "")
        if "Execute" not in name:
            continue
        metrics_list = op.get("metrics", [])
        if not metrics_list:
            continue
        metrics_dict = {m["metric"]: m["value"] for m in metrics_list}
        results.append({
            "command_type": _classify_command(name),
            "operator_name": name,
            "node_id": op["node_id"],
            "metrics": metrics_dict,
        })
    return results


def _classify_scan(operator_name: str) -> str:
    lower = operator_name.lower()
    if "parquet" in lower:
        return "parquet"
    if "existingrdd" in lower or "existing_rdd" in lower:
        return "existing_rdd"
    if "csv" in lower:
        return "csv"
    if "json" in lower:
        return "json"
    if "orc" in lower:
        return "orc"
    return "unknown"


def extract_scan_metrics(operators: List[dict]) -> List[dict]:
    """Extract metrics from scan operators (PhotonScan, Scan parquet, etc.)."""
    results = []
    for op in operators:
        name = op.get("name", "")
        if "Scan" not in name:
            continue
        metrics_list = op.get("metrics", [])
        if not metrics_list:
            continue
        metrics_dict = {m["metric"]: m["value"] for m in metrics_list}
        results.append({
            "operator_name": name,
            "node_id": op["node_id"],
            "scan_type": _classify_scan(name),
            "metrics": metrics_dict,
        })
    return results


_TABLE_PATH_PATTERN = re.compile(
    r'((?:dbfs|s3|s3a|abfss|wasbs|gs|gcs)://?[^\s\]\)]+)'
)
_TABLE_ID_PATTERN = re.compile(r'/tables/([0-9a-f\-]{36})')


def extract_table_paths(operators: List[dict], physical_plan: str) -> List[dict]:
    """Extract cloud storage paths from the physical plan text and associate with operators."""
    results = []
    seen = set()

    plan_lines = physical_plan.splitlines()

    for op in operators:
        name = op.get("name", "")
        metadata = op.get("metadata") or ""

        is_scan = "Scan" in name or "PhotonScan" in name
        is_write = "Execute" in name or "Write" in name or "OutputSpec" in name
        access_type = "read" if is_scan else ("write" if is_write else "unknown")

        for source in (metadata,):
            for m in _TABLE_PATH_PATTERN.finditer(source):
                path = m.group(1).rstrip(",;])")
                if path in seen:
                    continue
                seen.add(path)
                tid_m = _TABLE_ID_PATTERN.search(path)
                results.append({
                    "path": path,
                    "table_id": tid_m.group(1) if tid_m else None,
                    "access_type": access_type,
                    "operator_name": name,
                })

    for line in plan_lines:
        if "OutputSpec" in line or "Location:" in line or "TahoeFileIndex" in line:
            for m in _TABLE_PATH_PATTERN.finditer(line):
                path = m.group(1).rstrip(",;])")
                if path in seen:
                    continue
                seen.add(path)
                tid_m = _TABLE_ID_PATTERN.search(path)
                is_output = "OutputSpec" in line
                results.append({
                    "path": path,
                    "table_id": tid_m.group(1) if tid_m else None,
                    "access_type": "write" if is_output else "read",
                    "operator_name": None,
                })

    return results


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def write_json(
    path,               # type: str
    query_metadata,     # type: dict
    operators,          # type: List[dict]
    stages,             # type: List[dict]
    physical_plan,      # type: str
    execution_summary=None,   # type: Optional[List[dict]]
    io_cache=None,            # type: Optional[dict]
    sql_properties=None,      # type: Optional[dict]
    command_metrics=None,     # type: Optional[List[dict]]
    scan_metrics=None,        # type: Optional[List[dict]]
    table_paths=None,         # type: Optional[List[dict]]
):
    data = {
        "query_metadata": query_metadata,
        "operators": operators,
        "stages": stages,
        "physical_plan": physical_plan,
    }

    if execution_summary:
        data["execution_summary"] = execution_summary
    if io_cache:
        data["io_cache"] = io_cache
    if sql_properties:
        data["sql_properties"] = sql_properties
    if command_metrics:
        data["command_metrics"] = command_metrics[0] if len(command_metrics) == 1 else command_metrics
    if scan_metrics:
        data["scan_metrics"] = scan_metrics
    if table_paths:
        data["table_paths"] = table_paths

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  JSON written: {path}")


# ---------------------------------------------------------------------------
# README generation
# ---------------------------------------------------------------------------

def write_readme(
    path: str,
    query_name: str,
    query_metadata: dict,
    operators: list[dict],
    stages: list[dict],
    ops_with_metrics: int,
    execution_summary_count: int,
    sql_properties_count: int,
):
    ops_with_metrics_list = [op for op in operators if op.get("metrics")]
    metrics_list = "\n".join(
        f"  - **node {op['node_id']}** -- {op['name']} ({len(op['metrics'])} metrics)"
        for op in ops_with_metrics_list
    )

    stage_list = "\n".join(
        f"  - {s['name']}: {s['info'].splitlines()[0] if s['info'] else 'no info'} (nodes: {s['node_ids']})"
        for s in stages
    )

    content = f"""# {query_name} -- Parsed QPL Data

## Source
- **Query**: {query_metadata.get('title', query_name)}
- **Duration**: {query_metadata.get('duration', 'unknown')}
- **Submitted**: {query_metadata.get('submitted_time', 'unknown')}
- **Succeeded Jobs**: {query_metadata.get('succeeded_jobs', [])}

## Files

### `{query_name}.json` -- Full Query Plan with Metrics
The JSON contains the complete query plan structure with per-operator metrics inline. Key sections:
- **`query_metadata`**: title, duration, submitted time, job IDs, AQE versions
- **`operators`**: list of all {len(operators)} operators with:
  - `node_id`: unique ID within this plan
  - `name`: operator name with plan node number, e.g. "PhotonShuffledHashJoin (25)"
  - `stage`: which stage/codegen wrapper contains this operator (null if none)
  - `children`: list of child node IDs (data flows from children upward)
  - `parents`: list of parent node IDs
  - `edge_rows_to_parent`: row counts on edges (e.g. {{"6": "222"}} means 222 rows flow to parent node 6)
  - `metadata`: detailed plan node description from Spark
  - `metrics`: array of metric objects with `metric`, `value`, `is_experimental` (only present if operator has metrics)
- **`stages`**: list of {len(stages)} stage groupings (WholeStageCodegen, PhotonShuffleMapStage, etc.)
- **`physical_plan`**: full physical plan text from Spark UI
- **`execution_summary`**: per-operator duration, rows, peak memory from Spark's text summary
- **`io_cache`**: cache hit/miss statistics
- **`sql_properties`**: all {sql_properties_count} Spark configuration key/value pairs
- **`command_metrics`**: metrics from Execute-command operators (if any)
- **`scan_metrics`**: metrics from PhotonScan / Scan ExistingRDD operators (if any)
- **`table_paths`**: cloud storage paths with table IDs and access types

## Operators with Metrics ({ops_with_metrics} of {len(operators)})
{metrics_list}

## How to Read the Metrics

### Metric format: "total (min, med, max)"
Most metrics follow this pattern: `5.6 s (381 ms, 634 ms, 761 ms)`
- **total**: sum across ALL tasks in the stage
- **min**: smallest value among individual tasks
- **med**: median value among individual tasks
- **max**: largest value among individual tasks (often indicates a straggler)

### Key metrics for performance analysis
- **rows output**: number of rows produced by this operator
- **duration total**: wall-clock time spent in this operator across all tasks
- **spill size total**: data spilled to disk (indicates memory pressure)
- **peak memory total**: maximum memory used
- **data size total / shuffle bytes written**: amount of data shuffled
- **fetch wait time total**: time waiting for shuffle data (network/IO bottleneck)
- **scan time total**: time spent reading data from storage
- **number of partitions**: how many partitions the data is split into
- **partition data size**: size of data per partition (skew shows as large max vs small median)

## Stages
{stage_list}

## How to Use This Data

### For single-run analysis
1. Read the JSON to understand the DAG structure (which operators feed into which)
2. Check `execution_summary` for a quick overview of duration and row counts
3. Read the `metrics` array on operators with high duration or unexpected row counts
4. Check for spill, high shuffle sizes, or partition skew

### For comparing two runs
1. Read both JSONs to understand structural differences
2. Match operators by name (strip the plan node number in parentheses)
3. Compare `metrics` arrays for matched operators -- focus on rows, duration, shuffle sizes
4. Check `sql_properties` for Spark configuration changes between runs
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  README written: {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _get_query_name(html_path: str) -> str:
    stem = Path(html_path).stem
    m = re.search(r"Query\s*\d+", stem)
    return m.group(0).replace(" ", "_") if m else re.sub(r"[^\w]", "_", stem)[:40]


def _is_spark_ui_html(path: Path) -> bool:
    """Check if a file looks like a Spark UI query details HTML."""
    name = path.name.lower()
    return (
        path.suffix.lower() == ".html"
        and ("query" in name or "spark" in name or "details" in name)
    )


def _find_html_files(path: Path) -> List[Path]:
    """Find Spark UI HTML files in a directory (non-recursive, skips _files/ dirs)."""
    if path.is_file():
        return [path] if _is_spark_ui_html(path) else []
    results = []
    for f in sorted(path.iterdir()):
        if f.is_file() and _is_spark_ui_html(f):
            results.append(f)
    return results


def parse_single_html(html_path: str, output_base: Optional[Path] = None) -> str:
    """Parse one HTML file and write JSON + README. Returns the output folder path."""
    base = Path(html_path)
    query_name = _get_query_name(html_path)

    if output_base is None:
        output_base = base.parent
    out_dir = output_base / "parsed_output" / query_name
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Parsing: {html_path}")
    soup = load_html(html_path)

    print("  Extracting query metadata...")
    query_metadata = extract_query_metadata(soup)

    print("  Extracting DOT graph (nodes, edges, clusters)...")
    dot_text = extract_dot_content(soup)
    nodes, edges, clusters = parse_dot_graph(dot_text)
    print(f"    Nodes: {len(nodes)}, Edges: {len(edges)}, Clusters: {len(clusters)}")

    max_node_id = max(nodes.keys()) if nodes else 0
    print("  Extracting node metadata...")
    node_metadata = extract_node_metadata(soup, max_node_id)

    print("  Building operator list with topology...")
    operators, stages = build_operator_list(nodes, edges, clusters, node_metadata)
    ops_with_metrics = sum(1 for op in operators if op.get("metrics"))
    print(f"    Operators: {len(operators)}, with metrics: {ops_with_metrics}")

    print("  Extracting execution summary...")
    execution_summary = extract_execution_summary(soup)

    print("  Extracting physical plan...")
    physical_plan = extract_physical_plan(soup)

    print("  Extracting IO cache stats...")
    io_cache = extract_io_cache(soup)

    print("  Extracting SQL properties...")
    sql_properties = extract_sql_properties(soup)

    print("  Extracting command metrics...")
    command_metrics = extract_command_metrics(operators)

    print("  Extracting scan metrics...")
    scan_metrics = extract_scan_metrics(operators)

    print("  Extracting table paths...")
    table_paths = extract_table_paths(operators, physical_plan)

    json_path = str(out_dir / f"{query_name}.json")
    readme_path = str(out_dir / "README.md")

    print(f"  Writing outputs to: {out_dir}/")
    write_json(
        json_path, query_metadata, operators, stages, physical_plan,
        execution_summary=execution_summary,
        io_cache=io_cache,
        sql_properties=sql_properties,
        command_metrics=command_metrics,
        scan_metrics=scan_metrics,
        table_paths=table_paths,
    )
    write_readme(
        readme_path, query_name, query_metadata, operators, stages,
        ops_with_metrics, len(execution_summary), len(sql_properties),
    )

    print(f"  Done: {query_name} ({len(operators)} operators, {ops_with_metrics} with metrics)")
    return str(out_dir)


def main():
    force = "--force" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--force"]

    if not args:
        print("Usage: python parse_qpl.py <html_file_or_directory> [more_files...] [--force]")
        print()
        print("  Accepts one or more HTML files, or a directory containing them.")
        print("  Outputs go to parsed_output/{QueryName}/ next to the input.")
        print("  Already-parsed files are skipped unless --force is given.")
        sys.exit(1)

    html_files = []
    input_base = None
    for arg in args:
        p = Path(arg)
        if p.is_dir():
            input_base = p
            html_files.extend(_find_html_files(p))
        elif p.is_file():
            if input_base is None:
                input_base = p.parent
            html_files.append(p)
        else:
            print(f"Warning: '{arg}' not found, skipping")

    if not html_files:
        print("No Spark UI HTML files found.")
        sys.exit(1)

    print(f"Found {len(html_files)} HTML file(s):\n")

    parsed = []
    skipped = []

    for html_file in html_files:
        query_name = _get_query_name(str(html_file))
        out_dir = (input_base or html_file.parent) / "parsed_output" / query_name

        if out_dir.exists() and not force:
            print(f"  Skipping {query_name} (already parsed at {out_dir})")
            skipped.append(query_name)
            continue

        out_path = parse_single_html(str(html_file), output_base=input_base or html_file.parent)
        parsed.append(query_name)
        print()

    print("=" * 60)
    print(f"Summary: {len(parsed)} parsed, {len(skipped)} skipped")
    if parsed:
        print(f"  Parsed: {', '.join(parsed)}")
    if skipped:
        print(f"  Skipped (use --force to re-parse): {', '.join(skipped)}")
    print(f"  Output location: {(input_base or Path('.')).resolve()}/parsed_output/")


if __name__ == "__main__":
    main()
