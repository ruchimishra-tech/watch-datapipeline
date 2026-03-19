"""
validate_lineage.py
--------------------
Validates the generated OpenLineage NDJSON file and prints a structured
report that confirms the lineage is well-formed and ready for Marquez import.

Usage:
    python validate_lineage.py [--file path/to/file.ndjson]
"""

import json
import sys
import argparse
from pathlib import Path
from collections import defaultdict

INPUT_PATH = Path(__file__).parent.parent / "output" / "customer_orders_lineage.ndjson"

REQUIRED_EVENT_FIELDS = {"eventType", "eventTime", "run", "job", "producer", "schemaURL"}
REQUIRED_RUN_FIELDS   = {"runId"}
REQUIRED_JOB_FIELDS   = {"namespace", "name"}
REQUIRED_DS_FIELDS    = {"namespace", "name"}
VALID_EVENT_TYPES     = {"START", "COMPLETE", "FAIL", "ABORT", "OTHER"}


# ---------------------------------------------------------------------------
# ANSI colours (degrade gracefully on Windows)
# ---------------------------------------------------------------------------
def _c(code, text):
    try:
        return f"\033[{code}m{text}\033[0m"
    except Exception:
        return text

OK   = lambda t: _c("32", t)
WARN = lambda t: _c("33", t)
ERR  = lambda t: _c("31", t)
BOLD = lambda t: _c("1",  t)
DIM  = lambda t: _c("2",  t)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------
def validate_event(event: dict, idx: int) -> list[str]:
    errors = []

    # Required top-level fields
    for f in REQUIRED_EVENT_FIELDS:
        if f not in event:
            errors.append(f"Event[{idx}]: missing required field '{f}'")

    # eventType
    et = event.get("eventType", "")
    if et not in VALID_EVENT_TYPES:
        errors.append(f"Event[{idx}]: invalid eventType '{et}' (must be one of {VALID_EVENT_TYPES})")

    # run
    run = event.get("run", {})
    for f in REQUIRED_RUN_FIELDS:
        if f not in run:
            errors.append(f"Event[{idx}]: run missing field '{f}'")

    # job
    job = event.get("job", {})
    for f in REQUIRED_JOB_FIELDS:
        if f not in job:
            errors.append(f"Event[{idx}]: job missing field '{f}'")

    # datasets
    for role in ("inputs", "outputs"):
        for ds_idx, ds in enumerate(event.get(role, [])):
            for f in REQUIRED_DS_FIELDS:
                if f not in ds:
                    errors.append(
                        f"Event[{idx}] {role}[{ds_idx}]: missing required field '{f}'"
                    )

    return errors


def check_column_lineage(outputs: list[dict]) -> list[str]:
    warnings = []
    for ds in outputs:
        facets = ds.get("facets", {})
        schema_fields = {f["name"] for f in facets.get("schema", {}).get("fields", [])}
        col_lineage   = facets.get("columnLineage", {}).get("fields", {})
        if not col_lineage:
            warnings.append(
                f"  Dataset '{ds['name']}': no columnLineage facet — "
                "column-level graph will not be available in Marquez"
            )
            continue
        # Check every schema field has a lineage entry
        for field in schema_fields:
            if field not in col_lineage:
                warnings.append(
                    f"  Dataset '{ds['name']}': column '{field}' present in schema "
                    "but has no columnLineage entry"
                )
    return warnings


def collect_dataset_summary(events: list[dict]) -> dict:
    summary = {"inputs": defaultdict(set), "outputs": defaultdict(set)}
    for event in events:
        for role in ("inputs", "outputs"):
            for ds in event.get(role, []):
                key = f"{ds.get('namespace')}/{ds.get('name')}"
                schema = ds.get("facets", {}).get("schema", {}).get("fields", [])
                summary[role][key].update(f["name"] for f in schema)
    return summary


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------
def print_report(events: list[dict], all_errors: list[str], col_warnings: list[str]):
    sep = "─" * 70

    print()
    print(BOLD("=" * 70))
    print(BOLD("  OpenLineage Lineage File Validation Report"))
    print(BOLD("=" * 70))

    # ---- Events summary ---------------------------------------------------
    print()
    print(BOLD("Events"))
    print(sep)
    for i, event in enumerate(events):
        job  = event.get("job", {})
        run  = event.get("run", {})
        et   = event.get("eventType", "UNKNOWN")
        colour = OK if et in ("COMPLETE", "START") else WARN
        print(
            f"  [{i}] {colour(et):<20}"
            f"  job={job.get('namespace')}/{job.get('name')}"
        )
        print(f"       runId={run.get('runId')}")
        print(f"       inputs={len(event.get('inputs',[]))}  outputs={len(event.get('outputs',[]))}")

    # ---- Dataset summary --------------------------------------------------
    ds_summary = collect_dataset_summary(events)

    print()
    print(BOLD("Input Datasets"))
    print(sep)
    for ds_key, fields in sorted(ds_summary["inputs"].items()):
        print(f"  {OK('IN')}  {ds_key}")
        print(f"       fields ({len(fields)}): {', '.join(sorted(fields))}")

    print()
    print(BOLD("Output Datasets"))
    print(sep)
    for ds_key, fields in sorted(ds_summary["outputs"].items()):
        print(f"  {OK('OUT')} {ds_key}")
        print(f"       fields ({len(fields)}): {', '.join(sorted(fields))}")

    # ---- Column lineage summary -------------------------------------------
    print()
    print(BOLD("Column Lineage"))
    print(sep)
    complete_events = [e for e in events if e.get("eventType") == "COMPLETE"]
    for event in complete_events:
        for ds in event.get("outputs", []):
            col_lineage = ds.get("facets", {}).get("columnLineage", {}).get("fields", {})
            if not col_lineage:
                print(f"  {WARN('WARN')} {ds.get('name')}: no column lineage")
                continue
            print(f"  {OK('OK')}   {ds.get('name')} — {len(col_lineage)} columns mapped")
            for col, defn in col_lineage.items():
                input_fields = defn.get("inputFields", [])
                if input_fields:
                    sources = ", ".join(
                        f"{f['name'].split('.')[-1]}.{f['field']}"
                        for f in input_fields
                    )
                    print(f"        {col:<28} <- {sources}")
                else:
                    t = defn.get("transformationType", "CONSTANT")
                    print(f"        {col:<28} <- {DIM(t)}")

    # ---- Job facets -------------------------------------------------------
    print()
    print(BOLD("Job Facets"))
    print(sep)
    job_facets = events[0].get("job", {}).get("facets", {})
    for facet_name, facet_val in job_facets.items():
        print(f"  {OK('OK')}   {facet_name}")
        if facet_name == "sql":
            preview = facet_val.get("query", "")[:80].replace("\n", " ")
            print(f"       query preview: {DIM(preview)}...")

    # ---- Validation errors -----------------------------------------------
    print()
    print(BOLD("Validation"))
    print(sep)
    if all_errors:
        for err in all_errors:
            print(f"  {ERR('FAIL')} {err}")
    else:
        print(f"  {OK('PASS')} All required fields present and valid")

    if col_warnings:
        print()
        print(BOLD("Column Lineage Warnings"))
        print(sep)
        for w in col_warnings:
            print(f"  {WARN('WARN')} {w}")

    # ---- Marquez import instructions -------------------------------------
    print()
    print(BOLD("Marquez Import"))
    print(sep)
    ndjson_path = INPUT_PATH
    print("  POST each line to your Marquez instance:")
    print()
    print(DIM("  while IFS= read -r line; do"))
    print(DIM('    curl -s -X POST http://localhost:5000/api/v1/lineage \\'))
    print(DIM('      -H "Content-Type: application/json" \\'))
    print(DIM(f'      -d "$line"; echo'))
    print(DIM(f"  done < {ndjson_path}"))
    print()
    print("  Or import via Marquez Web UI:")
    print("    http://localhost:3000")

    # ---- Final verdict ---------------------------------------------------
    print()
    print(BOLD("=" * 70))
    if all_errors:
        print(ERR(f"  RESULT: FAILED — {len(all_errors)} error(s) found"))
    else:
        print(OK(f"  RESULT: PASSED — lineage file is valid and ready for Marquez"))
    print(BOLD("=" * 70))
    print()

    return len(all_errors)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Validate OpenLineage NDJSON file")
    parser.add_argument("--file", type=Path, default=INPUT_PATH,
                        help="Path to the NDJSON lineage file")
    args = parser.parse_args()

    path = args.file
    if not path.exists():
        print(ERR(f"File not found: {path}"))
        print("Run generate_lineage.py first.")
        sys.exit(1)

    events = []
    parse_errors = []
    with open(path) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as e:
                parse_errors.append(f"Line {line_no}: JSON parse error — {e}")

    if parse_errors:
        for e in parse_errors:
            print(ERR(e))
        sys.exit(1)

    print(f"Loaded {len(events)} event(s) from {path}")

    all_errors = []
    for i, event in enumerate(events):
        all_errors.extend(validate_event(event, i))

    complete_events = [e for e in events if e.get("eventType") == "COMPLETE"]
    col_warnings = []
    for event in complete_events:
        col_warnings.extend(check_column_lineage(event.get("outputs", [])))

    error_count = print_report(events, all_errors, col_warnings)
    sys.exit(0 if error_count == 0 else 1)


if __name__ == "__main__":
    main()
