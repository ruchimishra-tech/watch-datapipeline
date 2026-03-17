"""
Push all dashboards in dashboards/ to a running Grafana instance.

Usage:
    python cli/push_dashboards.py
    python cli/push_dashboards.py --url http://grafana:3000 --user admin --password secret

Environment variables (override flags):
    GRAFANA_URL, GRAFANA_USER, GRAFANA_PASSWORD
"""
import argparse
import json
import os
import sys
from pathlib import Path

import requests


def push(url: str, user: str, password: str, dashboard_dir: Path) -> int:
    session = requests.Session()
    session.auth = (user, password)
    session.headers["Content-Type"] = "application/json"

    files = sorted(dashboard_dir.glob("*.json"))
    if not files:
        print("No dashboard JSON files found.")
        return 0

    failures = 0
    for path in files:
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        payload = {"dashboard": dashboard, "overwrite": True, "folderId": 0}
        try:
            r = session.post(f"{url}/api/dashboards/import", json=payload, timeout=10)
            if r.status_code == 200:
                uid = r.json().get("uid", "?")
                print(f"  OK      {path.name}  ->  uid={uid}")
            else:
                print(f"  FAIL    {path.name}  ->  HTTP {r.status_code}: {r.text[:200]}")
                failures += 1
        except requests.RequestException as exc:
            print(f"  ERROR   {path.name}  ->  {exc}")
            failures += 1

    print()
    if failures:
        print(f"Push FAILED - {failures}/{len(files)} dashboard(s) not uploaded.")
        return 1
    print(f"Push PASSED - {len(files)} dashboard(s) uploaded successfully.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Push Grafana dashboards")
    parser.add_argument("--url",      default=os.getenv("GRAFANA_URL",      "http://localhost:3000"))
    parser.add_argument("--user",     default=os.getenv("GRAFANA_USER",     "admin"))
    parser.add_argument("--password", default=os.getenv("GRAFANA_PASSWORD", "admin"))
    args = parser.parse_args()

    repo_root = Path(__file__).parents[1]
    dashboard_dir = repo_root / "dashboards"

    print(f"Pushing dashboards from {dashboard_dir} -> {args.url}")
    sys.exit(push(args.url, args.user, args.password, dashboard_dir))


if __name__ == "__main__":
    main()
