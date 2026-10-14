"""Sync sql/questions/*.sql into Metabase native cards + rebuild the dashboard.

One command forever: card SQL always matches the committed files, so the
dashboard can never drift from the repo. Login via env (documented in
.env.example, values stay out of git):
  METABASE_URL, METABASE_USER, METABASE_PASSWORD, METABASE_DB_ID
Usage: python scripts/refresh_dashboard.py [--dashboard-id 2]
"""

import os
import sys

import httpx

DISP = {
    "kpi_match_rate": "scalar",
    "kpi_volume_rupees": "scalar",
    "kpi_at_risk_rupees": "scalar",
    "dlq_depth": "scalar",
    "match_rate": "pie",
    "daily_match_rate": "line",
    "daily_trend": "line",
    "volume_by_instrument": "bar",
    "mismatch_by_instrument": "bar",
    "exceptions_by_merchant": "table",
    "top_mismatches": "table",
}
ORDER = [
    "kpi_match_rate",
    "kpi_volume_rupees",
    "kpi_at_risk_rupees",
    "dlq_depth",
    "match_rate",
    "daily_match_rate",
    "daily_trend",
    "volume_by_instrument",
    "mismatch_by_instrument",
    "exceptions_by_merchant",
    "top_mismatches",
]


def _dashboard_id() -> int:
    if "--dashboard-id" in sys.argv:
        return int(sys.argv[sys.argv.index("--dashboard-id") + 1])
    return 2


def main() -> None:
    base = os.environ.get("METABASE_URL", "http://127.0.0.1:3001")
    db = int(os.environ.get("METABASE_DB_ID", "2"))
    dash = _dashboard_id()
    s = httpx.Client(timeout=60)
    tok = s.post(
        f"{base}/api/session",
        json={"username": os.environ["METABASE_USER"], "password": os.environ["METABASE_PASSWORD"]},
    ).json()["id"]
    h = {"X-Metabase-Session": tok}
    existing = {
        c["name"]: c["id"]
        for c in s.get(f"{base}/api/search", headers=h, params={"models": "card"}).json()["data"]
    }
    ids = {}
    for n in sorted(DISP):
        with open(f"sql/questions/{n}.sql", encoding="utf-8") as fh:
            sql = fh.read()
        # Starburst JDBC rejects a trailing ';' (Trino REST accepts it).
        sql = sql.rstrip()
        if sql.endswith(";"):
            sql = sql[:-1]
        body = {
            "name": n,
            "dataset_query": {"database": db, "type": "native", "native": {"query": sql}},
            "display": DISP[n],
            "visualization_settings": {},
        }
        if n in existing:
            cid = existing[n]
            s.put(f"{base}/api/card/{cid}", headers=h, json=body, timeout=300)
        else:
            cid = s.post(f"{base}/api/card", headers=h, json=body, timeout=300).json()["id"]
        back = s.get(f"{base}/api/card/{cid}", headers=h, timeout=120).json()
        assert back["dataset_query"]["native"]["query"] == sql, f"{n} drift!"
        ids[n] = cid
        print(f"{n} -> card {cid}")
    cards = [
        {
            "id": -i,
            "card_id": ids[n],
            "row": (i - 1) * 4,
            "col": 0,
            "size_x": 24,
            "size_y": 4,
            "series": [],
            "visualization_settings": {},
            "parameter_mappings": [],
        }
        for i, n in enumerate(ORDER, 1)
    ]
    r = s.put(f"{base}/api/dashboard/{dash}", headers=h, json={"dashcards": cards}, timeout=600)
    r.raise_for_status()
    print(f"dashboard {dash}: {len(r.json()['dashcards'])} cards")


if __name__ == "__main__":
    main()
