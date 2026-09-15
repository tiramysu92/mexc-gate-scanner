#!/usr/bin/env python3
"""Query unresolved durable client ids. Never POST, clear a circuit or rearm."""
import argparse
import json
import os
from pathlib import Path
import sqlite3
from mexc_v3.model import D, Spec, Clock
from mexc_v3.mexc import Rest, parse_receipt
from mexc_v3.journal import dumps


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("database")
    p.add_argument("--output",required=True)
    a = p.parse_args()
    db = sqlite3.connect(Path(a.database).resolve().as_uri()+"?mode=ro",uri=True)
    db.row_factory = sqlite3.Row
    rows = db.execute("SELECT * FROM orders WHERE status='PREPARED'").fetchall()
    db.close()
    rest = Rest(Clock(),os.environ.get("MEXC_API_KEY",""),os.environ.get("MEXC_API_SECRET",""))
    report = {"query_only":True,"orders":[]}
    try:
        if rows:
            rest.sync_time()
        for row in rows:
            data = json.loads(row["intent"])
            data["quantity"],data["price"] = D(data["quantity"]),D(data["price"])
            spec = Spec(**data)
            try:
                receipt = parse_receipt(rest.order(spec),spec,"rest_reconciliation")
                report["orders"].append({"attempt_id":row["attempt_id"],"receipt":receipt})
            except Exception as exc:
                report["orders"].append({"client_id":spec.client_id,"unresolved":True,"error":type(exc).__name__})
        Path(a.output).write_text(dumps(report)+"\n")
    finally:
        rest.close()
    print(a.output)


if __name__ == "__main__":main()
