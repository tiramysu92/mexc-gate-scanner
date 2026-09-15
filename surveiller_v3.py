#!/usr/bin/env python3
"""Read-only observation. Closing this process never changes LIVE or STOP."""
import argparse
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import time
from urllib.request import urlopen


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url",default="http://127.0.0.1:8083")
    p.add_argument("--minutes",type=float,default=10)
    p.add_argument("--interval",type=float,default=5)
    p.add_argument("--output-dir",default="data_v3")
    a = p.parse_args()
    if not 0 < a.minutes <= 1440 or a.interval < 1:
        p.error("Use 0 < minutes <= 1440 and interval >= 1")
    out = Path(a.output_dir)
    out.mkdir(parents=True,exist_ok=True)
    now = datetime.now(timezone.utc)
    path = out/("surveillance_v3_"+now.strftime("%Y%m%d_%H%M%S_%f")+".json.gz")
    report = {"started_utc":now.isoformat(),"samples":[],"errors":[],"last_status":None}
    began = time.monotonic()
    baseline = None
    try:
        while time.monotonic()-began < a.minutes*60:
            start = time.monotonic()
            try:
                with urlopen(a.url.rstrip("/")+"/api/status",timeout=8) as r:
                    s = json.load(r)
                flow = s["flow"]
                baseline = flow["reconnections"] if baseline is None else baseline
                sample = {"elapsed_s":round(time.monotonic()-began,3),"version":s["version"],
                    "depth":s["depth"],"flow":flow,"clock":s["clock"],"live":s["live"],
                    "funnel":s["funnel"]["counts"],"policy_comparison":{
                        k:{"counts":v["counts"],"reasons":v["reasons"]} for k,v in s["policy_comparison"].items()}}
                report["samples"].append(sample)
                report["last_status"] = s
                print(f"{sample['elapsed_s']:7.1f}s | WS {flow['connected']}/{flow['total']} | "
                    f"Depth {s['depth']['ready']}/{s['depth']['total']} | rattrapages {flow['catchups']} | "
                    f"attente {flow['queue_age_ms']} ms | reconnexions +{flow['reconnections']-baseline}",flush=True)
            except Exception as exc:
                report["errors"].append({"elapsed_s":time.monotonic()-began,"error":type(exc).__name__})
            time.sleep(max(0,min(a.interval-(time.monotonic()-start),a.minutes*60-(time.monotonic()-began))))
    except KeyboardInterrupt:
        report["interrupted"] = True
    finally:
        report["elapsed_s"] = time.monotonic()-began
        with gzip.open(path,"wt",encoding="utf-8") as f:
            json.dump(report,f,allow_nan=False)
        print("Rapport :",path)


if __name__ == "__main__":main()
