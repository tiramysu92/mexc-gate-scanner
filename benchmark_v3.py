#!/usr/bin/env python3
"""Local synthetic microbenchmark. Never interpreted as server LIVE latency."""
import json
import time
from mexc_v3.config import Policy
from mexc_v3.decision import Decisions
from mexc_v3.demo import fixture
from mexc_v3.model import D, ONE, Spec


def main():
    books,route,clock=fixture()
    engine=Decisions(books,Policy(),clock,lambda a:(ONE,ONE))
    plan=engine.evaluate(route,D(80))
    spec=Spec("benchmark",route.buy.symbol,"BUY",plan.quantity,plan.buy_limit,"leg1")
    report={"synthetic":True,"scope":"local warm-book decision CPU only; no HTTP or durable commit", "iterations":2000}
    for label,fn in (("full_sized_evaluation_us",lambda:engine.evaluate(route,D(80))),
                     ("unchanged_plan_guard_us",lambda:engine.guard_entry(plan,spec,D(80)))):
        before=time.perf_counter_ns()
        for _ in range(report["iterations"]):fn()
        report[label]=(time.perf_counter_ns()-before)/report["iterations"]/1000
    print(json.dumps(report,indent=2))


if __name__ == "__main__":main()
