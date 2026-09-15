"""Serialized two-leg state machine. There is no unconditional sell-all path."""
from dataclasses import replace
import threading
import time
import uuid
from .model import D, ZERO, ONE, Spec, Uncertain, GuardRejected


class Coordinator:
    def __init__(self, decisions, adapter, journal, policy, clock, stop_requested=lambda:False):
        self.decisions,self.adapter,self.journal = decisions,adapter,journal
        self.policy,self.clock,self.stop_requested = policy,clock,stop_requested
        self.lock = threading.Lock()
        self.busy = False

    def entry_block(self, own_attempt=False):
        totals = self.journal.totals()
        if self.stop_requested():
            return "stop_file"
        if totals["circuit"]:
            return "circuit:"+str(totals["circuit"])
        if totals["open_exposures"] and not own_attempt:
            return "unresolved_attempt"
        if totals["buys"] >= self.policy.max_buys:
            return "buy_quota"
        if totals["unwinds"]:
            return "first_unwind"
        if D(totals["pnl"]) <= -D(self.policy.loss_stop_usdt):
            return "loss_budget"
        return None

    def execute(self, plan):
        if not plan.eligible:
            return {"status":"rejected","reasons":plan.reasons}
        if not self.lock.acquire(blocking=False):
            return {"status":"busy"}
        self.busy = True
        attempt = uuid.uuid4().hex
        specs,records = [],[]
        begun = False
        acquired = ZERO
        unwind_count = 0
        try:
            reason = self.entry_block()
            if reason:
                return {"status":"blocked","reason":reason}
            baseline = self.adapter.balances()
            available = D(baseline.get(plan.route.buy.quote,0))
            # Inventory ownership is captured once, before the first request.
            buy = Spec("v3b"+attempt[:25],plan.route.buy.symbol,"BUY",plan.quantity,plan.buy_limit,"leg1",route_id=plan.route.id)
            evidence = self.decisions.evidence(plan.route)
            prepared = time.perf_counter_ns()
            self.journal.begin(attempt,plan,buy,self.clock.ms(),evidence=evidence)
            prepare_ms = (time.perf_counter_ns()-prepared)/1e6
            begun = True
            specs.append(buy)
            def buy_guard():
                reason = self.entry_block(own_attempt=True)
                if reason:
                    raise GuardRejected(reason)
                self.decisions.guard_entry(plan,buy,available)
            try:
                first = self.adapter.submit(buy,buy_guard).validate(buy)
                first.timings["prepare_journal_ms"] = prepare_ms
            except GuardRejected as exc:
                self.journal.undispatched(buy.client_id,str(exc))
                self.journal.transition(attempt,status="aborted",values={"reason":str(exc),"pnl":"0"})
                return {"status":"aborted","reason":str(exc)}
            records.append(first)
            acquired = first.executed
            if not acquired:
                self.journal.transition(attempt,first,status="no_fill",values={"pnl":"0"})
                return {"status":"no_fill"}
            base = plan.route.buy.base
            reserve = max(D(self.policy.fee_reserve),plan.route.buy.fee,plan.route.sell.fee)
            quantity = (acquired-D(first.fees.get(base,0))) if first.fees_known else acquired*(ONE-reserve)
            remaining = quantity
            deadline = self.clock.mono()+self.policy.exit_retry_ms/1000
            pending_receipt = first
            for index in range(self.policy.maximum_exit_orders):
                if index:
                    # A confirmed terminal result, exact commissions and a fresh
                    # account balance are required before another SELL.
                    previous,prev_spec = records[-1],specs[-1]
                    charged_base = D(previous.fees.get(base,0)) if previous.fees_known else ZERO
                    independent = self.adapter.reconcile(prev_spec).validate(prev_spec)
                    if (independent.order_id,independent.status,independent.executed,independent.quote) != (
                            previous.order_id,previous.status,previous.executed,previous.quote):
                        raise Uncertain("WS/REST terminal mismatch; no second exit")
                    previous = self.adapter.commissions(prev_spec,previous)
                    if not previous.fees_known:
                        raise Uncertain("Partial exit commission unresolved")
                    records[-1] = pending_receipt = previous
                    remaining -= D(previous.fees.get(base,0))-charged_base
                    if remaining < 0:
                        raise Uncertain("Exit base fee exceeded remaining inventory")
                    account = self.adapter.refresh_account()
                    own_free = max(ZERO,D(account.get(base,0))-D(baseline.get(base,0)))
                    remaining = min(remaining,own_free)
                    if self.clock.mono() >= deadline:
                        break
                candidates = self.decisions.exits(plan.route,remaining)
                if not candidates:
                    break
                chosen = candidates[0]
                purpose = chosen.purpose if index == 0 else "recovery"
                if purpose != "leg2":
                    unwind_count = 1
                sell = Spec("v3s"+str(index)+attempt[:24],chosen.market.symbol,"SELL",
                            chosen.quantity,chosen.limit,purpose)
                evidence = self.decisions.evidence(plan.route)
                prepared = time.perf_counter_ns()
                self.journal.transition(attempt,pending_receipt,sell,"exit_pending",
                    {"buys":1,"acquired":str(acquired),"baseline_inventory":str(baseline.get(base,0)),
                     "remaining_reserved":str(remaining),"unwinds":unwind_count,
                     "exit_evidence_"+str(index):evidence,"exit_choices_"+str(index):candidates})
                prepare_ms = (time.perf_counter_ns()-prepared)/1e6
                pending_receipt = None
                specs.append(sell)
                def exit_guard():
                    if index and self.clock.mono() >= deadline:
                        raise GuardRejected("exit_retry_deadline")
                    self.decisions.guard_exit(sell,chosen.market)
                try:
                    result = self.adapter.submit(sell,exit_guard).validate(sell)
                    result.timings["prepare_journal_ms"] = prepare_ms
                except GuardRejected as exc:
                    self.journal.undispatched(sell.client_id,str(exc))
                    # No uncertain order, but leave the inventory explicit.
                    self.journal.transition(attempt,status="exposure",values={"reason":str(exc),"buys":1})
                    break
                records.append(result)
                pending_receipt = result
                fee_base = D(result.fees.get(base,0)) if result.fees_known else ZERO
                remaining -= result.executed+fee_base
                if remaining < 0:
                    raise Uncertain("Exit consumed more than owned inventory")
                if remaining <= 0:
                    break
            # No network commission request is placed between BUY and first SELL.
            if pending_receipt:
                self.journal.transition(attempt,pending_receipt,status="accounting_pending",values={"buys":1,"unwinds":unwind_count})
            final = self.settle(attempt,plan,specs,records,unwind_count)
            if final["status"] in ("exposure","accounting_pending"):
                self.journal.stop(final["status"])
            elif unwind_count:
                self.journal.stop("first_unwind")
            return final
        except Uncertain as exc:
            if begun:
                self.journal.transition(attempt,status="reconciliation_required",
                    values={"reason":str(exc),"buys":int(acquired>0),"acquired":str(acquired),"unwinds":unwind_count})
                self.journal.stop("order_uncertain")
            return {"status":"reconciliation_required","reason":str(exc)}
        except Exception as exc:
            # Includes journal failure: no further dispatch after a failed commit.
            if begun:
                try:
                    self.journal.transition(attempt,status="reconciliation_required",
                        values={"reason":type(exc).__name__,"buys":int(acquired>0)})
                    self.journal.stop("execution_error")
                except Exception:
                    pass
            return {"status":"error","reason":type(exc).__name__}
        finally:
            self.busy = False
            self.lock.release()
            self.adapter.release(specs)

    def settle(self, attempt, plan, specs, records, unwinds):
        by_id = {x.client_id:x for x in specs}
        effects = {}
        receipts = []
        try:
            for receipt in records:
                spec = by_id[receipt.client_id]
                receipt = self.adapter.commissions(spec,receipt)
                if not receipt.fees_known:
                    raise Uncertain("Missing final commissions")
                market = plan.route.buy if spec.symbol == plan.route.buy.symbol else plan.route.sell
                sign = ONE if spec.side == "BUY" else -ONE
                effects[market.base] = effects.get(market.base,ZERO)+sign*receipt.executed
                effects[market.quote] = effects.get(market.quote,ZERO)-sign*receipt.quote
                for asset,fee in receipt.fees.items():
                    effects[asset] = effects.get(asset,ZERO)-D(fee)
                receipts.append(receipt)
        except Exception as exc:
            self.journal.transition(attempt,status="accounting_pending",values={"reason":type(exc).__name__,"buys":1})
            return {"status":"accounting_pending"}
        base = plan.route.buy.base
        remaining = effects.get(base,ZERO)
        if remaining < 0:
            raise Uncertain("Final accounting crossed owned inventory")
        cash = ZERO
        for asset,amount in effects.items():
            if asset == base:
                continue
            bid,ask = self.decisions.valuation(asset)
            if bid <= 0 or ask <= 0:
                self.journal.transition(attempt,status="accounting_pending",values={"effects":effects,"buys":1})
                return {"status":"accounting_pending"}
            cash += amount*(ask if amount < 0 else bid)
        # Residual inventory is never called realized profit. Allocate its cost
        # proportionally; dust remains recorded and excluded from spendable cash.
        first = receipts[0]
        start_mark = plan.marks[0]
        cost = (first.quote+D(first.fees.get(plan.route.buy.quote,0)))*start_mark
        acquired = first.executed-D(first.fees.get(base,0))
        residual_cost = cost*remaining/acquired if acquired else ZERO
        book = self.decisions.books.get(plan.route.buy.symbol)
        mark = D(book.bids[0][0])*self.decisions.valuation(plan.route.buy.quote)[0] if book else ZERO
        residual_value = remaining*mark
        status = "exposure" if remaining and (mark <= 0 or residual_value > D(self.policy.dust_usdt)) else (
            "unwind" if unwinds else "dust" if remaining else "completed")
        values = {"effects":effects,"remaining":remaining,"residual_cost_usdt":residual_cost,
            "residual_mark_usdt":mark,"residual_value_usdt":residual_value,
            "pnl":cash+residual_cost,"economic_pnl":cash+residual_value,
            "receipts":receipts,"buys":1,"unwinds":unwinds}
        self.journal.transition(attempt,status=status,values=values)
        return {"status":status,"attempt":attempt,**values}
