import json
from pathlib import Path
import tempfile
import unittest
from dataclasses import replace
from unittest.mock import patch
from mexc_v3.books import Delta
from mexc_v3.config import Policy
from mexc_v3.decision import Decisions, freshness
from mexc_v3.demo import fixture
from mexc_v3.execution import Coordinator
from mexc_v3.journal import Journal
from mexc_v3.model import D, ONE, ZERO, Spec, Receipt, GuardRejected, Uncertain
from mexc_v3.simulation import PaperAdapter


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.books,self.route,self.clock = fixture()
        self.policy = Policy()
        self.engine = Decisions(self.books,self.policy,self.clock,lambda a:(ONE,ONE))
        self.j = Journal(Path(self.tmp.name)/"paper.db")
        self.addCleanup(lambda:self.j.close())
        self.adapter = PaperAdapter(self.books,{m.symbol:m for m in (self.route.buy,self.route.sell)},self.clock)
        self.adapter.fees_at_confirmation = True
        self.co = Coordinator(self.engine,self.adapter,self.j,self.policy,self.clock)

    def plan(self):
        return self.engine.evaluate(self.route,D(80))

    def test_full_chain_records_exact_fee_adjusted_cash_flows(self):
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"completed")
        self.assertEqual(result["remaining"],ZERO)
        self.assertEqual(result["pnl"],result["effects"]["USDT"]+result["effects"]["USD1"])
        self.assertEqual(len(self.adapter.sent),2)
        self.assertLessEqual(-result["effects"]["USDT"],D(20))

    def test_delayed_commissions_reserve_inventory_and_record_dust_cost(self):
        self.adapter.fees_at_confirmation = False
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"dust")
        self.assertGreater(result["remaining"],ZERO)
        self.assertLessEqual(result["residual_value_usdt"],D(self.policy.dust_usdt))
        self.assertEqual(result["pnl"],result["effects"]["USDT"]+result["effects"]["USD1"]+result["residual_cost_usdt"])
        self.assertEqual(self.adapter.sent[1].quantity,D("19.92"))

    def test_sized_price_limit_never_exceeds_budget_when_book_walks(self):
        s = self.books.slots[self.route.buy.symbol]
        with s.lock:
            s.asks = {1.0:2,1.01:3,1.02:100}
            s.best_ask = 1
            s.snapshot = None
        plan = self.plan()
        self.assertLessEqual(plan.quantity*plan.buy_limit*D("1.002"),D(20))

    def test_affordable_size_uses_balance_not_configured_capital(self):
        plan = self.engine.evaluate(self.route,D("12.34"))
        self.assertLessEqual(plan.cost_usdt,D("12.34"))
        self.assertGreater(plan.cost_usdt,D(10))

    def test_five_old_exchange_cases_remain_rejected(self):
        original = self.books.get(self.route.sell.symbol)
        for lag in (123896,181297,539617,527939,526057):
            with self.subTest(lag=lag):
                old = replace(original,exchange_ms=self.clock.ms()-lag)
                self.assertIn("exchange_age",freshness(old,self.clock.ms(),self.clock.ms(),50,100))

    def test_fresh_receive_does_not_rejuvenate_exchange_time(self):
        book = replace(self.books.get(self.route.sell.symbol),received_ms=self.clock.ms(),exchange_ms=self.clock.ms()-5000)
        self.assertIn("exchange_age",freshness(book,self.clock.ms(),self.clock.ms(),50,100))

    def test_unrelated_unready_book_does_not_block_route(self):
        from mexc_v3.books import Slot
        self.books.slots["OTHERUSDT"] = Slot("OTHERUSDT")
        self.assertTrue(self.plan().eligible)

    def test_static_headroom_is_separately_measurable(self):
        self.clock.on_advance = None
        self.clock.sleep(.02)
        strict = self.plan()
        flexible = Decisions(self.books,replace(self.policy,static_headroom=False),self.clock,lambda a:(ONE,ONE)).evaluate(self.route,D(80))
        self.assertIn("static_exit_local_headroom",strict.reasons)
        self.assertTrue(flexible.eligible)
        self.assertEqual(strict.grade,flexible.grade)
        self.assertEqual(strict.quantity,flexible.quantity)

    def test_selling_leg_does_not_require_positive_entry_profit(self):
        s = self.books.slots[self.route.sell.symbol]
        with s.lock:
            s.bids = {.999:10000}
            s.best_bid = .999
            s.snapshot = None
        choices = self.engine.exits(self.route,D(19))
        self.assertEqual(choices[0].market.symbol,self.route.sell.symbol)
        self.assertLess(choices[0].net_usdt,D(19))

    def test_entry_guard_still_requires_positive_edge(self):
        plan = self.plan()
        spec = Spec("x",self.route.buy.symbol,"BUY",plan.quantity,plan.buy_limit,"leg1")
        s = self.books.slots[self.route.sell.symbol]
        self.books.delta(Delta(self.route.sell.symbol,s.version+1,s.version+1,
            self.clock.ms()-5,self.clock.ms()-2,((1.02,0),(.98,10000)),()),1)
        with self.assertRaises(GuardRejected):
            self.engine.guard_entry(plan,spec,D(80))

    def test_cached_validation_still_checks_age_cash_and_feed_state(self):
        plan = self.plan()
        spec = Spec("x",self.route.buy.symbol,"BUY",plan.quantity,plan.buy_limit,"leg1")
        self.engine.guard_entry(plan,spec,D(80))
        self.assertEqual(self.engine.validation_reuses,1)
        with self.assertRaises(GuardRejected):self.engine.guard_entry(plan,spec,D(1))
        self.books.catching_up([self.route.sell.symbol],True)
        with self.assertRaises(GuardRejected):self.engine.guard_entry(plan,spec,D(80))
        self.books.catching_up([self.route.sell.symbol],False)
        self.clock.on_advance = None
        self.clock.sleep(.03)
        with self.assertRaises(GuardRejected):self.engine.guard_entry(plan,spec,D(80))

    def test_changed_policy_invalidates_cached_grade(self):
        plan = self.plan()
        spec = Spec("x",self.route.buy.symbol,"BUY",plan.quantity,plan.buy_limit,"leg1")
        self.engine.policy = replace(self.policy,min_edge=".05",grade_a_edge=".06")
        with self.assertRaises(GuardRejected):self.engine.guard_entry(plan,spec,D(80))

    def test_canceled_buy_never_submits_sell(self):
        self.adapter.faults.append("cancel")
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"no_fill")
        self.assertEqual(len(self.adapter.sent),1)
        self.assertEqual(self.j.totals()["buys"],0)

    def test_buy_timeout_stops_and_never_reposts(self):
        self.adapter.faults.append("unknown")
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"reconciliation_required")
        self.assertEqual(len(self.adapter.sent),1)
        self.assertEqual(self.j.totals()["circuit"],"order_uncertain")
        self.assertEqual(self.co.execute(self.plan())["status"],"blocked")

    def test_cto_sequence_stale_leg2_uses_available_return_book(self):
        original = self.clock.on_advance
        count = [0]
        def advance(now):
            original(now)
            count[0] += 1
            if count[0] == 1:
                s = self.books.slots[self.route.sell.symbol]
                with s.lock:
                    s.received = now-105;s.exchange = now-122;s.snapshot=None
        self.clock.on_advance = advance
        result = self.co.execute(self.plan())
        self.assertEqual(self.adapter.sent[1].symbol,self.route.buy.symbol)
        self.assertEqual(result["status"],"unwind")
        self.assertEqual(self.j.totals()["circuit"],"first_unwind")

    def test_no_fresh_exit_records_exposure_without_discarding_cost(self):
        submit = self.adapter.submit
        def disconnect_after_fill(spec, guard):
            receipt = submit(spec,guard)
            self.books.connection(list(self.books.slots),2,False)
            return receipt
        self.adapter.submit = disconnect_after_fill
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"exposure")
        self.assertEqual(len(self.adapter.sent),1)
        self.assertGreater(result["residual_cost_usdt"],0)
        self.assertEqual(result["pnl"],ZERO)

    def test_two_canceled_exits_stop_without_third_order(self):
        self.adapter.faults.extend([None,"cancel","cancel"])
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"exposure")
        self.assertEqual(len(self.adapter.sent),3)
        self.assertEqual(len({x.client_id for x in self.adapter.sent}),3)

    def test_partial_exit_retry_uses_only_remaining_new_inventory(self):
        self.adapter.wallet["DEMO"] = D(100)
        self.adapter.faults.extend([None,"partial",None])
        result = self.co.execute(self.plan())
        self.assertIn(result["status"],("unwind","dust"))
        self.assertEqual(self.adapter.sent[2].quantity,self.adapter.sent[1].quantity/2)
        self.assertEqual(self.adapter.wallet["DEMO"],D(100))

    def test_disagreeing_terminal_confirmation_prevents_retry(self):
        self.adapter.faults.extend([None,"cancel"])
        original = self.adapter.reconcile
        self.adapter.reconcile = lambda spec:replace(original(spec),order_id="different")
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"reconciliation_required")
        self.assertEqual(len(self.adapter.sent),2)

    def test_stop_during_buy_keeps_first_exit_running(self):
        stop = [False]
        self.co.stop_requested = lambda:stop[0]
        original = self.clock.on_advance
        def advance(now):
            original(now);stop[0]=True
        self.clock.on_advance = advance
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"completed")
        self.assertEqual(self.co.entry_block(),"stop_file")

    def test_failed_atomic_confirmation_never_dispatches_exit(self):
        self.j.db.execute("CREATE TRIGGER fail_exit BEFORE INSERT ON orders WHEN NEW.status='PREPARED' AND NEW.intent LIKE '%SELL%' BEGIN SELECT RAISE(ABORT,'injected'); END")
        result = self.co.execute(self.plan())
        self.assertEqual(len(self.adapter.sent),1)
        self.assertEqual(result["status"],"error")
        rows = self.j.export()["orders"]
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]["status"],"PREPARED")

    def test_receipt_quantities_are_checked_before_followup(self):
        original = self.adapter.submit
        self.adapter.submit = lambda spec,guard:replace(original(spec,guard),executed=spec.quantity+D(1))
        result = self.co.execute(self.plan())
        self.assertEqual(result["status"],"reconciliation_required")
        self.assertEqual(len(self.adapter.sent),1)

    def test_restart_retains_completed_buy_count_and_pnl(self):
        self.co.execute(self.plan())
        before = self.j.totals()
        self.j.close()
        self.j = Journal(Path(self.tmp.name)/"paper.db")
        self.assertEqual(self.j.totals(),before)

    def test_open_prepared_order_blocks_after_restart(self):
        plan = self.plan()
        self.j.begin("interrupted",plan,Spec("old",self.route.buy.symbol,"BUY",plan.quantity,plan.buy_limit,"leg1"),self.clock.ms())
        self.assertEqual(self.co.entry_block(),"unresolved_attempt")

    def test_explicit_live_and_paper_journals_cannot_be_mixed(self):
        with self.assertRaises(RuntimeError):
            Journal(self.j.path,mode="live")

    def test_policy_rejects_invalid_numeric_inputs(self):
        for value in ("NaN","Infinity","-1","21"):
            with self.subTest(value=value),self.assertRaises(ValueError):
                Policy(cap_usdt=value)


if __name__ == "__main__":
    unittest.main()
