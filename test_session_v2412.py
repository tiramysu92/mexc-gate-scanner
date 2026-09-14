"""Risk-allocation, journal and final-entry guards; entirely offline."""
import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from session_live_v2412 import LiveSession
from test_public_flow import load_offline
import test_regularisation as accounting_tests
import test_live_recovery as recovery_tests
import reprendre_live_v2412 as restart
import regulariser_cto as accounting

HERE=Path(__file__).resolve().parent


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.wall=1000000; self.mono=0
        self.policy=dict(session_id='a'*32,created_ts_ms=self.wall,expires_ts_ms=self.wall+60000,
                         additional_loss_usd=5,max_buys=10,baseline=dict(pnl=-12,buys=1,unwinds=1))
        self.control=LiveSession(self.policy,lambda:self.wall,lambda:self.mono)
    def state(self,pnl=-12,buys=1,unwinds=1):
        return self.control.status(dict(pnl=pnl,buys=buys,unwinds=unwinds))
    def test_historical_loss_is_retained_and_additional_loss_stops_at_limit(self):
        self.assertEqual(self.state()['baseline_realized_pnl'],-12)
        self.assertEqual(self.state()['pnl'],0)
        self.assertIsNone(self.state(pnl=-16.9)['reason'])
        self.assertEqual(self.state(pnl=-17)['reason'],'session_loss_limit')
    def test_restart_preserves_spent_budget_and_original_expiry(self):
        self.wall+=40000; self.mono+=40
        self.control=LiveSession(self.policy,lambda:self.wall,lambda:self.mono)
        self.assertEqual(self.state(pnl=-16)['pnl'],-4)
        self.wall+=20000;self.mono+=20
        self.assertEqual(self.state()['reason'],'session_expired')
    def test_midnight_has_no_effect_on_cumulative_loss_budget(self):
        self.policy.update(created_ts_ms=1704153590000,expires_ts_ms=1704153650000)
        self.wall=1704153590000
        self.control=LiveSession(self.policy,lambda:self.wall,lambda:self.mono)
        self.wall+=15000
        self.assertEqual(self.state(pnl=-17)['reason'],'session_loss_limit')
    def test_first_unwind_stops_even_if_profitable(self):
        self.assertEqual(self.state(pnl=-10,unwinds=2)['reason'],'session_first_unwind')
    def test_tenth_purchase_stops_further_entries(self):
        self.assertIsNone(self.state(buys=10)['reason'])
        self.assertEqual(self.state(buys=11)['reason'],'session_buy_limit')
    def test_clock_rollback_cannot_extend_session(self):
        self.mono=61
        self.assertEqual(self.state()['reason'],'session_expired')
        self.mono=0;self.wall-=5000
        self.assertEqual(self.state()['reason'],'session_clock_invalid')
    def test_missing_accounting_and_regressed_counts_block(self):
        self.assertEqual(self.control.status(None)['reason'],'session_accounting_unavailable')
        self.assertEqual(self.state(buys=0)['reason'],'session_journal_regressed')
    def test_invalid_or_excessive_allocations_are_rejected(self):
        for field,value in [('additional_loss_usd',float('nan')),('additional_loss_usd',0),
                            ('additional_loss_usd',6),('max_buys',11),('expires_ts_ms',3000000)]:
            with self.subTest(field=field,value=value),self.assertRaises(ValueError):
                LiveSession(dict(self.policy,**{field:value}))
    def prepare_app(self):
        m=load_offline(HERE/'app.py');m.TRADING_MODE='live';m.LIVE_SESSION_REQUIRED=True
        m.live_session_control=self.control;m.live_session_totals=dict(pnl=-12,buys=1,unwinds=1)
        m.now_ms=lambda:self.wall
        return m
    def test_expiry_blocks_at_last_guard_before_order_post(self):
        m=self.prepare_app();self.wall+=60000
        with tempfile.TemporaryDirectory() as tmp:
            m.LIVE_STOP_FILE=str(Path(tmp)/'missing_stop');m.LIVE_ARM_FILE=str(Path(tmp)/'missing_arm')
            with self.assertRaisesRegex(m.MexcSignalExpired,'session_expired'):
                m._entry_price_guard({'path':['USDT','X','USDC']},{},self.wall,20)
    def test_cto_is_rejected_at_route_guard_in_either_direction(self):
        m=self.prepare_app();m.LIVE_EXCLUDED_ASSETS=frozenset(['CTO'])
        for path in (['USDT','CTO','USD1'],['USD1','CTO','USDT']):
            self.assertEqual(m._live_health_status({'path':path})[0],'route_asset_excluded')
    def test_default_daily_stop_and_session_allocation_are_distinct(self):
        m=self.prepare_app()
        self.assertFalse(m.live_loss_limit_reached(-12))
        m.live_session_totals['pnl']=-17
        self.assertTrue(m.live_loss_limit_reached(-17))
        m.LIVE_SESSION_REQUIRED=False
        self.assertTrue(m.live_loss_limit_reached(-12))
        self.assertEqual(m.LIVE_DAILY_LOSS_LIMIT_USD,5)
    def test_expired_session_still_allows_the_existing_position_to_unwind(self):
        case=recovery_tests.RecoveryTests('test_full_real_attempt_records_recovery_after_second_exit_fills')
        case.setUp()
        self.wall+=60000
        case.m.LIVE_SESSION_REQUIRED=True
        case.m.live_session_control=self.control
        case.m.live_session_totals=dict(pnl=-12,buys=1,unwinds=1)
        self.assertEqual(case.m.live_session_status()['reason'],'session_expired')
        case.run_full_attempt(success_retry=True)


class AllocationTests(unittest.TestCase):
    read=accounting_tests.AccountingTests.read
    def setUp(self):
        accounting_tests.AccountingTests.setUp(self)
        patcher=patch.object(restart,'ATTEMPT','synthetic_cto_attempt')
        patcher.start();self.addCleanup(patcher.stop)
        accounting.reconcile(self.path,apply=True)
    def allocate(self,expected=None):
        return restart.allocate(self.path,Path(self.tmp.name),'b'*32,5,30,
                                expected or restart.read_plan(self.path))
    def test_acknowledgement_preserves_orders_losses_and_original_causes(self):
        orders=self.read('SELECT * FROM live_orders_v240')
        pnl=self.read('SELECT realized_pnl FROM live_attempts_v240')
        causes=self.read('SELECT event_id,reason,error FROM live_circuit_events_v246')
        policy,backup=self.allocate()
        self.assertTrue(backup.is_file())
        self.assertEqual(self.read('SELECT * FROM live_orders_v240'),orders)
        self.assertEqual(self.read('SELECT realized_pnl FROM live_attempts_v240'),pnl)
        self.assertEqual(self.read('SELECT event_id,reason,error FROM live_circuit_events_v246'),causes)
        self.assertEqual(self.read('SELECT circuit_open FROM live_state_v240'),[(0,)])
        self.assertEqual(LiveSession(policy).status(restart.read_plan(self.path)['totals'])['pnl'],0)
        self.assertEqual(self.read('SELECT COUNT(*) FROM live_rearm_sessions_v2412'),[(1,)])
    def test_unknown_circuit_is_never_acknowledged(self):
        with sqlite3.connect(self.path) as con:
            con.execute("UPDATE live_circuit_events_v246 SET reason='order_status_unknown' WHERE event_id=2")
        with self.assertRaises(restart.RestartError):self.allocate()
        self.assertEqual(self.read('SELECT circuit_open FROM live_state_v240'),[(1,)])
    def test_unresolved_order_blocks_rearming(self):
        with sqlite3.connect(self.path) as con:
            con.execute("UPDATE live_orders_v240 SET status='submit_unknown' WHERE side='SELL'")
        with self.assertRaises(restart.RestartError):self.allocate()
    def test_changed_loss_between_preview_and_apply_is_rejected(self):
        plan=restart.read_plan(self.path)
        with sqlite3.connect(self.path) as con:
            cur=con.execute('SELECT * FROM live_attempts_v240 LIMIT 1')
            row=dict(zip([c[0] for c in cur.description],cur.fetchone()))
            row.update(attempt_id='new_synthetic_attempt',decision_id='new_synthetic_decision',realized_pnl=-1)
            con.execute('INSERT INTO live_attempts_v240('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',list(row.values()))
        with self.assertRaises(restart.RestartError):self.allocate(plan)
        self.assertEqual(self.read('SELECT circuit_open FROM live_state_v240'),[(1,)])
    def test_duplicate_allocation_rolls_back_without_resetting_session(self):
        policy,_=self.allocate()
        with self.assertRaises(sqlite3.IntegrityError):self.allocate()
        self.assertEqual(self.read('SELECT COUNT(*) FROM live_rearm_sessions_v2412'),[(1,)])
        stored=json.loads(self.read('SELECT policy_json FROM live_rearm_sessions_v2412')[0][0])
        self.assertEqual(stored,policy)


if __name__=='__main__':unittest.main(verbosity=2)
