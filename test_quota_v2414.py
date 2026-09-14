"""Durable quota and narrowly scoped inventory acknowledgement, fully offline."""
import copy
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch

import quota_live_v2414 as q
import regulariser_cto as accounting
from relancer_mexc_v2411 import RestartError
from session_live_v2412 import LiveSession, TOTALS_SQL, totals
from test_public_flow import load_offline
from test_regularisation import SYNTHETIC_PARAMETERS

HERE=Path(__file__).resolve().parent


def evidence():
    return dict(api_ok=True,account_cache_age_ms=4755,account_cache_max_age_ms=10000,
        allowed_start_assets=['USDT','USDC','USD1'],balances={
        'USDT':dict(free=86.76,locked=0,total=86.76),
        'USD1':dict(free=100,locked=0,total=100)})


class QuotaTests(unittest.TestCase):
    def setUp(self):
        self.wall=1704153590000;self.mono=0
        self.policy=dict(session_id='a'*32,mode='trade_quota',created_ts_ms=self.wall,
            expires_ts_ms=None,additional_loss_usd=5,max_buys=10,
            baseline=dict(pnl=-13.144151092630048,buys=4,unwinds=3))
        self.control=LiveSession(self.policy,lambda:self.wall,lambda:self.mono)
    def status(self,**changes):
        current=dict(self.policy['baseline']);current.update(changes)
        return self.control.status(current)
    def test_no_expiry_after_midnight_or_thirty_days(self):
        self.wall+=30*86400000;self.mono+=30*86400
        self.assertIsNone(self.status()['reason'])
        self.assertIsNone(self.status()['expires_ts_ms'])
        self.assertEqual(self.status()['mode'],'trade_quota')
    def test_restart_retains_spent_budget_and_count(self):
        current=dict(pnl=self.policy['baseline']['pnl']-4,buys=12,unwinds=3)
        before=self.control.status(current)
        self.wall+=86400000
        self.control=LiveSession(json.loads(json.dumps(self.policy)),lambda:self.wall,lambda:self.mono)
        self.assertEqual(self.control.status(current),before)
        self.assertEqual(before['buys'],8);self.assertAlmostEqual(before['pnl'],-4)
    def test_tenth_actual_purchase_blocks_new_entry(self):
        self.assertIsNone(self.status(buys=13)['reason'])
        self.assertEqual(self.status(buys=14)['reason'],'session_buy_limit')
    def test_loss_limit_is_incremental_not_daily(self):
        baseline=self.policy['baseline']['pnl']
        self.assertIsNone(self.status(pnl=baseline-4.99)['reason'])
        self.assertEqual(self.status(pnl=baseline-5)['reason'],'session_loss_limit')
    def test_first_unwind_blocks_even_with_profit(self):
        self.assertEqual(self.status(pnl=0,unwinds=4)['reason'],'session_first_unwind')
    def test_invalid_clock_missing_accounting_and_regression_fail_closed(self):
        self.assertEqual(self.control.status(None)['reason'],'session_accounting_unavailable')
        self.assertEqual(self.status(buys=3)['reason'],'session_journal_regressed')
        self.wall-=2000
        self.assertEqual(self.status()['reason'],'session_clock_invalid')
    def test_null_expiry_alone_never_disables_legacy_deadline(self):
        invalid=[{k:v for k,v in self.policy.items() if k!='mode'},
                 {k:v for k,v in self.policy.items() if k!='expires_ts_ms'}]
        invalid += [dict(self.policy,**{key:value}) for key,value in (
            ('mode','forever'),('expires_ts_ms',0),('max_buys',11),
            ('additional_loss_usd',6),('additional_loss_usd',float('nan')))]
        for policy in invalid:
            with self.subTest(policy=policy),self.assertRaises(ValueError):LiveSession(policy)
    def test_original_timed_policy_still_expires(self):
        timed=dict(self.policy,mode='timed',expires_ts_ms=self.wall+60000)
        control=LiveSession(timed,lambda:self.wall,lambda:self.mono)
        self.wall+=60000
        self.assertEqual(control.status(timed['baseline'])['reason'],'session_expired')
    def test_quota_blocks_real_last_entry_guard(self):
        m=load_offline(HERE/'app.py');m.TRADING_MODE='live';m.LIVE_SESSION_REQUIRED=True
        m.live_session_control=self.control;m.live_session_totals=dict(self.policy['baseline'],buys=14)
        m.now_ms=lambda:self.wall
        with tempfile.TemporaryDirectory() as root:
            m.LIVE_STOP_FILE=str(Path(root)/'STOP');m.LIVE_ARM_FILE=str(Path(root)/'ARM')
            with self.assertRaisesRegex(m.MexcSignalExpired,'session_buy_limit'):
                m._entry_price_guard({'path':['USDT','X','USD1']},{},self.wall,20)
    def test_quota_does_not_block_exit_of_tenth_purchase(self):
        import test_live_recovery
        case=test_live_recovery.RecoveryTests('test_full_real_attempt_records_recovery_after_second_exit_fills')
        case.setUp()
        try:
            case.m.LIVE_SESSION_REQUIRED=True;case.m.live_session_control=self.control
            case.m.live_session_totals=dict(self.policy['baseline'],buys=14)
            self.assertEqual(case.m.live_session_status()['reason'],'session_buy_limit')
            case.run_full_attempt(success_retry=True)
        finally:case.doCleanups()


class JournalFixture(unittest.TestCase):
    """Always leave the caller's working directory BEFORE building a journal."""
    def setUp(self):
        self.cwd=Path.cwd();self.tmp=tempfile.TemporaryDirectory(prefix='mexc_quota_test_')
        self.root=Path(self.tmp.name);self.db=self.root/'local.db'
        self.addCleanup(self.cleanup);os.chdir(self.root)
        patcher=patch.multiple(accounting,**SYNTHETIC_PARAMETERS)
        patcher.start();self.addCleanup(patcher.stop)
        m=load_offline(HERE/'app.py');m.LIVE_DB_PATH=str(self.db);m.LIVE_IMPORT_CAPABILITIES_DB=''
        m.initialize_live_journal()
        for table,rows in json.loads((HERE/'cto_journal_fixture_v2411.json').read_text()).items():
            for row in rows:
                m.live_db_connection.execute('INSERT INTO '+table+'('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',list(row.values()))
        m.live_db_connection.commit();m.live_db_connection.close();m.live_db_connection=None
        accounting.reconcile(self.db,apply=True)
        with sqlite3.connect(self.db) as con:
            con.row_factory=sqlite3.Row
            con.execute('UPDATE live_circuit_events_v246 SET active=0')
            con.execute('INSERT INTO live_circuit_events_v246(ts_ms,day,attempt_id,reason,error,active) VALUES(?,?,?,?,?,1)',
                (q.REVIEWED_SKL_TS,'2026-09-14',None,'protected_asset_detected',q.REVIEWED_SKL_ERROR))
            con.execute('UPDATE live_state_v240 SET circuit_open=1,circuit_reason=?,last_error=?',
                ('protected_asset_detected',q.REVIEWED_SKL_ERROR))
            con.execute('CREATE TABLE live_rearm_sessions_v2412(session_id TEXT PRIMARY KEY,policy_json TEXT NOT NULL,acknowledgement_json TEXT NOT NULL,backup_path TEXT NOT NULL)')
            created=int(time.time()*1000)-120000
            self.prior=dict(session_id='b'*32,created_ts_ms=created,expires_ts_ms=created+60000,
                additional_loss_usd=5,max_buys=10,baseline=totals(con.execute(TOTALS_SQL).fetchone()))
            con.execute('INSERT INTO live_rearm_sessions_v2412 VALUES(?,?,?,?)',
                (self.prior['session_id'],json.dumps(self.prior),'{}','synthetic-prior-backup'))
    def cleanup(self):
        os.chdir(self.cwd);self.tmp.cleanup()
    def query(self,sql,params=()):
        with sqlite3.connect(self.db) as con:return con.execute(sql,params).fetchall()
    def allocate(self):
        plan=q.read_plan(self.db);policy,new=q.choose_policy(plan,self.prior['session_id'],5)
        backup=q.commit_quota(self.db,self.root,plan,policy,new,True,evidence())
        return policy,backup
    def add_attempt(self,identifier,pnl=0,bought=True,unwind=False,created=None):
        with sqlite3.connect(self.db) as con:
            con.row_factory=sqlite3.Row
            row=dict(con.execute('SELECT * FROM live_attempts_v240 WHERE attempt_id=?',('synthetic_cto_attempt',)).fetchone())
            row.update(attempt_id=identifier,decision_id=identifier,day='2026-09-15',
                created_ts_ms=created if created is not None else int(time.time()*1000),
                status='completed' if bought else 'leg1_canceled',realized_pnl=pnl,
                mid_acquired=1 if bought else 0,residual_mid=0,
                unwind_client_id=identifier+'exit' if unwind else None)
            con.execute('INSERT INTO live_attempts_v240('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',list(row.values()))


class JournalTests(JournalFixture):
    def test_acknowledges_only_exact_skl_event_and_preserves_history(self):
        orders=self.query('SELECT * FROM live_orders_v240');attempts=self.query('SELECT * FROM live_attempts_v240')
        prior=self.query('SELECT * FROM live_rearm_sessions_v2412')
        policy,backup=self.allocate()
        self.assertEqual(self.query('SELECT * FROM live_orders_v240'),orders)
        self.assertEqual(self.query('SELECT * FROM live_attempts_v240'),attempts)
        self.assertEqual(self.query('SELECT * FROM live_rearm_sessions_v2412 WHERE session_id=?',(self.prior['session_id'],)),prior)
        self.assertEqual(self.query('SELECT circuit_open FROM live_state_v240'),[(0,)])
        self.assertEqual(self.query('SELECT COUNT(*) FROM live_circuit_ack_v2414'),[(1,)])
        self.assertTrue(backup.is_file())
        with sqlite3.connect(backup) as con:self.assertEqual(con.execute('SELECT circuit_open FROM live_state_v240').fetchone()[0],1)
        self.assertEqual(policy['baseline']['pnl'],-12.004)
    def test_skl_requires_explicit_acknowledgement(self):
        plan=q.read_plan(self.db);policy,new=q.choose_policy(plan,self.prior['session_id'],5)
        with self.assertRaises(RestartError):q.commit_quota(self.db,self.root,plan,policy,new,False,evidence())
        self.assertEqual(self.query('SELECT COUNT(*) FROM live_rearm_sessions_v2412'),[(1,)])
        self.assertEqual(self.query('SELECT circuit_open FROM live_state_v240'),[(1,)])
    def test_different_or_additional_circuits_are_not_acknowledged(self):
        original=q.read_plan(self.db)['causes']
        variants=[original+original, [dict(original[0],error='protected_unknown_asset:SAGA')],
            [dict(original[0],ts_ms=q.REVIEWED_SKL_TS+1)], [dict(original[0],attempt_id='new')],
            [dict(original[0],reason='unwind_failed')]]
        for causes in variants:
            with self.subTest(causes=causes),self.assertRaises(RestartError):q.check_causes(causes,True)
        q.check_causes([dict(original[0],event_id=None)],True)
    def test_stale_locked_nonstable_and_invalid_balances_block(self):
        variants=[dict(evidence(),account_cache_age_ms=10001),dict(evidence(),api_ok=False),
                  dict(evidence(),balances={}),dict(evidence(),allowed_start_assets=['CTO'])]
        for key,value in [('free',float('nan')),('locked',1),('total',-1),('total',87)]:
            e=evidence();e['balances']['USDT'][key]=value;variants.append(e)
        for asset in ('SKL','CTO','SAGA','牛来'):
            e=evidence();e['balances'][asset]=dict(free=.00001,locked=0,total=.00001);variants.append(e)
        for e in variants:
            with self.subTest(e=e),self.assertRaises(RestartError):q.stable_balances(e)
        q.stable_balances(evidence())
    def test_existing_quota_reused_after_midnight_without_reset(self):
        policy,_=self.allocate();self.add_attempt('trade-next-day',pnl=-2)
        plan=q.read_plan(self.db);again,new=q.choose_policy(plan,policy['session_id'],None)
        self.assertEqual(again,policy);self.assertFalse(new)
        q.commit_quota(self.db,self.root,plan,again,new,False,evidence())
        ss=LiveSession(again).status(q.read_plan(self.db)['totals'])
        self.assertEqual(ss['buys'],1);self.assertEqual(ss['pnl'],-2)
        self.assertEqual(self.query('SELECT COUNT(*) FROM live_rearm_sessions_v2412'),[(2,)])
    def test_finished_quota_cannot_be_renewed_or_budget_increased(self):
        policy,_=self.allocate()
        with self.assertRaises(RestartError):q.choose_policy(q.read_plan(self.db),policy['session_id'],4)
        self.add_attempt('loss',pnl=-5)
        for active in (policy['session_id'],self.prior['session_id']):
            with self.assertRaisesRegex(RestartError,'session_loss_limit'):q.choose_policy(q.read_plan(self.db),active,5)
    def test_previous_executed_timed_trial_needs_new_review(self):
        self.add_attempt('prior-purchase')
        with self.assertRaisesRegex(RestartError,'session précédente'):q.choose_policy(q.read_plan(self.db),self.prior['session_id'],5)
    def test_changed_journal_between_checks_prevents_commit(self):
        plan=q.read_plan(self.db);policy,new=q.choose_policy(plan,self.prior['session_id'],5)
        self.add_attempt('changed')
        with self.assertRaisesRegex(RestartError,'journal a changé'):q.commit_quota(self.db,self.root,plan,policy,new,True,evidence())
        self.assertEqual(self.query('SELECT circuit_open FROM live_state_v240'),[(1,)])
    def test_unresolved_order_and_open_exposure_block(self):
        for sql in ("UPDATE live_orders_v240 SET status='submit_unknown' WHERE side='SELL'",
                    "UPDATE live_open_exposures_v246 SET status='open',units=1"):
            with sqlite3.connect(self.db) as con:con.execute(sql)
            with self.assertRaises((RestartError,RuntimeError)):q.read_plan(self.db)
    def test_sql_failure_rolls_back_quota_circuit_and_ack(self):
        with sqlite3.connect(self.db) as con:
            con.execute("CREATE TRIGGER fail_state BEFORE UPDATE ON live_state_v240 BEGIN SELECT RAISE(ABORT,'synthetic error'); END")
        with self.assertRaises(sqlite3.Error):self.allocate()
        self.assertEqual(self.query('SELECT COUNT(*) FROM live_rearm_sessions_v2412'),[(1,)])
        self.assertEqual(self.query('SELECT circuit_open FROM live_state_v240'),[(1,)])
        self.assertEqual(self.query("SELECT name FROM sqlite_master WHERE name='live_circuit_ack_v2414'"),[])
    def test_running_scanner_blocks_commit_before_write(self):
        with patch.object(q,'assert_scanner_stopped',side_effect=RuntimeError('Scanner encore actif')):
            with self.assertRaisesRegex(RuntimeError,'encore actif'):self.allocate()
        self.assertEqual(self.query('SELECT COUNT(*) FROM live_rearm_sessions_v2412'),[(1,)])
    def test_gateway_loads_durable_quota_from_database_on_restart(self):
        policy,_=self.allocate();self.add_attempt('restart-purchase',pnl=-2)
        for _ in range(2):
            m=load_offline(HERE/'app.py');m.LIVE_DB_PATH=str(self.db);m.TRADING_MODE='shadow'
            m.LIVE_SESSION_REQUIRED=True;m.LIVE_SESSION_ID=policy['session_id']
            m.initialize_live_gateway()
            try:
                ss=m.live_session_status()
                self.assertEqual(ss['buys'],1);self.assertEqual(ss['pnl'],-2)
                self.assertEqual(ss['session_id'],policy['session_id']);self.assertIsNone(ss['expires_ts_ms'])
            finally:m.live_db_connection.close()


if __name__=='__main__':unittest.main(verbosity=2)
