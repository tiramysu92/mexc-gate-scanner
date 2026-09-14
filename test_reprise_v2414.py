"""Real local processes/HTTP and synthetic DB; no exchange client or real gate."""
from collections import deque
import contextlib
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import time
import types
import unittest
from unittest.mock import patch

import reprendre_live_v2414 as r
import quota_live_v2414 as q
from test_quota_v2414 import JournalFixture, HERE, evidence

FAKE=r'''
import os,json,time,hashlib,sqlite3
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
from session_live_v2412 import LiveSession,TOTALS_SQL,totals
VERSION='__VERSION__'
NEW=VERSION=='2.4.14-trade-quota-session'
PUBLIC_PROTOBUF_BACKEND='upb'
LIVE_SESSION_REQUIRED=os.environ.get('LIVE_SESSION_REQUIRED')=='1'
LIVE_SESSION_ID=os.environ.get('LIVE_SESSION_ID','')
START=time.time()
def _initialize_depth_codec():pass
def data():
 with sqlite3.connect('local.db') as con:
  con.row_factory=sqlite3.Row
  circuit=bool(con.execute('SELECT circuit_open FROM live_state_v240').fetchone()[0])
  causes=[dict(row) for row in con.execute('SELECT * FROM live_circuit_events_v246 WHERE active=1')]
  for row in causes:row['event_id']=None
  policy=json.loads(con.execute('SELECT policy_json FROM live_rearm_sessions_v2412 WHERE session_id=?',(LIVE_SESSION_ID,)).fetchone()[0])
  ss=LiveSession(policy).status(totals(con.execute(TOTALS_SQL).fetchone()))
 stop=Path(os.environ['LIVE_STOP_FILE']).exists(); arm=Path(os.environ['LIVE_ARM_FILE'])
 fresh=arm.stat().st_mtime>=START-1;valid=arm.read_text().strip()=='ENABLE MEXC V247 LIVE 20 USD'
 effective=fresh and valid and not stop and not circuit and not ss.get('reason')
 lv={k:1 for k in __KEYS__}
 lv.update(__BALANCES__)
 lv.update(configured_mode='live',cap_usd=20,capital_limit_usd=200,daily_loss_limit_usd=5,
  live_journal_path='local.db',entry_order_policy='FILL_OR_KILL',protected_bags=True,max_concurrent=1,
  busy=os.environ.get('FAKE_BUSY')=='1',queued=0,circuit_open=circuit,circuit_causes=causes,open_exposures=0,
  private_order_stream={'pending_orders':0},validated_routes=1,
  session=ss,excluded_assets=os.environ.get('LIVE_EXCLUDED_ASSETS','').split(','),
  max_consecutive_unwinds=int(os.environ.get('LIVE_MAX_CONSECUTIVE_UNWINDS',2)),
  arm={'stop_file':stop,'private_ws_ready':True,'effective':effective,'real_orders':effective,
       'reason':'stop_file' if stop else ('live_triple_gate_open' if effective else 'blocked')})
 if os.environ.get('FAKE_ASSET') or (NEW and os.environ.get('FAKE_NEW_ASSET')):
  lv['balances']['SKL']={'free':.01,'locked':0,'total':.01}
 if os.environ.get('FAKE_STALE') or (NEW and os.environ.get('FAKE_NEW_STALE')):lv['account_cache_age_ms']=11000
 bad=NEW and os.environ.get('FAKE_BAD_FLOW')=='1'
 if NEW and os.environ.get('FAKE_BAD_LIMIT')=='1':lv['bbo_age_ms']=999
 if NEW and os.environ.get('FAKE_BAD_SESSION')=='1':lv['session']['buys']=99
 return dict(version=VERSION,live=lv,ws=1,ws_expected=1,depth_ready=1,symbols=1,
   health={'ws_reconnects':0,'ws_disconnects':0},public_flow={'protobuf_backend':'upb',
   'process_started_ms':int(START*1000),'totals':{},'workers':[
   {'active':True,'catching_up':bad,'queue_age_ms':30000 if bad else 0}]})
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  self.send_response(200);self.end_headers();self.wfile.write(json.dumps(data()).encode())
 def log_message(self,*a):pass
if __name__=='__main__':
 if NEW and os.environ.get('FAKE_EXIT')=='1':raise SystemExit(3)
 if NEW and os.environ.get('FAKE_TOUCH_STOP')=='1':os.utime(os.environ['LIVE_STOP_FILE'],None)
 Path('env_hash_'+str(os.getpid())).write_text(hashlib.sha256(os.environ['MEXC_API_SECRET'].encode()).hexdigest())
 HTTPServer.allow_reuse_address=True
 HTTPServer(('127.0.0.1',int(os.environ['PORT'])),H).serve_forever()
'''.replace('__KEYS__',repr(r.LIMIT_KEYS)).replace('__BALANCES__',repr(evidence()))


class ResumeTests(JournalFixture):
    def setUp(self):
        super().setUp();self.children=[]
        self.addCleanup(self.stop_children)
        (self.root/'session_live_v2412.py').write_bytes((HERE/'session_live_v2412.py').read_bytes())
        with socket.socket() as s:s.bind(('127.0.0.1',0));self.port=s.getsockname()[1]
        self.stop=self.root/'STOP';self.stop.write_text('user stop')
        self.arm=self.root/'ARM';self.arm.write_text('ENABLE MEXC V247 LIVE 20 USD\n')
        self.env=dict(os.environ,PORT=str(self.port),TRADING_MODE='live',LIVE_ARMED='1',
            LIVE_STOP_FILE=str(self.stop),LIVE_ARM_FILE=str(self.arm),
            LIVE_SESSION_ID=self.prior['session_id'],LIVE_SESSION_REQUIRED='1',LIVE_RESET_CIRCUIT='0',
            LIVE_EXCLUDED_ASSETS='CTO',MEXC_API_SECRET='dummy-secret-not-to-print')
    def stop_children(self):
        for child in self.children:
            if child.poll() is None:child.terminate()
            child.wait(timeout=3)
    def start(self,active_version=None,**flags):
        self.env.update(flags)
        (self.root/'app.py').write_text(FAKE.replace('__VERSION__',active_version or r.OLD_VERSION))
        self.old=subprocess.Popen([sys.executable,'-u','app.py'],cwd=self.root,env=self.env,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
        self.children.append(self.old);self.read=r.status_reader(self.port)
        for _ in range(100):
            try:self.read();break
            except OSError:time.sleep(.02)
        else:self.fail('Fake scanner did not start')
        (self.root/'app.py').write_text(FAKE.replace('__VERSION__',r.VERSION))
    def run_resume(self,apply=True,budget=5,acknowledge=True):
        root=self.root;env=self.env;old=self.old;realpath=Path;self.output=io.StringIO()
        # Adapt only managed-runtime PID mapping. Real process, HTTP, filesystem,
        # journal and STOP checks still run. Production code has no test bypass.
        class Proc:
            def __init__(self,leaf=''):self.leaf=leaf
            def __truediv__(self,leaf):return Proc(leaf)
            def stat(self):return types.SimpleNamespace(st_uid=os.getuid())
            def resolve(self):return root
            def read_text(self):return '1 (python) '+' '.join(['S']+['0']*18+['12345'])
            def read_bytes(self):
                if self.leaf=='cmdline':return (sys.executable+'\0-u\0app.py\0').encode()
                if self.leaf=='environ':return b'\0'.join((k+'='+v).encode() for k,v in env.items())
                raise AssertionError(self.leaf)
        def path(*a):return Proc() if a==('/proc',) else realpath(*a)
        def terminate(*a):old.terminate();old.wait(timeout=3)
        realpopen=subprocess.Popen
        def popen(*a,**kw):
            child=realpopen(*a,**kw);self.children.append(child);return child
        with contextlib.redirect_stdout(self.output),contextlib.redirect_stderr(self.output),\
             patch.object(r,'Path',path),\
             patch.object(r.os,'pidfd_open',lambda pid:os.open(os.devnull,os.O_RDONLY)),\
             patch.object(r.signal,'pidfd_send_signal',terminate),\
             patch.object(r.select,'select',lambda a,b,c,t:(a if old.poll() is not None else [],[],[])),\
             patch.object(r.subprocess,'Popen',popen):
            return r.resume(old.pid,root,sys.executable,budget=budget,apply=apply,acknowledge_skl=acknowledge,
                drain_timeout=.7,startup_timeout=2,stable_seconds=0,watch=False)
    def test_preview_changes_no_process_gate_or_journal(self):
        self.start();mtime=self.arm.stat().st_mtime_ns;plan=q.read_plan(self.db)
        self.run_resume(apply=False)
        self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
        self.assertEqual(self.arm.stat().st_mtime_ns,mtime);self.assertEqual(q.read_plan(self.db),plan)
    def test_full_restart_acknowledges_only_skl_then_arms_new_quota(self):
        self.start();self.run_resume();lv=self.read()['live']
        self.assertFalse(self.stop.exists());self.assertTrue(lv['arm']['real_orders'])
        self.assertIsNone(lv['session']['expires_ts_ms']);self.assertEqual(lv['session']['mode'],'trade_quota')
        self.assertEqual(lv['session']['buys'],0);self.assertEqual(lv['session']['pnl'],0)
        self.assertEqual(lv['session']['additional_loss_usd'],5)
        self.assertEqual(lv['excluded_assets'],['CTO']);self.assertEqual(lv['max_consecutive_unwinds'],1)
        self.assertFalse(lv['circuit_open']);self.assertEqual(len(q.read_plan(self.db)['policies']),2)
        self.assertNotIn(self.env['MEXC_API_SECRET'],self.output.getvalue())
        pid=int((self.root/'scanner_v2414_live.pid').read_text())
        self.assertEqual((self.root/f'env_hash_{pid}').read_text(),hashlib.sha256(self.env['MEXC_API_SECRET'].encode()).hexdigest())
    def test_restart_reuses_quota_with_spent_budget(self):
        policy,_=self.allocate();self.add_attempt('already-purchased',pnl=-1)
        self.env['LIVE_SESSION_ID']=policy['session_id']
        self.start(active_version=r.VERSION);self.run_resume(budget=None,acknowledge=False)
        ss=self.read()['live']['session']
        self.assertEqual(ss['session_id'],policy['session_id']);self.assertEqual(ss['buys'],1)
        self.assertEqual(ss['pnl'],-1);self.assertEqual(len(q.read_plan(self.db)['policies']),2)
    def test_exhausted_quota_is_not_rearmed_or_reset(self):
        policy,_=self.allocate();self.add_attempt('loss',pnl=-5)
        self.env['LIVE_SESSION_ID']=policy['session_id'];self.start(active_version=r.VERSION)
        with self.assertRaisesRegex(r.RestartError,'session_loss_limit'):self.run_resume()
        self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
    def test_absent_explicit_budget_preserves_old_scanner(self):
        self.start()
        with self.assertRaises(r.RestartError):self.run_resume(budget=None)
        self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
    def test_no_explicit_skl_ack_preserves_circuit(self):
        self.start()
        with self.assertRaises(r.RestartError):self.run_resume(acknowledge=False)
        self.assertIsNone(self.old.poll());self.assertTrue(self.read()['live']['circuit_open'])
    def test_other_circuit_cannot_be_cleared(self):
        with sqlite3.connect(self.db) as con:
            con.execute("UPDATE live_circuit_events_v246 SET error='protected_unknown_asset:SAGA' WHERE active=1")
        self.start()
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertIsNone(self.old.poll());self.assertTrue(self.read()['live']['circuit_open'])
    def test_busy_order_prevents_process_termination(self):
        self.start(FAKE_BUSY='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
    def test_stale_account_prevents_process_termination(self):
        self.start(FAKE_STALE='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
    def test_remaining_inventory_prevents_process_termination(self):
        self.start(FAKE_ASSET='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
    def test_global_circuit_reset_is_refused(self):
        self.start(LIVE_RESET_CIRCUIT='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
    def test_failed_native_precheck_preserves_process_and_circuit(self):
        self.start();(self.root/'app.py').write_text('raise RuntimeError("synthetic failure")')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertIsNone(self.old.poll());self.assertTrue(self.read()['live']['circuit_open'])
    def test_bad_new_public_flow_leaves_stop(self):
        self.start(FAKE_BAD_FLOW='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertTrue(self.stop.exists());self.assertFalse(self.read()['live']['arm']['real_orders'])
        self.assertEqual(len(list(self.root.glob('bilan_live_demarrage_*.json.gz'))),1)
    def test_changed_limits_leave_stop(self):
        self.start(FAKE_BAD_LIMIT='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertTrue(self.stop.exists())
    def test_new_stale_balances_leave_stop(self):
        self.start(FAKE_NEW_STALE='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertTrue(self.stop.exists())
    def test_new_inventory_leaves_stop(self):
        self.start(FAKE_NEW_ASSET='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertTrue(self.stop.exists())
    def test_changed_session_counts_leave_stop(self):
        self.start(FAKE_BAD_SESSION='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertTrue(self.stop.exists())
    def test_new_startup_failure_retains_stop_and_committed_quota(self):
        self.start(FAKE_EXIT='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertTrue(self.stop.exists());self.assertEqual(len(q.read_plan(self.db)['policies']),2)
    def test_user_touched_stop_is_not_removed(self):
        self.start(FAKE_TOUCH_STOP='1')
        with self.assertRaises(r.RestartError):self.run_resume()
        self.assertTrue(self.stop.exists());self.assertFalse(self.read()['live']['arm']['real_orders'])
    def test_monitor_stops_new_entries_but_collects_settled_exit(self):
        self.start();self.run_resume();ss=self.read()['live']['session']
        policy=q.read_plan(self.db)['policies'][ss['session_id']]
        def read():
            d=self.read();d['live']['session']['reason']='session_buy_limit';return d
        with contextlib.redirect_stdout(io.StringIO()):
            r.monitor(read,self.stop,self.root,ss['session_id'],deque(maxlen=1200),self.db,policy['created_ts_ms'])
        self.assertTrue(self.stop.exists());self.assertIsNone(self.children[-1].poll())
        files=list(self.root.glob('bilan_live_session_*.json.gz'));self.assertEqual(len(files),1)
        with gzip.open(files[0],'rt') as f:report=json.load(f)
        self.assertTrue(report['settled']);self.assertEqual(report['flow_counts']['ws_reconnects'],0)
        self.assertEqual(len(list(self.root.glob('*_courant.json.gz'))),1)
    def test_monitor_http_failure_preserves_stop_and_partial_bilan(self):
        self.start();self.run_resume();ss=self.read()['live']['session']
        def read():raise TimeoutError('synthetic timeout')
        with contextlib.redirect_stdout(io.StringIO()),self.assertRaises(TimeoutError):
            r.monitor(read,self.stop,self.root,ss['session_id'],[],self.db,int(time.time()*1000))
        self.assertTrue(self.stop.exists());self.assertEqual(len(list(self.root.glob('bilan_live_session_*.json.gz'))),1)


class ReportTests(JournalFixture):
    def test_bounded_samples_keep_initial_counter_baseline(self):
        initial=dict(health={'ws_reconnects':10},public_flow={'totals':{'catchup_started':20}})
        traces=deque([dict(health={'ws_reconnects':15},public_flow={'totals':{'catchup_started':30}})]*1300,maxlen=1200)
        report=r.make_report(traces,initial,self.db,0)
        self.assertEqual(len(report['samples']),1200)
        self.assertEqual(report['flow_counts']['ws_reconnects'],5)
        self.assertEqual(report['flow_counts']['catchup_started'],10)
    def test_checkpoint_replaces_one_file_and_keeps_source_journal(self):
        before=self.query('SELECT * FROM live_attempts_v240')
        path=r.checkpoint(self.root,'d'*32,{'number':1})
        r.checkpoint(self.root,'d'*32,{'number':2})
        self.assertEqual(len(list(self.root.glob('*_courant.json.gz'))),1)
        with gzip.open(path,'rt') as f:self.assertEqual(json.load(f),{'number':2})
        self.assertEqual(self.query('SELECT * FROM live_attempts_v240'),before)
    def test_export_keeps_early_purchase_after_many_zero_fills(self):
        policy,_=self.allocate();created=policy['created_ts_ms']
        self.add_attempt('early-purchase',created=created)
        with sqlite3.connect(self.db) as con:
            con.row_factory=sqlite3.Row
            row=dict(con.execute("SELECT * FROM live_attempts_v240 WHERE attempt_id='early-purchase'").fetchone())
            for i in range(1005):
                row.update(attempt_id='empty'+str(i),decision_id='empty'+str(i),mid_acquired=0,
                    status='leg1_canceled',created_ts_ms=created+i+1)
                con.execute('INSERT INTO live_attempts_v240('+','.join(row)+') VALUES('+','.join('?' for _ in row)+')',list(row.values()))
        report=r.session_journal(self.db,created)
        self.assertTrue(report['truncated']);self.assertEqual(len(report['attempts']),1001)
        self.assertIn('early-purchase',[row['attempt_id'] for row in report['attempts']])
        self.assertEqual(sum(row['count'] for row in report['status_counts']),1006)


if __name__=='__main__':unittest.main(verbosity=2)
