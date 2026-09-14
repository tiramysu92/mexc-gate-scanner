"""Restart/arming integration with fake HTTP scanners and fictitious journal."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch
import reprendre_live_v2413 as r
import regulariser_cto as accounting
import test_regularisation as fixture
from test_public_flow import load_offline

HERE=Path(__file__).resolve().parent
FAKE=r'''
import os,json,time,hashlib,sqlite3
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
from session_live_v2412 import LiveSession,TOTALS_SQL,totals
VERSION='__VERSION__'
PUBLIC_PROTOBUF_BACKEND='upb'
LIVE_SESSION_REQUIRED=os.environ.get('LIVE_SESSION_REQUIRED')=='1'
LIVE_SESSION_ID=os.environ.get('LIVE_SESSION_ID','')
START=time.time()
def _initialize_depth_codec():pass
def data():
 with sqlite3.connect('local.db') as con:
  con.row_factory=sqlite3.Row
  circuit=bool(con.execute('SELECT circuit_open FROM live_state_v240').fetchone()[0])
  if LIVE_SESSION_REQUIRED:
   policy=json.loads(con.execute('SELECT policy_json FROM live_rearm_sessions_v2412 WHERE session_id=?',(LIVE_SESSION_ID,)).fetchone()[0])
   ss=LiveSession(policy).status(totals(con.execute(TOTALS_SQL).fetchone()))
  else:ss={'required':False,'reason':None}
 stop=Path(os.environ['LIVE_STOP_FILE']).exists(); arm=Path(os.environ['LIVE_ARM_FILE'])
 fresh=arm.stat().st_mtime>=START-1;valid=arm.read_text().strip()=='ENABLE MEXC V247 LIVE 20 USD'
 effective=fresh and valid and not stop and not circuit and not ss.get('reason')
 lv={k:1 for k in __KEYS__}
 lv.update(configured_mode='live',cap_usd=20,capital_limit_usd=200,daily_loss_limit_usd=5,
  live_journal_path='local.db',entry_order_policy='FILL_OR_KILL',protected_bags=True,max_concurrent=1,
  busy=os.environ.get('FAKE_BUSY')=='1',queued=0,circuit_open=circuit,open_exposures=0,
  balances={'CTO':{'free':1,'locked':0}} if os.environ.get('FAKE_CTO')=='1' else {},
  private_order_stream={'pending_orders':0},api_ok=True,validated_routes=1,
  session=ss,excluded_assets=os.environ.get('LIVE_EXCLUDED_ASSETS','').split(','),
  max_consecutive_unwinds=int(os.environ.get('LIVE_MAX_CONSECUTIVE_UNWINDS',2)),
  arm={'stop_file':stop,'private_ws_ready':True,'effective':effective,'real_orders':effective,
       'reason':'stop_file' if stop else ('live_triple_gate_open' if effective else 'blocked')})
 bad=LIVE_SESSION_REQUIRED and os.environ.get('FAKE_BAD_FLOW')=='1'
 if LIVE_SESSION_REQUIRED and os.environ.get('FAKE_BAD_LIMIT')=='1':lv['bbo_age_ms']=999
 return dict(version=VERSION,live=lv,ws=1,ws_expected=1,depth_ready=1,symbols=1,
   health={'ws_reconnects':0},public_flow={'protobuf_backend':'upb','workers':[
   {'active':True,'catching_up':bad,'queue_age_ms':30000 if bad else 0}]})
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  self.send_response(200);self.end_headers();self.wfile.write(json.dumps(data()).encode())
 def log_message(self,*a):pass
if __name__=='__main__':
 if LIVE_SESSION_REQUIRED and os.environ.get('FAKE_EXIT')=='1':raise SystemExit(3)
 if LIVE_SESSION_REQUIRED and os.environ.get('FAKE_TOUCH_STOP')=='1':os.utime(os.environ['LIVE_STOP_FILE'],None)
 Path('env_hash_'+str(os.getpid())).write_text(hashlib.sha256(os.environ['MEXC_API_SECRET'].encode()).hexdigest())
 HTTPServer.allow_reuse_address=True
 HTTPServer(('127.0.0.1',int(os.environ['PORT'])),H).serve_forever()
'''.replace('__KEYS__',repr(r.LIMIT_KEYS))


class ResumeTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.cwd=Path.cwd();self.children=[]
  self.addCleanup(self.cleanup)
  for target,values in ((accounting,fixture.SYNTHETIC_PARAMETERS),(r,{'ATTEMPT':'synthetic_cto_attempt'})):
   patcher=patch.multiple(target,**values);patcher.start();self.addCleanup(patcher.stop)
  app=load_offline(HERE/'app.py');app.LIVE_DB_PATH=str(self.root/'local.db');app.initialize_live_journal()
  for table,rows in json.loads((HERE/'cto_journal_fixture_v2411.json').read_text()).items():
   for row in rows:
    keys=list(row);app.live_db_connection.execute('INSERT INTO '+table+'('+','.join(keys)+') VALUES('+','.join('?' for _ in keys)+')',[row[k] for k in keys])
  app.live_db_connection.commit();app.live_db_connection.close()
  accounting.reconcile(self.root/'local.db',apply=True)
  import sqlite3
  with sqlite3.connect(self.root/'local.db') as con:
   con.execute('UPDATE live_circuit_events_v246 SET active=0')
   con.execute('UPDATE live_state_v240 SET circuit_open=0,circuit_reason=NULL')
  (self.root/'session_live_v2412.py').write_bytes((HERE/'session_live_v2412.py').read_bytes())
  with socket.socket() as s:s.bind(('127.0.0.1',0));self.port=s.getsockname()[1]
  self.stop=self.root/'STOP';self.stop.write_text('prior stop')
  self.arm=self.root/'ARM';self.arm.write_text('ENABLE MEXC V247 LIVE 20 USD\n')
  self.env=dict(os.environ,PORT=str(self.port),TRADING_MODE='live',LIVE_ARMED='1',LIVE_STOP_FILE=str(self.stop),LIVE_ARM_FILE=str(self.arm),MEXC_API_SECRET='dummy-secret-not-to-print')
 def cleanup(self):
  for c in self.children:
   if c.poll() is None:c.terminate()
   c.wait(timeout=3)
  os.chdir(self.cwd);self.tmp.cleanup()
 def start(self,active_version=None,**flags):
  self.env.update(flags)
  (self.root/'app.py').write_text(FAKE.replace('__VERSION__',active_version or r.OLD_VERSION))
  self.old=subprocess.Popen([sys.executable,'-u','app.py'],cwd=self.root,env=self.env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
  self.children.append(self.old);self.read=r.status_reader(self.port)
  for _ in range(100):
   try:self.read();break
   except OSError:time.sleep(.02)
  else:self.fail('Fake scanner did not start')
  (self.root/'app.py').write_text(FAKE.replace('__VERSION__',r.VERSION))
 def run_resume(self,apply=True,budget=5):
  root=self.root;env=self.env;old=self.old;realpath=Path;self.output=io.StringIO()
  # Managed execution virtualizes child PIDs. Adapt /proc+pidfd only;
  # subprocesses, HTTP, SQLite, env inheritance and actual files are exercised.
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
  with contextlib.redirect_stdout(self.output),contextlib.redirect_stderr(self.output),patch.object(r,'Path',path),patch.object(r.os,'pidfd_open',lambda pid:os.open(os.devnull,os.O_RDONLY)),patch.object(r.signal,'pidfd_send_signal',terminate),patch.object(r.select,'select',lambda a,b,c,t:(a if old.poll() is not None else [],[],[])),patch.object(r.subprocess,'Popen',popen):
   return r.resume(old.pid,root,sys.executable,budget=budget,minutes=30,apply=apply,drain_timeout=.7,startup_timeout=2,stable_seconds=0,watch=False)
 def test_preview_changes_no_gate_circuit_or_process(self):
  self.start();mtime=self.arm.stat().st_mtime_ns
  self.run_resume(apply=False,budget=None)
  self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
  self.assertEqual(self.arm.stat().st_mtime_ns,mtime);self.assertFalse(self.read()['live']['circuit_open'])
 def test_explicit_session_arms_only_after_acknowledgement_and_health(self):
  self.start();self.run_resume();d=self.read();lv=d['live']
  self.assertFalse(self.stop.exists());self.assertTrue(lv['arm']['real_orders'])
  self.assertEqual(lv['session']['pnl'],0);self.assertAlmostEqual(lv['session']['baseline_realized_pnl'],-12.004)
  self.assertEqual(lv['session']['additional_loss_usd'],5)
  self.assertEqual(lv['excluded_assets'],['CTO']);self.assertEqual(lv['max_consecutive_unwinds'],1)
  self.assertNotIn(self.env['MEXC_API_SECRET'],self.output.getvalue())
  newpid=int((self.root/'scanner_v2413_live.pid').read_text())
  self.assertEqual((self.root/f'env_hash_{newpid}').read_text(),hashlib.sha256(self.env['MEXC_API_SECRET'].encode()).hexdigest())
 def test_missing_explicit_loss_budget_leaves_process_and_stop(self):
  self.start()
  with self.assertRaises(r.RestartError):self.run_resume(budget=None)
  self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
 def test_expired_session_is_retained_and_new_session_has_unchanged_history(self):
  import sqlite3,uuid
  db=self.root/'local.db'
  prior,_=r.allocate(db,self.root,uuid.uuid4().hex,5,1,r.read_plan(db))
  prior['created_ts_ms']=int(time.time()*1000)-120000
  prior['expires_ts_ms']=prior['created_ts_ms']+60000
  with sqlite3.connect(db) as con:
   con.execute('UPDATE live_rearm_sessions_v2412 SET policy_json=?',(json.dumps(prior),))
  self.start(LIVE_SESSION_REQUIRED='1',LIVE_SESSION_ID=prior['session_id'])
  self.assertEqual(self.read()['live']['session']['reason'],'session_expired')
  self.run_resume()
  ss=self.read()['live']['session']
  self.assertNotEqual(ss['session_id'],prior['session_id'])
  self.assertEqual(ss['baseline_realized_pnl'],prior['baseline']['pnl'])
  self.assertIsNone(ss['reason']);self.assertEqual(ss['buys'],0)
  with sqlite3.connect(db) as con:
   rows=con.execute('SELECT session_id,policy_json FROM live_rearm_sessions_v2412').fetchall()
  self.assertEqual(len(rows),2)
  self.assertEqual(json.loads(dict(rows)[prior['session_id']]),prior)
 def test_active_circuit_is_not_acknowledged_or_restarted(self):
  import sqlite3
  with sqlite3.connect(self.root/'local.db') as con:
   con.execute('UPDATE live_circuit_events_v246 SET active=1')
   con.execute("UPDATE live_state_v240 SET circuit_open=1,circuit_reason='residual_intermediate_asset'")
  self.start();mtime=self.arm.stat().st_mtime_ns
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
  self.assertEqual(self.arm.stat().st_mtime_ns,mtime)
  self.assertTrue(self.read()['live']['circuit_open'])
 def test_different_active_version_leaves_process_and_stop(self):
  self.start(active_version='2.4.12-bounded-live-session')
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
 def test_busy_order_preserves_old_scanner(self):
  self.start(FAKE_BUSY='1')
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
 def test_bad_public_flow_retains_stop(self):
  self.start(FAKE_BAD_FLOW='1')
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertTrue(self.stop.exists());self.assertFalse(self.read()['live']['arm']['real_orders'])
 def test_changed_freshness_limit_retains_stop(self):
  self.start(FAKE_BAD_LIMIT='1')
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertTrue(self.stop.exists())
 def test_remaining_cto_retains_stop(self):
  self.start(FAKE_CTO='1')
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertTrue(self.stop.exists())
 def test_new_startup_failure_retains_stop(self):
  self.start(FAKE_EXIT='1')
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertTrue(self.stop.exists())
 def test_user_stop_touched_during_restart_is_not_removed(self):
  self.start(FAKE_TOUCH_STOP='1')
  with self.assertRaises(r.RestartError):self.run_resume()
  self.assertTrue(self.stop.exists());self.assertFalse(self.read()['live']['arm']['real_orders'])
 def test_monitor_closes_entries_and_exports_settled_accounting(self):
  self.start();self.run_resume()
  d=self.read();session=d['live']['session']['session_id']
  policy=json.loads(__import__('sqlite3').connect(self.root/'local.db').execute('SELECT policy_json FROM live_rearm_sessions_v2412').fetchone()[0])
  def read():
   current=self.read();current['live']['session']['reason']='session_expired';return current
  with contextlib.redirect_stdout(io.StringIO()):
   r.monitor(read,self.stop,self.root,session,[],self.root/'local.db',policy['created_ts_ms'])
  self.assertTrue(self.stop.exists())
  files=list(self.root.glob('bilan_live_session_*.json.gz'));self.assertEqual(len(files),1)
  import gzip
  with gzip.open(files[0],'rt') as f:report=json.load(f)
  self.assertTrue(report['settled']);self.assertEqual(report['journal']['attempts'],[])
  self.assertEqual(report['flow_counts']['ws_reconnects'],0)
 def test_monitor_http_failure_preserves_stop_and_writes_partial_report(self):
  self.start();self.run_resume()
  d=self.read();session=d['live']['session']['session_id']
  def unavailable():raise TimeoutError('test unavailable')
  with contextlib.redirect_stdout(io.StringIO()),self.assertRaises(TimeoutError):
   r.monitor(unavailable,self.stop,self.root,session,[],self.root/'local.db',int(time.time()*1000))
  self.assertTrue(self.stop.exists())
  self.assertEqual(len(list(self.root.glob('bilan_live_session_*.json.gz'))),1)


if __name__=='__main__':unittest.main(verbosity=2)
