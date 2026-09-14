"""Integration checks of restart orchestration against local fake processes."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import types

spec=importlib.util.spec_from_file_location('restart',Path(__file__).resolve().parents[1]/'relancer_mexc_v249.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
FAKE=r'''
import os,json,time,hashlib
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
VERSION = '__VERSION__'
PUBLIC_PROTOBUF_BACKEND='upb'
START=time.time()
def _initialize_depth_codec():pass

def data():
 stop=Path(os.environ['LIVE_STOP_FILE']).exists()
 arm=Path(os.environ['LIVE_ARM_FILE'])
 fresh=arm.stat().st_mtime>=START-1
 armed=os.environ.get('LIVE_ARMED')=='1'
 circuit=os.environ.get('FAKE_CIRCUIT')=='1'
 effective=armed and fresh and not stop and not circuit
 lv={k:1 for k in __KEYS__}
 lv.update(configured_mode='live',cap_usd=20,capital_limit_usd=200,daily_loss_limit_usd=5,
  live_journal_path='local.db',entry_order_policy='FILL_OR_KILL',protected_bags=True,
  busy=os.environ.get('FAKE_BUSY')=='1',queued=0,circuit_open=circuit,
  private_order_stream={'pending_orders':0},
  arm={'env_armed':armed,'arm_file_ok':True,'arm_file_fresh':fresh,'stop_file':stop,
       'reason':'stop_file' if stop else ('live_triple_gate_open' if effective else 'blocked'),
       'effective':effective,'real_orders':effective})
 if VERSION.startswith('2.4.9') and os.environ.get('FAKE_DIFFERENT')=='1':lv['cap_usd']=999
 return {'version':VERSION,'live':lv,'public_flow':{'protobuf_backend':'upb'},
         'ws':1,'ws_expected':1,'depth_ready':1,'symbols':1}
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  payload=json.dumps(data()).encode();self.send_response(200);self.end_headers();self.wfile.write(payload)
 def log_message(self,*a):pass
if __name__=='__main__':
 if VERSION.startswith('2.4.9') and os.environ.get('FAKE_EXIT')=='1':raise SystemExit(3)
 Path('env_hash_'+str(os.getpid())).write_text(hashlib.sha256(os.environ['MEXC_API_SECRET'].encode()).hexdigest())
 HTTPServer.allow_reuse_address=True
 HTTPServer(('127.0.0.1',int(os.environ['PORT'])),H).serve_forever()
'''.replace('__KEYS__',repr(m.LIMIT_KEYS))

class RestartTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(prefix='mexc_restart_test_');self.root=Path(self.tmp.name)
  self.cwd=Path.cwd();self.children=[]
  with socket.socket() as s:s.bind(('127.0.0.1',0));self.port=s.getsockname()[1]
  self.stop=self.root/'STOP';self.arm=self.root/'ARM';self.arm.write_text('test arm')
  self.env=dict(os.environ,PORT=str(self.port),TRADING_MODE='live',LIVE_ARMED='1',
                LIVE_STOP_FILE=str(self.stop),LIVE_ARM_FILE=str(self.arm),MEXC_API_SECRET='dummy-secret-must-not-appear')
 def start(self,**flags):
  self.env.update(flags)
  (self.root/'app.py').write_text(FAKE.replace('__VERSION__','2.4.8-bounded-public-flow'))
  self.old=subprocess.Popen([sys.executable,'-u','app.py'],cwd=self.root,env=self.env,
                             stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
  self.children.append(self.old);self.read=m.status_reader(self.port)
  for _ in range(100):
   try:self.read();break
   except OSError:time.sleep(.02)
  else:self.fail('Fake old process did not start')
  os.utime(self.arm,None)
  (self.root/'app.py').write_text(FAKE.replace('__VERSION__',m.VERSION))
 def run_restart(self):
  self.out=io.StringIO();root=self.root;env=self.env;old=self.old;realpath=Path
  # This managed runtime virtualizes child PIDs but not /proc. Adapt only that
  # OS boundary; actual child startup, HTTP status, env and files are exercised.
  class Proc:
   def __init__(self,leaf=''):self.leaf=leaf
   def __truediv__(self,part):return Proc(part)
   def stat(self):return types.SimpleNamespace(st_uid=os.getuid())
   def resolve(self):return root
   def read_text(self):return '1 (python) '+' '.join(['S']+['0']*18+['12345'])
   def read_bytes(self):
    if self.leaf=='cmdline':return (sys.executable+'\0-u\0app.py\0').encode()
    if self.leaf=='environ':return b'\0'.join((k+'='+v).encode() for k,v in env.items())
    raise AssertionError(self.leaf)
  def path(*a):return Proc() if a==('/proc',) else realpath(*a)
  def terminate(*a):old.terminate();old.wait(timeout=3)
  original_popen=subprocess.Popen
  def popen(*a,**kw):
   child=original_popen(*a,**kw);self.children.append(child);return child
  with contextlib.redirect_stdout(self.out),contextlib.redirect_stderr(self.out), \
       patch.object(m,'Path',path), \
       patch.object(m.os,'pidfd_open',lambda pid:os.open(os.devnull,os.O_RDONLY)), \
       patch.object(m.signal,'pidfd_send_signal',terminate), \
       patch.object(m.select,'select',lambda r,w,x,t: (r if old.poll() is not None else [],[],[])), \
       patch.object(m.subprocess,'Popen',popen):
   return m.restart(self.old.pid,self.root,sys.executable,drain_timeout=1.2,stop_timeout=2,startup_timeout=2)
 def tearDown(self):
  for child in self.children:
   if child.poll() is None:child.terminate()
   child.wait(timeout=3)
  os.chdir(self.cwd);self.tmp.cleanup()
 def test_restart_preserves_env_and_resumes_existing_arm(self):
  self.start();d=self.run_restart()
  self.assertEqual(d['version'],m.VERSION);self.assertTrue(d['live']['arm']['effective'])
  self.assertFalse(self.stop.exists());self.assertIsNotNone(self.old.poll())
  newpid=int((self.root/'scanner_v249_live.pid').read_text())
  self.assertEqual((self.root/f'env_hash_{newpid}').read_text(),hashlib.sha256(self.env['MEXC_API_SECRET'].encode()).hexdigest())
  self.assertNotIn(self.env['MEXC_API_SECRET'],self.out.getvalue())
 def test_busy_trade_prevents_termination(self):
  self.start(FAKE_BUSY='1')
  with self.assertRaises(m.RestartError):self.run_restart()
  self.assertIsNone(self.old.poll());self.assertTrue(self.stop.exists())
  self.assertFalse((self.root/'scanner_v249_live.pid').exists())
 def test_failed_preflight_leaves_old_process_and_gate(self):
  self.start();(self.root/'app.py').write_text('raise RuntimeError("dummy-secret-must-not-appear")')
  with self.assertRaises(m.RestartError):self.run_restart()
  self.assertIsNone(self.old.poll());self.assertFalse(self.stop.exists())
  self.assertNotIn(self.env['MEXC_API_SECRET'],self.out.getvalue())
 def test_existing_stop_file_is_retained(self):
  self.start();self.stop.write_text('user stop')
  d=self.run_restart();self.assertEqual(self.stop.read_text(),'user stop')
  self.assertFalse(d['live']['arm']['effective'])
 def test_unarmed_environment_is_not_armed(self):
  self.start(LIVE_ARMED='0');mtime=self.arm.stat().st_mtime_ns
  d=self.run_restart();self.assertFalse(d['live']['arm']['env_armed'])
  self.assertEqual(self.arm.stat().st_mtime_ns,mtime)
 def test_new_startup_failure_keeps_entries_blocked(self):
  self.start(FAKE_EXIT='1')
  with self.assertRaises(m.RestartError):self.run_restart()
  self.assertTrue(self.stop.exists());self.assertIsNotNone(self.old.poll())
 def test_changed_limits_are_not_resumed(self):
  self.start(FAKE_DIFFERENT='1')
  with self.assertRaises(m.RestartError):self.run_restart()
  self.assertTrue(self.stop.exists());self.assertTrue(self.read()['live']['arm']['stop_file'])

if __name__=='__main__':unittest.main(verbosity=2)
