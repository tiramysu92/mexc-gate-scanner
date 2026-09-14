"""Public WebSocket closure accounting with fake sockets, no network."""
import contextlib
import io
import pathlib
import sqlite3
import tempfile
import types
import unittest
from test_public_flow import load_offline

HERE=pathlib.Path(__file__).resolve().parent


class SocketTests(unittest.TestCase):
    def setUp(self):
        self.m=load_offline(HERE/'app.py');self.rows=[];self.m.put_db=self.rows.append
        self.m.state['symbols']=['XUSDT'];self.m.state['ws_expected']=1
        self.m.now_ms=lambda:1000000
        self.m.mexc_now_ms=lambda:1000000
        self.m._wait_public_reconnect_slot=lambda:None
        self.m.time=types.SimpleNamespace(monotonic=lambda:1.0,perf_counter_ns=lambda:1)
        self.m.time.sleep=lambda _:None
        self.m.threading=types.SimpleNamespace(**vars(self.m.threading))
        self.m.threading.Thread=lambda *a,**k:types.SimpleNamespace(start=lambda:None)

    def run_sockets(self,scenario,count=1):
        m=self.m;opened=[]
        class Socket:
            def __init__(self,url,**callbacks):self.callbacks=callbacks;self.sent=[]
            def send(self,message):self.sent.append(message)
            def close(self):pass
            def run_forever(self,**kwargs):
                opened.append(self);self.callbacks['on_open'](self)
                scenario(self,m)
        m.websocket.WebSocketApp=Socket
        def pause(_):
            if len(opened)>=count:raise StopIteration
        m.time.sleep=pause
        with contextlib.redirect_stdout(io.StringIO()),self.assertRaises(StopIteration):
            m.ws_worker(['XUSDT'],1)
        return opened

    def test_transport_error_survives_reopen_and_is_counted_once(self):
        def scenario(s,m):
            s.callbacks['on_message'](s,'{"code":0,"msg":"PONG"}')
            s.callbacks['on_error'](s,RuntimeError('Connection to remote host was lost.'))
            s.callbacks['on_close'](s,None,None)
        self.run_sockets(scenario,count=2)
        events=list(self.m.state['public_ws_close_events'])
        self.assertEqual(len(events),2)
        self.assertEqual([e['epoch'] for e in events],[1,2])
        self.assertEqual(events[0]['last_error'],'Connection to remote host was lost.')
        self.assertIsNone(events[0]['local_reason'])
        self.assertEqual(events[0]['last_pong_ms'],1000000)
        self.assertEqual(self.m.state['ws_disconnects'],2)
        self.assertEqual(self.m.state['ws_reconnects'],1)
        self.assertEqual(len([r for r in self.rows if r[0]=='public_ws_close_v2413']),2)

    def test_local_recovery_and_control_error_are_preserved(self):
        def scenario(s,m):
            s.callbacks['on_message'](s,'{"code":30001,"msg":"Unknown symbol"}')
            m.public_feeds[1].fail('receive_queue_full')
            s.callbacks['on_close'](s,None,None)
        self.run_sockets(scenario)
        event=self.m.state['public_ws_close_events'][-1]
        self.assertEqual(event['local_reason'],'receive_queue_full')
        self.assertEqual(event['subscription_error']['code'],30001)
        self.assertEqual(self.m.state['ws_disconnects'],1)

    def test_exit_without_on_close_preserves_evidence_once(self):
        def scenario(s,m):
            s.callbacks['on_error'](s,RuntimeError('Transport ended'))
        self.run_sockets(scenario)
        event=self.m.state['public_ws_close_events'][-1]
        self.assertEqual(event['source'],'run_forever_exit')
        self.assertEqual(event['last_error'],'Transport ended')
        self.assertEqual(self.m.state['ws_disconnects'],1)

    def test_ping_callback_observes_without_sending_second_pong(self):
        def scenario(s,m):
            before=list(s.sent)
            s.callbacks['on_ping'](s,b'ping')
            self.assertEqual(s.sent,before)
            s.callbacks['on_close'](s,1000,'test')
        self.run_sockets(scenario)
        self.assertEqual(self.m.state['public_ws_close_events'][-1]['last_server_ping_ms'],1000000)

    def test_health_identifies_the_actual_unready_book_and_queue_membership(self):
        m=self.m
        m.state['symbols']=['XUSDT','YUSDT']
        m.state['depth']={'XUSDT':{'ready':False,'version':100},'YUSDT':{'ready':True,'version':500,'ts':999900}}
        m.queue_depth_resync('XUSDT','version_or_book_invalid')
        h=m.runtime_health(1000000)
        self.assertEqual(len(h['depth_not_ready']),1)
        self.assertEqual(h['depth_not_ready'][0]['symbol'],'XUSDT')
        self.assertTrue(h['depth_not_ready'][0]['pending'])

    def test_public_closure_reaches_the_research_database(self):
        m=self.m
        def scenario(s,m):s.callbacks['on_close'](s,1000,'closure evidence')
        self.run_sockets(scenario)
        row=next(row for row in self.rows if row[0]=='public_ws_close_v2413')
        pending=[row]
        def get(*a,**kw):
            if pending:return pending.pop()
            return None
        m.dbq=types.SimpleNamespace(get=get,task_done=lambda:None)
        m.DB_COMMIT_BATCH=1
        with tempfile.TemporaryDirectory() as tmp:
            m.DB_PATH=str(pathlib.Path(tmp)/'research.db')
            m.db_writer()
            with sqlite3.connect(m.DB_PATH) as con:
                saved=con.execute('SELECT worker_id,epoch,event_json FROM public_ws_close_events_v2413').fetchone()
            self.assertEqual(saved[:2],(1,1))
            self.assertIn('closure evidence',saved[2])


if __name__=='__main__':unittest.main(verbosity=2)
