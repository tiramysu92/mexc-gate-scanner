"""Deterministic resync races; no network, exchange orders or running scanner."""
import pathlib
import queue
import threading
import time
import types
import unittest
from test_public_flow import load_offline

HERE=pathlib.Path(__file__).resolve().parent


class ResyncTests(unittest.TestCase):
    def setUp(self):
        self.m=load_offline(HERE/'app.py')
        self.events=[]
        self.m.put_db=self.events.append
        self.m.now_ms=lambda:1000000
        self.m.mexc_now_ms=lambda:1000000
        self.m.state['symbols']=['XUSDT']
        self.m.state['depth']['XUSDT']={
            'bids':{99.0:1},'asks':{101.0:1},'version':100,
            'ready':False,'send_ts':999990,'ts':999990}
        self.m.queue_depth_resync('XUSDT','bootstrap')
        self.gets=0
        original_get=self.m.depth_resync_q.get
        def bounded_get():
            self.gets+=1
            if self.gets>5: raise queue.Empty
            return original_get(block=False)
        self.m.depth_resync_q.get=bounded_get
        self.m.time=types.SimpleNamespace(**{k:getattr(time,k) for k in dir(time) if not k.startswith('__')})
        self.m.time.sleep=lambda _:None

    def run_worker(self):
        with self.assertRaises(queue.Empty):self.m.depth_resync_worker()

    def gap(self):
        self.m.apply_depth_update({'symbol':'XUSDT','from':103,'to':103,
            'bids':[(99.0,2)],'asks':[],'send':999995,'received_ts':999995})

    def test_new_gap_during_post_snapshot_pause_is_not_lost(self):
        m=self.m;calls=[];paused=[]
        def init(sym):
            calls.append(sym)
            m.state['depth'][sym].update(ready=True,version=100 if len(calls)==1 else 103)
            return True
        def pause(seconds):
            if seconds==.25 and not paused:
                paused.append(True)
                thread=threading.Thread(target=self.gap)
                thread.start();thread.join(1)
                self.assertFalse(thread.is_alive())
        m.init_depth_symbol=init;m.time.sleep=pause
        self.run_worker()
        self.assertEqual(len(calls),2,'The invalidated book lost its queued resynchronization')
        self.assertTrue(m.state['depth']['XUSDT']['ready'])
        self.assertNotIn('XUSDT',m.depth_resync_pending)
        self.assertEqual(m.depth_resync_q.unfinished_tasks,0)

    def test_new_gap_before_success_record_is_not_reported_as_ready(self):
        m=self.m;calls=[]
        def init(sym):
            calls.append(sym)
            m.state['depth'][sym].update(ready=True,version=100 if len(calls)==1 else 103)
            if len(calls)==1:self.gap()
            return True
        m.init_depth_symbol=init
        self.run_worker()
        self.assertEqual(len(calls),2)
        ready=[row for typ,row in self.events if typ=='depth_sync' and row[2]=='READY']
        self.assertEqual(len(ready),1,'Obsolete success must not overwrite a newer invalidation')
        self.assertTrue(m.state['depth']['XUSDT']['ready'])

    def test_duplicate_pending_requests_remain_one_queue_item(self):
        m=self.m
        for _ in range(100):m.queue_depth_resync('XUSDT','version_or_book_invalid')
        self.assertEqual(m.depth_resync_q.qsize(),1)
        m.init_depth_symbol=lambda _:True
        self.run_worker()
        self.assertFalse(m.depth_resync_pending)
        self.assertEqual(m.depth_resync_q.unfinished_tasks,0)

    def test_failed_symbol_goes_to_tail_and_does_not_starve_other_symbol(self):
        m=self.m;calls=[]
        m.state['depth']['YUSDT']={'ready':False}
        m.queue_depth_resync('YUSDT','bootstrap')
        def init(sym):
            calls.append(sym)
            if sym=='XUSDT' and calls.count(sym)<=2:return False
            m.state['depth'][sym]['ready']=True
            return True
        m.init_depth_symbol=init
        self.run_worker()
        self.assertEqual(calls,['XUSDT','XUSDT','YUSDT','XUSDT'])
        self.assertFalse(m.depth_resync_pending)


if __name__=='__main__':unittest.main(verbosity=2)
