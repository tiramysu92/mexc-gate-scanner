"""Offline correctness tests using native Protobuf; no network or bot startup."""
import ast
import collections
import copy
import hashlib
import json
import os
import pathlib
import random
import threading
import time
import types
import unittest
from unittest.mock import patch

HERE = pathlib.Path(__file__).resolve().parent


def load_offline(path):
    tree = ast.parse(pathlib.Path(path).read_text())
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            node.names = [a for a in node.names if a.name not in ('requests', 'websocket')]
            if not node.names: continue
        if isinstance(node, ast.ImportFrom) and node.module == 'flask': continue
        if isinstance(node, ast.If) and '__name__' in ast.unparse(node.test): continue
        nodes.append(node)
    class App:
        def __init__(self, *a): pass
        def get(self, *a): return lambda fn: fn
    def forbidden(*a, **kw): raise AssertionError('Network/startup forbidden in offline test')
    m = types.ModuleType('offline_mexc')
    m.__dict__.update(Flask=App, jsonify=lambda x:x, render_template_string=lambda x:x,
                      requests=types.SimpleNamespace(get=forbidden, Session=forbidden),
                      websocket=types.SimpleNamespace(WebSocketApp=forbidden))
    with patch.dict(os.environ, {}, clear=True):
        exec(compile(ast.Module(body=nodes,type_ignores=[]), str(path), 'exec'),m.__dict__)
    m.put_db=lambda x:None
    m.enqueue_scan=lambda x:None
    return m


def varint(n):
    out=bytearray()
    while n>=128: out.append((n&127)|128); n>>=7
    out.append(n); return bytes(out)


def field(n,value):
    if isinstance(value,int):return varint(n<<3)+varint(value)
    value=value.encode() if isinstance(value,str) else value
    return varint(n<<3|2)+varint(len(value))+value


def packet(symbol, sent, start=101, end=101, bids=((99,2),), asks=()):
    body=b''.join(field(1,field(1,str(p))+field(2,str(q))) for p,q in asks)
    body+=b''.join(field(2,field(1,str(p))+field(2,str(q))) for p,q in bids)
    body+=field(4,str(start))+field(5,str(end))
    return field(1,'spot@public.aggre.depth.v3.api.pb@10ms@'+symbol)+field(3,symbol)+field(6,sent)+field(313,body)


def reference_merge(ob,x):
    for side in ('asks','bids'):
        for price,qty in x[side]:
            if qty==0:ob[side].pop(price,None)
            else:ob[side][price]=qty
    ob.update(version=x['to'],send_ts=x['send'],ts=x['received_ts'])
    ob.pop('_rows',None)
    return bool(ob['bids'] and ob['asks'] and max(ob['bids'])<min(ob['asks']))


class FlowTests(unittest.TestCase):
    def setUp(self):
        self.m=load_offline(HERE/'app.py')
        self.m._initialize_depth_codec()
        self.m.now_ms=lambda:1000000
        self.m.mexc_now_ms=lambda:1000005
        self.feed=self.m.PublicFeed(['XUSDT'],1,1)
        self.m.public_feeds[1]=self.feed
        self.m.public_symbol_feeds['XUSDT']=self.feed
        self.feed.active=True
        self.m.state['depth']['XUSDT']={'bids':{99.0:1},'asks':{101.0:1},
            'version':100,'ready':True,'send_ts':999990,'ts':999995}

    def test_official_envelope_decode_matches_legacy_decoder(self):
        raw=packet('牛来USDT',999980,bids=((99,2),(98,0)),asks=((101,3),))
        h=self.m._public_depth_header(raw);x=self.m.decode_depth(raw)
        self.assertEqual(h,('牛来USDT',999980))
        self.assertEqual(x['bids'],[(99.,2.),(98.,0.)])
        self.assertEqual(x['asks'],[(101.,3.)])
        self.assertEqual((x['from'],x['to']),(101,101))

    def test_native_decoder_matches_python_on_valid_packets(self):
        for n in (0,1,25,100):
            raw=packet('牛来USDT',999980,bids=tuple((99-i*.001,2) for i in range(n)),asks=((101,3),))
            native=self.m.decode_depth(raw);legacy=self.m._decode_depth_python(raw)
            self.assertEqual(native,legacy)
        self.assertIn(self.m.PUBLIC_PROTOBUF_BACKEND,('upb','cpp'))

    def test_reader_does_not_decode_full_depth(self):
        self.m.decode_depth=lambda x: self.fail('Full decode in receiver')
        self.assertTrue(self.feed.accept(packet('XUSDT',999990),1000000,time.monotonic()))
        self.assertEqual(self.feed.inbox.qsize(),1)
        self.assertEqual(self.m.state['depth']['XUSDT']['version'],100)

    def test_batch_keeps_all_deltas_and_original_receive_time(self):
        seen=[];self.m._publish_depth_bbo=lambda sym:seen.append(sym)
        for v in (101,102,103):
            self.assertTrue(self.feed.accept(packet('XUSDT',999993,start=v,end=v,bids=((99,v),)),999995,time.monotonic()))
        while not self.feed.inbox.empty():self.feed.consume_batch(self.feed.inbox.get_nowait())
        ob=self.m.state['depth']['XUSDT']
        self.assertEqual(ob['version'],103);self.assertEqual(ob['bids'][99.0],103)
        self.assertEqual(ob['ts'],999995);self.assertEqual(ob['send_ts'],999993)
        self.assertEqual(self.feed.metrics['applied'],3)
        self.assertGreaterEqual(len(seen),1)

    def test_gap_invalidates_instead_of_skipping(self):
        queued=[];self.m.queue_depth_resync=lambda sym,why:queued.append((sym,why))
        self.feed.accept(packet('XUSDT',999990,start=103,end=103),1000000,time.monotonic())
        self.feed.consume_batch(self.feed.inbox.get_nowait())
        self.assertFalse(self.m.state['depth']['XUSDT']['ready'])
        self.assertEqual(queued,[('XUSDT','version_or_book_invalid')])

    def test_queue_overflow_closes_and_blocks_immediately(self):
        self.feed.inbox=self.m.queue.Queue(maxsize=1)
        raw=packet('XUSDT',999990)
        self.assertTrue(self.feed.accept(raw,1000000,time.monotonic()))
        self.assertFalse(self.feed.accept(raw,1000000,time.monotonic()))
        self.assertEqual(self.feed.reason,'receive_queue_full')
        self.assertFalse(self.m._public_symbol_usable('XUSDT'))
        self.assertEqual(self.m._snapshot_freshness({'XUSDT':{'ts':1000000,'send_ts':1000000}},50,100,50),'route_public_feed_recovering')

    def test_queue_residence_recovers(self):
        self.feed.accept(packet('XUSDT',999990),1000000,time.monotonic()-0.2)
        self.feed.consume_batch(self.feed.inbox.get_nowait())
        self.assertEqual(self.feed.reason,'receive_queue_age')
        self.assertEqual(self.feed.metrics['applied'],0)

    def test_persistent_exchange_lag_recovers_but_one_spike_does_not(self):
        raw=packet('XUSDT',400000);base=time.monotonic()
        self.assertTrue(self.feed.accept(raw,1000000,base))
        self.assertTrue(self.feed.accept(raw,1000000,base+.11))
        self.assertFalse(self.feed.accept(raw,1000000,base+.22))
        self.assertEqual(self.feed.reason,'exchange_backlog')

    def test_recovery_epoch_rejects_old_delta_and_old_cleanup(self):
        old=self.feed
        fresh=self.m.PublicFeed(['XUSDT'],1,2);fresh.active=True
        self.m.public_symbol_feeds['XUSDT']=fresh
        x=self.m.decode_depth(packet('XUSDT',999990));x['source_epoch']=1
        self.assertFalse(self.m.apply_depth_update(x))
        self.assertEqual(self.m.state['depth']['XUSDT']['version'],100)
        old.stop()
        self.assertTrue(self.m.state['depth']['XUSDT']['ready'])

    def test_rest_return_from_previous_epoch_cannot_replace_new_book(self):
        m=self.m
        class Response:
            def raise_for_status(self):pass
            def json(self):return {'lastUpdateId':999,'bids':[['98','1']],'asks':[['102','1']]}
        def get(*a,**kw):
            fresh=m.PublicFeed(['XUSDT'],1,2);fresh.active=True
            m.public_symbol_feeds['XUSDT']=fresh
            return Response()
        m.requests.get=get
        self.assertFalse(m.init_depth_symbol('XUSDT'))
        self.assertEqual(m.state['depth']['XUSDT']['version'],100)

    def test_rest_snapshot_without_exchange_timestamp_is_not_live_fresh(self):
        m=self.m
        m.requests.get=lambda *a,**kw:types.SimpleNamespace(raise_for_status=lambda:None,
            json=lambda:{'lastUpdateId':100,'bids':[['99','1']],'asks':[['101','1']]})
        self.assertTrue(m.init_depth_symbol('XUSDT'))
        ob=m.state['depth']['XUSDT']
        self.assertEqual(ob['send_ts'],0)
        self.assertEqual(m._snapshot_freshness({'XUSDT':ob},50,100,50),'route_exchange_timestamp_missing')
        x=m.decode_depth(packet('XUSDT',999990));x['source_epoch']=1
        self.assertTrue(m.apply_depth_update(x))
        self.assertIsNone(m._snapshot_freshness({'XUSDT':ob},50,100,50))

    def test_top_cache_equivalent_to_full_scan_randomized(self):
        rng=random.Random(483);new={'bids':{},'asks':{}};ref=copy.deepcopy(new)
        for v in range(2000):
            x={'bids':[(rng.randrange(1,120),rng.randrange(0,5)) for _ in range(3)],
               'asks':[(rng.randrange(80,200),rng.randrange(0,5)) for _ in range(3)],
               'to':v,'send':v+1,'received_ts':v+2}
            self.assertEqual(self.m._merge_depth_delta(new,x),reference_merge(ref,x))
            self.assertEqual(new['bids'],ref['bids']);self.assertEqual(new['asks'],ref['asks'])
            self.assertEqual(self.m._book_top(new),(max(ref['bids'],default=None),min(ref['asks'],default=None)))

    def test_bad_frame_fails_closed(self):
        self.assertFalse(self.feed.accept(b'\x1a\xff\xff\x01X',1000000,time.monotonic()))
        self.assertTrue(self.feed.halt.is_set())

    def test_all_five_user_cases_rejected_in_live_and_shadow(self):
        for a,s,e,k in [(34,14,123896,123849),(36,30,181297,83971),(54,4,539617,539544),(47,11,527939,527875),(137,43,526057,525906)]:
            books={'a':{'ts':1000000-a,'send_ts':1000005-e},'b':{'ts':1000000-a+s,'send_ts':1000005-e+k}}
            self.assertIsNotNone(self.m._snapshot_freshness(books,50,100,50))
            self.assertEqual(self.m._research_exchange_reason({'exchange_ts_complete':True,'exchange_max_age':e,'exchange_skew':k}),'exchange_depth_stale')

    def test_watchdog_blocks_admission_before_depth_lock_is_available(self):
        self.feed.accept(packet('XUSDT',999990),1000000,time.monotonic()-.2)
        with self.m.depth_lock:
            worker=threading.Thread(target=self.feed.watchdog,daemon=True);worker.start()
            self.assertTrue(self.feed.halt.wait(.5))
            self.assertFalse(self.m._public_symbol_usable('XUSDT'))
        worker.join(.5)

    def test_threaded_healthy_stream_progresses_without_recovery(self):
        threads=[threading.Thread(target=fn,daemon=True) for fn in (self.feed.run,self.feed.watchdog)]
        for thread in threads:thread.start()
        try:
            for v in range(101,301):
                self.assertTrue(self.feed.accept(packet('XUSDT',999995,start=v,end=v),1000000,time.monotonic()))
                time.sleep(.0005)
            deadline=time.monotonic()+2
            while self.feed.metrics['applied']<200 and time.monotonic()<deadline:time.sleep(.002)
            self.assertEqual(self.feed.metrics['applied'],200)
            self.assertEqual(self.m.state['depth']['XUSDT']['version'],300)
            self.assertIsNone(self.feed.reason)
        finally:
            self.feed.stop()
            for thread in threads:thread.join(.5)

    def test_private_limits_and_default_control_unchanged(self):
        for key,value in {'LIVE_CAP_USD':20.,'LIVE_CAPITAL_LIMIT_USD':200.,'LIVE_DAILY_LOSS_LIMIT_USD':5.,
                          'LIVE_BBO_AGE_MS':50,'LIVE_EXCHANGE_AGE_MS':100,'LIVE_MAX_EXCHANGE_SKEW_MS':50,
                          'LIVE_LEG2_BBO_AGE_MS':100,'LIVE_LEG2_EXCHANGE_AGE_MS':150,
                          'TRADING_MODE':'shadow','LIVE_ARMED':False,'LIVE_RESET_CIRCUIT':False,
                          'LIVE_ARM_PHRASE':'ENABLE MEXC V247 LIVE 20 USD'}.items():
            self.assertEqual(getattr(self.m,key),value,key)


if __name__=='__main__':unittest.main(verbosity=2)
