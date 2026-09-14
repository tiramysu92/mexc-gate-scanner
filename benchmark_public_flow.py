"""Synthetic local benchmark. Does not run the scanner or place orders."""
import copy
import json
import pathlib
import statistics
import time
from test_public_flow import load_offline, packet, reference_merge

m = load_offline(pathlib.Path(__file__).with_name('app.py'))
m._initialize_depth_codec()
result = {'scope': 'synthetic, local, not full scanner throughput',
          'backend': m.PUBLIC_PROTOBUF_BACKEND, 'decode': {}, 'book': {}}

def measure(fn, repeat=2000):
    times=[]
    for _ in range(5):
        start=time.perf_counter_ns()
        for _ in range(repeat):fn()
        times.append((time.perf_counter_ns()-start)/repeat/1000)
    return statistics.median(times)

for n in (5,25,100):
    raw=packet('XUSDT',1000000,bids=tuple((99-i*.001,1) for i in range(n)),
               asks=tuple((101+i*.001,1) for i in range(n)))
    result['decode'][n] = {name:measure(lambda f=fn:f(raw)) for name,fn in
        [('python_us',m._decode_depth_python),('native_us',m.decode_depth),('header_us',m._public_depth_header)]}

for n in (100,1000):
    base={'bids':{100-i*.001:1. for i in range(n)},'asks':{101+i*.001:1. for i in range(n)}}
    old=copy.deepcopy(base);new=copy.deepcopy(base)
    delta={'bids':[(99.95,3.)],'asks':[(101.05,3.)],'to':101,'send':999,'received_ts':1000}
    def before():
        reference_merge(old,delta);max(old['bids']);min(old['asks'])
    def after():
        m._merge_depth_delta(new,delta);m._book_top(new)
    result['book'][n]={'old_us':measure(before),'new_us':measure(after),
                       'condition':'quantity changes; no best-level deletion'}
print(json.dumps(result,ensure_ascii=False,indent=2))
