"""Bounded receive queues, one fair decoder, per-symbol reconstruction."""
from collections import deque, Counter
from concurrent.futures import ThreadPoolExecutor
import json
import random
import threading
import time
from .codec import decode_depth
from .heartbeat import Heartbeat


class Channel:
    def __init__(self, number, symbols):
        self.number, self.symbols = number, symbols
        self.epoch = 0
        self.connected = False
        self.queue = deque()
        self.bytes = 0
        self.lock = threading.Lock()
        self.ws = None
        self.catchup = False
        self.inflight = 0
        self.progress = time.monotonic()
        self.lag_since = None
        self.last_receive = 0
        self.messages = 0
        self.reconnections = 0
        self.failed_epoch = None
        self.heartbeat = None


class PublicFeed:
    def __init__(self, books, rest, groups, clock):
        self.books, self.rest, self.clock = books, rest, clock
        self.channels = [Channel(i+1, group) for i,group in enumerate(groups)]
        self.done = threading.Event()
        self.wake = threading.Event()
        self.recoveries = deque(maxlen=200)
        self.errors = deque(maxlen=50)
        self.totals = Counter()
        self.resync_pending = set()
        self.resync_lock = threading.Lock()
        self.resync_workers = ThreadPoolExecutor(max_workers=3, thread_name_prefix="snapshot")
        self.threads = []

    def start(self):
        for target, args in [(self.dispatch,()),(self.maintain,())]+[(self.connect,(c,)) for c in self.channels]:
            t = threading.Thread(target=target,args=args,daemon=True)
            t.start()
            self.threads.append(t)

    def receive(self, c, raw, epoch):
        received = self.clock.ms()
        with c.lock:
            if c.epoch != epoch or not c.connected:
                return
        if not isinstance(raw, bytes):
            try:
                msg = json.loads(raw)
                if msg.get("msg") == "PONG" and c.heartbeat:
                    c.heartbeat.pong()
                    self.totals["app_pongs"] += 1
                if msg.get("code",0) not in (0,None):
                    self.errors.append({"channel":c.number,"error":"subscription_rejected","response":msg})
                    self.recover(c, "subscription_rejected")
            except (ValueError, TypeError):
                pass
            return
        overflow = False
        with c.lock:
            if c.epoch != epoch or not c.connected:
                return
            if len(c.queue) >= 1024 or c.bytes+len(raw) > 4*1024*1024:
                overflow = True
            else:
                if not c.queue and not c.inflight:
                    c.progress = time.monotonic()
                c.queue.append((raw,received,time.monotonic(),epoch))
                c.bytes += len(raw)
                c.last_receive = received
        if overflow:
            self.recover(c, "receive_queue_full",epoch=epoch)
        self.wake.set()

    def recover(self, c, reason, lag=None, epoch=None):
        with c.lock:
            if epoch is not None and epoch != c.epoch:
                return
            if c.failed_epoch == c.epoch:
                return
            c.failed_epoch = c.epoch
            c.connected = False
            record = {"ts_ms":self.clock.ms(),"channel":c.number,"epoch":c.epoch,
                "reason":reason,"queue":len(c.queue),"lag_ms":lag}
            c.queue.clear()
            c.bytes = 0
            failed_epoch,ws = c.epoch,c.ws
        self.totals[reason] += 1
        self.recoveries.append(record)
        self.books.connection(c.symbols, failed_epoch, False)
        if ws:
            ws.close()

    def connect(self, c):
        import websocket
        while not self.done.is_set():
            c.epoch += 1
            epoch = c.epoch
            connection_stop = threading.Event()
            def opened(ws):
                with c.lock:
                    c.connected = True
                    c.queue.clear()
                    c.bytes = 0
                    c.progress = time.monotonic()
                    c.lag_since = None
                self.books.connection(c.symbols, epoch, True)
                ws.send(json.dumps({"method":"SUBSCRIPTION","params":[
                    "spot@public.aggre.depth.v3.api.pb@10ms@"+s for s in c.symbols]}))
                c.heartbeat = Heartbeat(ws.send,phase=15*(c.number-1)/max(1,len(self.channels)))
                threading.Thread(target=c.heartbeat.run,args=(connection_stop,lambda reason:self.recover(c,reason,epoch=epoch)),
                                 daemon=True,name=f"public-ping-{c.number}").start()
            def error(ws, exc):
                self.errors.append({"ts_ms":self.clock.ms(),"channel":c.number,"error":type(exc).__name__})
            c.ws = websocket.WebSocketApp("wss://wbs-api.mexc.com/ws",on_open=opened,
                on_message=lambda ws,raw:self.receive(c,raw,epoch),on_error=error)
            try:
                # websocket-client still answers RFC PING automatically. MEXC
                # liveness is the JSON heartbeat above, without RFC ping timers.
                c.ws.run_forever(ping_interval=0,skip_utf8_validation=True)
            except Exception as exc:
                error(c.ws,exc)
            finally:
                connection_stop.set()
                self.recover(c,"connection_closed",epoch=epoch)
                c.reconnections += 1
            self.done.wait(1+random.random()*2)

    def process_channel(self, c, quantum=32):
        processed = 0
        for _ in range(quantum):
            with c.lock:
                if not c.queue:
                    break
                raw, received, entered, epoch = c.queue.popleft()
                c.bytes -= len(raw)
                c.inflight = entered
            if (time.monotonic()-entered)*1000 > 50 and not c.catchup:
                c.catchup = True
                self.books.catching_up(c.symbols, True)
                self.totals["catchup_started"] += 1
            try:
                if epoch != c.epoch or not c.connected:
                    continue
                delta = decode_depth(raw, received)
                if delta:
                    if delta.symbol not in c.symbols:
                        raise ValueError("Wrong channel symbol")
                    lag = received+self.rest.offset_ms-delta.exchange_ms
                    if lag > 1000:
                        c.lag_since = c.lag_since or time.monotonic()
                        if time.monotonic()-c.lag_since > 2:
                            self.recover(c,"exchange_backlog",lag,epoch=epoch)
                            break
                    else:
                        c.lag_since = None
                    self.books.delta(delta, epoch)
                    c.messages += 1
                    processed += 1
            except Exception as exc:
                self.errors.append({"channel":c.number,"error":type(exc).__name__})
                self.recover(c,"decode_or_book_error",epoch=epoch)
                break
            finally:
                with c.lock:
                    c.inflight = 0
                    c.progress = time.monotonic()
        with c.lock:
            oldest = c.queue[0][2] if c.queue else None
        if c.catchup and (oldest is None or (time.monotonic()-oldest)*1000 <= 10):
            c.catchup = False
            self.books.catching_up(c.symbols, False)
            self.totals["catchup_completed"] += 1
        return processed

    def dispatch(self):
        start = 0
        while not self.done.is_set():
            self.wake.clear()
            work = 0
            for i in range(len(self.channels)):
                work += self.process_channel(self.channels[(start+i)%len(self.channels)])
            start = (start+1)%max(1,len(self.channels))
            if not work:
                self.wake.wait(.01)

    def resync(self, symbol):
        try:
            token = self.books.token(symbol)
            response = self.rest.depth(symbol)
            self.books.install(symbol,token,response)
        except Exception as exc:
            self.errors.append({"symbol":symbol,"error":"snapshot_"+type(exc).__name__})
        finally:
            with self.resync_lock:
                self.resync_pending.discard(symbol)

    def maintain(self):
        rotation = deque(self.books.slots)
        last_try = {}
        while not self.done.wait(.05):
            now = time.monotonic()
            for c in self.channels:
                with c.lock:
                    oldest = c.queue[0][2] if c.queue else c.inflight
                if oldest and (now-oldest)*1000 > 50:
                    if not c.catchup:
                        c.catchup = True
                        self.totals["catchup_started"] += 1
                    self.books.catching_up(c.symbols, True)
                    if now-c.progress > 5:
                        self.recover(c,"consumer_stalled")
            # Bounded task count, rotating unready symbols naturally on completion.
            for _ in range(len(rotation)):
                symbol = rotation[0]
                rotation.rotate(-1)
                s = self.books.slots[symbol]
                with s.lock:
                    needed = s.connected and not s.ready
                if needed:
                    with self.resync_lock:
                        if symbol in self.resync_pending or now-last_try.get(symbol,0) < 1:
                            continue
                        if len(self.resync_pending) >= 12:
                            break
                        self.resync_pending.add(symbol)
                        last_try[symbol] = now
                    self.resync_workers.submit(self.resync,symbol)

    def status(self):
        age = 0
        for c in self.channels:
            with c.lock:
                oldest = c.queue[0][2] if c.queue else c.inflight
            if oldest:
                age = max(age,(time.monotonic()-oldest)*1000)
        return {"connected":sum(c.connected for c in self.channels),"total":len(self.channels),
            "catchups":sum(c.catchup for c in self.channels),"queue_age_ms":round(age,2),
            "reconnections":sum(c.reconnections for c in self.channels),"counters":dict(self.totals),
            "app_ping_pong":[{"channel":c.number,"pings":c.heartbeat.pings,"pongs":c.heartbeat.pongs,
                "pong_age_ms":round((time.monotonic()-c.heartbeat.last_pong)*1000)} for c in self.channels if c.heartbeat],
            "recoveries":list(self.recoveries),"errors":list(self.errors)}

    def close(self):
        self.done.set()
        self.wake.set()
        for c in self.channels:
            if c.ws:
                c.ws.close()
        self.resync_workers.shutdown(wait=True,cancel_futures=True)
