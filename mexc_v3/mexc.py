"""MEXC HTTP and private order adapters. No network or orders at import time."""
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, OrderedDict
from decimal import Decimal
import hashlib
import hmac
import json
import threading
import time
from urllib.parse import urlencode
from .model import D, ZERO, Market, Receipt, TERMINAL, Uncertain, GuardRejected
from .codec import decode_order, decode_deal
from .heartbeat import Heartbeat


class APIError(RuntimeError):
    pass


class Rest:
    def __init__(self, clock, key="", secret=""):
        import requests
        self.clock, self.key, self.secret = clock, key, secret
        self.sessions = {role:requests.Session() for role in ("public","snapshot0","snapshot1","snapshot2",
            "order0","order1","status","account","stream")}
        self.locks = {role:threading.Lock() for role in self.sessions}
        self.offset_ms = 0
        self.clock_uncertainty_ms = float("inf")
        self.time_checked_ms = 0
        self.snapshot_counter = 0
        self.rate_lock = threading.Lock()
        self.next_snapshot = 0

    def request(self, method, path, params=None, *, signed=False, role="public", guard=None):
        params = dict(params or {})
        waiting = time.monotonic_ns()
        with self.locks[role]:
            acquired = time.monotonic_ns()
            if guard:
                guard()
            guarded = time.monotonic_ns()
            headers = {}
            if self.key:
                headers["X-MEXC-APIKEY"] = self.key
            if signed:
                if not self.key or not self.secret:
                    raise GuardRejected("Missing API credentials")
                params.update(timestamp=self.clock.ms()+round(self.offset_ms),recvWindow=5000)
                encoded = urlencode(params)
                params["signature"] = hmac.new(self.secret.encode(),encoded.encode(),hashlib.sha256).hexdigest()
            signed_at = time.monotonic_ns()
            started = self.clock.ms()
            response = self.sessions[role].request(method,"https://api.mexc.com"+path,
                params=params,headers=headers,timeout=(3,5))
            finished = time.monotonic_ns()
            received_ms = self.clock.ms()
            # Never expose signed request URLs, response headers or credentials.
            if response.status_code != 200:
                try:
                    code = response.json().get("code","unknown")
                except ValueError:
                    code = "unknown"
                raise APIError(f"HTTP {response.status_code}, MEXC code {code}")
            data = response.json()
            if isinstance(data,dict) and data.get("code",0) not in (0,200,None):
                raise APIError(f"MEXC code {data['code']}")
        return data,{"request_started_ms":started,"queue_ms":(acquired-waiting)/1e6,
            "response_received_ms":received_ms,
            "guard_ms":(guarded-acquired)/1e6,"sign_ms":(signed_at-guarded)/1e6,
            "http_call_ms":(finished-signed_at)/1e6}

    def sync_time(self):
        best = None
        for _ in range(3):
            before = self.clock.ms()
            data,_ = self.request("GET","/api/v3/time")
            after = self.clock.ms()
            sample = (after-before,int(data["serverTime"])-(before+after)/2)
            if best is None or sample[0] < best[0]:
                best = sample
        self.offset_ms = best[1]
        self.clock_uncertainty_ms = best[0]/2
        self.time_checked_ms = self.clock.ms()

    def depth(self, symbol):
        # <= 8 snapshots/sec globally, with at most three in flight.
        with self.rate_lock:
            delay = max(0,self.next_snapshot-time.monotonic())
            self.next_snapshot = max(self.next_snapshot,time.monotonic())+.125
        self.clock.sleep(delay)
        role = "snapshot"+str(threading.get_ident()%3)
        return self.request("GET","/api/v3/depth",{"symbol":symbol,"limit":1000},role=role)[0]

    def markets(self):
        data,_ = self.request("GET","/api/v3/exchangeInfo")
        result = []
        self.market_rejections = []
        for row in data.get("symbols",[]):
            if str(row.get("status")) != "1" or row.get("isSpotTradingAllowed") is not True:
                continue
            try:
                side = int(row.get("tradeSideType",1))
                filters = {x["filterType"]:x for x in row.get("filters",[]) if "filterType" in x}
                lot = filters.get("LOT_SIZE",{})
                price_filter = filters.get("PRICE_FILTER",{})
                notional = filters.get("NOTIONAL",filters.get("MIN_NOTIONAL",{}))
                result.append(Market(row["symbol"],row["baseAsset"],row["quoteAsset"],
                    D(lot["stepSize"]) if lot.get("stepSize") else Decimal(1).scaleb(-int(row["baseAssetPrecision"])),
                    D(price_filter["tickSize"]) if price_filter.get("tickSize") else Decimal(1).scaleb(-int(row["quotePrecision"])),
                    max(D(row["baseSizePrecision"]),D(lot.get("minQty",0))),
                    max(D(row["quoteAmountPrecision"]),D(notional.get("minNotional",0))),
                    min(D(row.get("maxQuoteAmount") or "1e20"),D(notional.get("maxNotional") or "1e20")),
                    D(row["takerCommission"] if row.get("takerCommission") is not None else ".0005"),
                    side in (1,2),side in (1,3),max_quantity=D(lot.get("maxQty") or "1e30")))
            except (KeyError,ValueError,ArithmeticError) as exc:
                self.market_rejections.append({"symbol":row.get("symbol"),"reason":str(exc)})
                continue
        return result

    def account(self):
        data,_ = self.request("GET","/api/v3/account",signed=True,role="account")
        if data.get("canTrade") is not True:
            raise APIError("Account trading unavailable")
        return {r["asset"]:{"free":D(r["free"]),"locked":D(r["locked"])}
                for r in data.get("balances",[]) if D(r["free"])+D(r["locked"]) > 0}

    def order(self, spec):
        return self.request("GET","/api/v3/order",{"symbol":spec.symbol,"origClientOrderId":spec.client_id},
                            signed=True,role="status")[0]

    def fee(self, symbol):
        data,_ = self.request("GET","/api/v3/tradeFee",{"symbol":symbol},signed=True,role="account")
        fee = D(data["data"]["takerCommission"])
        if not 0 <= fee < 1:
            raise APIError("Invalid fee")
        return fee

    def test_order(self, spec):
        return self.request("POST","/api/v3/order/test",self.params(spec),signed=True,role="account")[0]

    @staticmethod
    def params(spec):
        return {"symbol":spec.symbol,"side":spec.side,"type":spec.order_type,
            "quantity":str(spec.quantity),"price":str(spec.price),"newClientOrderId":spec.client_id}

    def close(self):
        for session in self.sessions.values():
            session.close()


def parse_receipt(data, spec, source, timings=None):
    receipt = Receipt(str(data.get("clientOrderId","")),str(data.get("symbol","")),
        str(data.get("side","")).upper(),str(data.get("status","")).upper(),
        D(data.get("executedQty",-1)),D(data.get("cummulativeQuoteQty",data.get("cumulativeQuoteQty",-1))),
        str(data.get("orderId","")),source=source,timings=timings or {},raw=dict(data))
    return receipt.validate(spec)


class PrivateOrders:
    def __init__(self, rest):
        self.rest = rest
        self.ready = False
        self.done = threading.Event()
        self.condition = threading.Condition()
        self.expected = set()
        self.events = {}
        self.deals = OrderedDict()
        self.deal_conflicts = set()
        self.ws = None
        self.key = None
        self.heartbeat = None

    def register(self, spec):
        with self.condition:
            self.expected.add(spec.client_id)

    def take(self, spec):
        with self.condition:
            item = self.events.get(spec.client_id)
            if item:
                receipt = parse_receipt(item[0],spec,"private_ws",{"terminal_received_ms":item[1]})
                return self.decorate(receipt,spec)

    def deal(self, data):
        if not data.get("trade_id") or not data.get("order_id"):
            return
        with self.condition:
            old = self.deals.get(data["trade_id"])
            if old is not None and old != data:
                self.deal_conflicts.add(data["order_id"])
                return
            self.deals[data["trade_id"]] = data
            while len(self.deals) > 2000:
                _,removed = self.deals.popitem(last=False)
                self.deal_conflicts.discard(removed["order_id"])

    def decorate(self, receipt, spec):
        with self.condition:
            if receipt.order_id in self.deal_conflicts:
                return receipt
            rows = [r for r in self.deals.values() if r["order_id"] == receipt.order_id]
        if not rows:
            return receipt
        try:
            if any(r["symbol"] != spec.symbol or r["side"] != spec.side or
                   (r["client_id"] and r["client_id"] != spec.client_id) for r in rows):
                return receipt
            if sum((D(r["quantity"]) for r in rows),ZERO) != receipt.executed or sum((D(r["amount"]) for r in rows),ZERO) != receipt.quote:
                return receipt
            fees = defaultdict(lambda:ZERO)
            for row in rows:
                fee = D(row["fee"])
                if fee < 0 or D(row["quantity"]) <= 0 or D(row["amount"]) <= 0 or (fee > 0 and not row["fee_asset"]):
                    return receipt
                if fee:
                    fees[row["fee_asset"]] += fee
            receipt.fees,receipt.fees_known = dict(fees),True
            receipt.timings["commission_source"] = "private_deals"
        except (ValueError,ArithmeticError):
            pass
        return receipt

    def forget(self, spec):
        with self.condition:
            self.expected.discard(spec.client_id)
            self.events.pop(spec.client_id,None)

    def start(self):
        threading.Thread(target=self.run,daemon=True,name="private-orders").start()
        threading.Thread(target=self.keepalive,daemon=True,name="private-keepalive").start()

    def run(self):
        import websocket
        while not self.done.is_set():
            connection_stop = threading.Event()
            try:
                self.key = self.rest.request("POST","/api/v3/userDataStream",role="stream")[0]["listenKey"]
                def opened(ws):
                    ws.send(json.dumps({"method":"SUBSCRIPTION","params":["spot@private.orders.v3.api.pb","spot@private.deals.v3.api.pb"]}))
                    self.heartbeat = Heartbeat(ws.send)
                    def fail_private(reason):
                        if ws is self.ws:
                            self.ready = False
                        ws.close()
                    threading.Thread(target=self.heartbeat.run,args=(connection_stop,fail_private),
                                     daemon=True,name="private-app-ping").start()
                def message(ws, raw):
                    received = self.rest.clock.ms()
                    if isinstance(raw,str):
                        ack = json.loads(raw)
                        if ack.get("msg") == "PONG" and self.heartbeat:
                            self.heartbeat.pong()
                        if ack.get("code") == 0 and "private.orders" in str(ack.get("msg","")):
                            self.ready = True
                        return
                    data = decode_order(raw)
                    if data is None:
                        deal = decode_deal(raw)
                        if deal:
                            self.deal(deal)
                    if data and data.get("status") in TERMINAL:
                        with self.condition:
                            if data["clientOrderId"] in self.expected:
                                old = self.events.get(data["clientOrderId"])
                                if old and (old[0].get("executedQty"),old[0].get("status")) != (data.get("executedQty"),data.get("status")):
                                    self.ready = False
                                    return
                                self.events[data["clientOrderId"]] = data,received
                                self.condition.notify_all()
                self.ws = websocket.WebSocketApp("wss://wbs-api.mexc.com/ws?listenKey="+self.key,
                    on_open=opened,on_message=message)
                self.ws.run_forever(ping_interval=0)
            except Exception:
                pass
            finally:
                connection_stop.set()
                self.ready = False
            self.done.wait(2)

    def keepalive(self):
        while not self.done.wait(1200):
            if self.key:
                try:
                    self.rest.request("PUT","/api/v3/userDataStream",{"listenKey":self.key},role="stream")
                except Exception:
                    self.ready = False
                    if self.ws:
                        self.ws.close()

    def close(self):
        self.done.set()
        if self.ws:
            self.ws.close()


class LiveAdapter:
    """Order state protocol shared with the paper adapter.

    A terminal authenticated WS event can finish submit before HTTP returns.
    At most two order transports exist; new entries remain serialized by the
    coordinator. No automatic HTTP POST retry, even for a timeout or 5xx.
    """
    mode = "live"

    def __init__(self, rest, private, permitted):
        self.rest, self.private, self.permitted = rest, private, permitted
        self.pool = ThreadPoolExecutor(max_workers=2,thread_name_prefix="order-http")
        self.status_pool = ThreadPoolExecutor(max_workers=1,thread_name_prefix="order-status")
        self.poll_future = None
        self.poll_client = None
        self.futures = {}
        self.serial = 0
        self.signed_balances = {}
        self.balance_ms = 0
        self.confirmation_timeout = 5

    def refresh_account(self):
        self.signed_balances = self.rest.account()
        self.balance_ms = self.rest.clock.ms()
        return {k:v["free"] for k,v in self.signed_balances.items()}

    def balances(self):
        # Refresh belongs to the background account worker. The final LIVE
        # gate independently refuses a cache older than ten seconds.
        return {k:v["free"] for k,v in self.signed_balances.items()}

    def submit(self, spec, guard):
        self.private.register(spec)
        def final_guard():
            if not self.permitted(spec.purpose):
                raise GuardRejected("LIVE dispatch not authorized")
            guard()
        role = "order"+str(self.serial%2)
        self.serial += 1
        began = time.monotonic()
        future = self.pool.submit(self.rest.request,"POST","/api/v3/order",self.rest.params(spec),
            signed=True,role=role,guard=final_guard)
        self.futures[spec.client_id] = future
        polled_at = began
        ack = None
        while time.monotonic()-began < self.confirmation_timeout:
            terminal = self.private.take(spec)
            if terminal:
                terminal.timings["confirmation_ms"] = (time.monotonic()-began)*1000
                if future.done():
                    try:
                        ack,timing = future.result()
                        terminal.timings.update(timing)
                    except GuardRejected:
                        raise Uncertain("Terminal event contradicts an undispatched request")
                    except Exception:
                        terminal.timings["http_ack_unavailable"] = True
                return terminal
            if future.done():
                try:
                    ack,timing = future.result()
                    if ack.get("status") in TERMINAL:
                        terminal = parse_receipt(ack,spec,"post",timing)
                        terminal.timings["confirmation_ms"] = (time.monotonic()-began)*1000
                        return terminal
                except GuardRejected:
                    raise
                except Exception:
                    # An exchange rejection is reconciled, not assumed unsent.
                    pass
            if self.poll_future and self.poll_future.done():
                try:
                    data = self.poll_future.result()
                    if self.poll_client == spec.client_id and data.get("status") in TERMINAL:
                        return parse_receipt(data,spec,"rest",{"confirmation_ms":(time.monotonic()-began)*1000})
                except Exception:
                    pass
                self.poll_future = None
            if self.poll_future is None and time.monotonic()-polled_at >= .1:
                polled_at = time.monotonic()
                self.poll_client = spec.client_id
                self.poll_future = self.status_pool.submit(self.rest.order,spec)
            with self.private.condition:
                self.private.condition.wait(.003)
        raise Uncertain("No authoritative terminal confirmation before deadline")

    def reconcile(self, spec):
        try:
            return parse_receipt(self.rest.order(spec),spec,"rest_reconciliation")
        except Exception as exc:
            raise Uncertain("Independent terminal reconciliation unavailable") from exc

    def commissions(self, spec, receipt):
        future = self.futures.get(spec.client_id)
        if future:
            try:
                # This audit runs after the first exit, outside the two-leg
                # critical path. Capture the late HTTP acknowledgement too.
                ack,timings = future.result(timeout=.5)
                if str(ack.get("orderId",receipt.order_id)) != receipt.order_id:
                    raise Uncertain("HTTP/terminal order identity mismatch")
                receipt.timings.update(timings)
                if receipt.timings.get("terminal_received_ms") and timings.get("request_started_ms"):
                    receipt.timings["post_to_terminal_ms"] = receipt.timings["terminal_received_ms"]-timings["request_started_ms"]
                receipt.raw["post_response"] = dict(ack)
            except Uncertain:
                raise
            except Exception:
                receipt.timings["http_ack_unavailable"] = True
        if hasattr(self.private,"decorate"):
            receipt = self.private.decorate(receipt,spec)
        if receipt.fees_known:
            return receipt
        if not receipt.executed:
            receipt.fees_known = True
            return receipt
        rows,_ = self.rest.request("GET","/api/v3/myTrades",{"symbol":spec.symbol,"orderId":receipt.order_id,"limit":1000},
                                   signed=True,role="account")
        rows = [x for x in rows if str(x.get("orderId")) == receipt.order_id]
        if not rows or len(rows) >= 1000 or sum((D(x["qty"]) for x in rows),ZERO) != receipt.executed:
            raise Uncertain("Incomplete fills/commissions")
        fees = defaultdict(lambda:ZERO)
        for row in rows:
            fee = D(row["commission"])
            asset = row.get("commissionAsset")
            if fee < 0 or (fee > 0 and not asset):
                raise Uncertain("Invalid commission evidence")
            if fee:
                fees[asset] += fee
        receipt.fees,receipt.fees_known = dict(fees),True
        return receipt

    def release(self, specs):
        for spec in specs:
            self.private.forget(spec)
            self.futures.pop(spec.client_id,None)

    def close(self):
        self.private.close()
        self.pool.shutdown(wait=True)
        self.status_pool.shutdown(wait=True)
