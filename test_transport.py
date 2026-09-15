import threading
import time
import unittest
from unittest.mock import patch
from mexc_v3.demo import ReplayClock
from mexc_v3.mexc import Rest, LiveAdapter, PrivateOrders, parse_receipt
from mexc_v3.codec import decode_order
from mexc_v3.model import D, ZERO, Spec, Receipt, GuardRejected, Uncertain


class PrivateFake:
    ready = True
    def __init__(self):
        self.condition = threading.Condition()
        self.event = None
        self.registered = set()
    def register(self,spec):self.registered.add(spec.client_id)
    def take(self,spec):return self.event
    def forget(self,spec):self.registered.discard(spec.client_id)
    def close(self):pass


class RestFake:
    def __init__(self, private, spec):
        self.private,self.spec = private,spec
        self.calls = 0
        self.http_delay = .02
        self.ws_delay = .01
        self.status_delay = .01
        self.failure = False
        self.response = {"clientOrderId":spec.client_id,"symbol":spec.symbol,"side":spec.side,
            "status":"FILLED","executedQty":str(spec.quantity),"cummulativeQuoteQty":str(spec.quantity*spec.price),"orderId":"actual"}
    def request(self,*args,guard=None,**kwargs):
        guard()
        self.calls += 1
        if not self.failure:
            def notify():
                self.private.event = parse_receipt(self.response,self.spec,"private_ws")
                with self.private.condition:self.private.condition.notify_all()
            threading.Timer(self.ws_delay,notify).start()
        time.sleep(self.http_delay)
        if self.failure:raise TimeoutError()
        return self.response,{"http_call_ms":self.http_delay*1000}
    def order(self,spec):
        time.sleep(self.status_delay)
        if self.failure:raise TimeoutError()
        return self.response
    params = staticmethod(Rest.params)


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.spec = Spec("unique","DEMOUSDT","BUY",D(10),D(1),"leg1")
        self.private = PrivateFake()
        self.rest = RestFake(self.private,self.spec)
        self.adapter = LiveAdapter(self.rest,self.private,lambda purpose:True)
        self.addCleanup(self.adapter.close)

    def test_ws_terminal_does_not_wait_for_http_ack(self):
        self.rest.http_delay = .3
        began = time.monotonic()
        result = self.adapter.submit(self.spec,lambda:None)
        self.assertEqual(result.source,"private_ws")
        self.assertLess(time.monotonic()-began,.2)
        self.assertEqual(self.rest.calls,1)

    def test_slow_rest_fallback_does_not_block_ws_confirmation(self):
        self.rest.http_delay = .35
        self.rest.ws_delay = .13
        self.rest.status_delay = .4
        began = time.monotonic()
        result = self.adapter.submit(self.spec,lambda:None)
        self.assertEqual(result.source,"private_ws")
        self.assertLess(time.monotonic()-began,.3)

    def test_final_guard_failure_never_reaches_transport(self):
        def guard():raise GuardRejected("changed")
        with self.assertRaises(GuardRejected):
            self.adapter.submit(self.spec,guard)
        self.assertEqual(self.rest.calls,0)

    def test_ambiguous_timeout_never_repeats_post(self):
        self.rest.failure = True
        self.adapter.confirmation_timeout = .04
        with self.assertRaises(Uncertain):
            self.adapter.submit(self.spec,lambda:None)
        self.assertEqual(self.rest.calls,1)

    def test_wrong_identity_side_quantity_or_limit_is_not_authoritative(self):
        for change in ({"clientOrderId":"other"},{"side":"SELL"},{"executedQty":"11"},
                       {"cummulativeQuoteQty":"11"},{"status":"NEW"}):
            with self.subTest(change=change),self.assertRaises(Uncertain):
                parse_receipt({**self.rest.response,**change},self.spec,"rest")

    def test_native_private_order_decodes_independent_wire_fixture(self):
        raw = bytes.fromhex("1a0844454d4f555344548213140a014f120158380440016a023130720231307802")
        data = decode_order(raw)
        result = parse_receipt(data,Spec("X","DEMOUSDT","BUY",D(10),D(1),"leg1"),"private_ws")
        self.assertEqual(result.executed,D(10))
        self.assertEqual(result.order_id,"O")

    def test_complete_private_deals_supply_fees_without_rest(self):
        private = PrivateOrders(self.rest)
        receipt = parse_receipt(self.rest.response,self.spec,"private_ws")
        deal = {"trade_id":"one","order_id":"actual","client_id":"unique","symbol":"DEMOUSDT",
                "side":"BUY","quantity":"10","amount":"10","fee":".005","fee_asset":"USDT"}
        private.deal(deal);private.deal(deal)
        receipt = private.decorate(receipt,self.spec)
        self.assertTrue(receipt.fees_known)
        self.assertEqual(receipt.fees,{"USDT":D(".005")})

    def test_zero_fee_does_not_require_a_fictitious_commission_asset(self):
        private=PrivateOrders(self.rest)
        receipt=parse_receipt(self.rest.response,self.spec,"private_ws")
        private.deal({"trade_id":"zero","order_id":"actual","client_id":"unique","symbol":"DEMOUSDT",
            "side":"BUY","quantity":"10","amount":"10","fee":"0","fee_asset":""})
        self.assertTrue(private.decorate(receipt,self.spec).fees_known)
        self.assertEqual(receipt.fees,{})

    def test_incomplete_or_conflicting_private_deals_do_not_certify_fees(self):
        private = PrivateOrders(self.rest)
        receipt = parse_receipt(self.rest.response,self.spec,"private_ws")
        deal = {"trade_id":"one","order_id":"actual","client_id":"unique","symbol":"DEMOUSDT",
                "side":"BUY","quantity":"5","amount":"5","fee":".005","fee_asset":"USDT"}
        private.deal(deal)
        self.assertFalse(private.decorate(receipt,self.spec).fees_known)
        private.deal({**deal,"quantity":"10","amount":"10"})
        self.assertFalse(private.decorate(receipt,self.spec).fees_known)

    def test_signing_happens_after_final_guard(self):
        clock = ReplayClock()
        rest = Rest(clock,"dummy-key","dummy-secret")
        self.addCleanup(rest.close)
        captured = {}
        class Response:
            status_code = 200
            def json(self):return {}
        class Session:
            def request(self,*args,**kwargs):captured.update(kwargs);return Response()
            def close(self):pass
        rest.sessions["order0"].close()
        rest.sessions["order0"] = Session()
        before = clock.ms()
        _,timing = rest.request("POST","/api/v3/order",{},signed=True,role="order0",guard=lambda:clock.sleep(.02))
        self.assertEqual(captured["params"]["timestamp"],before+20)
        self.assertIn("signature",captured["params"])
        self.assertTrue({"guard_ms","sign_ms","http_call_ms","queue_ms"}.issubset(timing))

    def test_market_minimum_quantity_is_distinct_from_precision_step(self):
        row={"symbol":"XUSDT","baseAsset":"X","quoteAsset":"USDT","status":"1","isSpotTradingAllowed":True,
             "baseAssetPrecision":2,"quotePrecision":6,"baseSizePrecision":"5","quoteAmountPrecision":"1",
             "takerCommission":0,"maxQuoteAmount":"10000","tradeSideType":1}
        rest=Rest.__new__(Rest)
        rest.request=lambda *a,**kw:({"symbols":[row]}, {})
        market=rest.markets()[0]
        self.assertEqual(market.quantity_step,D(".01"))
        self.assertEqual(market.min_quantity,D(5))
        self.assertEqual(market.fee,ZERO)
        self.assertTrue(market.valid(D("5.01"),D(1),"BUY"))

    def test_unsupported_lot_grid_is_reported_instead_of_silently_rounded(self):
        row={"symbol":"XUSDT","baseAsset":"X","quoteAsset":"USDT","status":"1","isSpotTradingAllowed":True,
             "baseAssetPrecision":2,"quotePrecision":6,"baseSizePrecision":"1","quoteAmountPrecision":"1",
             "filters":[{"filterType":"LOT_SIZE","stepSize":".03","minQty":"1","maxQty":"100"}]}
        rest=Rest.__new__(Rest)
        rest.request=lambda *a,**kw:({"symbols":[row]}, {})
        self.assertEqual(rest.markets(),[])
        self.assertIn("non-decimal",rest.market_rejections[0]["reason"])


if __name__ == "__main__":unittest.main()
