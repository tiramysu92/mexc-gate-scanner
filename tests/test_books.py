import random
import unittest
from mexc_v3.books import Books, Delta
from mexc_v3.codec import Envelope, decode_depth
from mexc_v3.demo import fixture, ReplayClock


class BookTests(unittest.TestCase):
    def setUp(self):
        self.books,self.route,self.clock = fixture()
        self.symbol = self.route.buy.symbol

    def delta(self,first,last=None,bids=(),asks=()):
        return Delta(self.symbol,first,last or first,self.clock.ms()-5,self.clock.ms()-2,bids,asks)

    def test_gap_invalidates_and_retains_delta_for_snapshot_replay(self):
        self.assertFalse(self.books.delta(self.delta(10),1))
        self.assertIsNone(self.books.get(self.symbol))
        self.assertEqual(len(self.books.slots[self.symbol].buffer),1)

    def test_duplicate_delta_is_not_applied_twice(self):
        self.books.delta(self.delta(3,bids=((.99,15),)),1)
        self.books.delta(self.delta(3,bids=((.99,99),)),1)
        self.assertEqual(self.books.get(self.symbol).bids[0][1],15)

    def test_previous_connection_cannot_republish_a_book(self):
        self.books.connection([self.symbol],2,True)
        self.assertFalse(self.books.delta(self.delta(3),1))
        self.assertIsNone(self.books.get(self.symbol))

    def test_rest_snapshot_from_previous_generation_is_rejected(self):
        self.books.invalidate(self.symbol,"first")
        token = self.books.token(self.symbol)
        self.books.invalidate(self.symbol,"second")
        self.assertFalse(self.books.install(self.symbol,token,{"lastUpdateId":2,"bids":[[.99,1]],"asks":[[1,1]]}))

    def test_rest_snapshot_without_delta_has_no_exchange_freshness(self):
        self.books.connection([self.symbol],2,True)
        self.books.install(self.symbol,self.books.token(self.symbol),{"lastUpdateId":20,"bids":[[.99,1]],"asks":[[1,1]]})
        self.assertEqual(self.books.get(self.symbol).exchange_ms,0)

    def test_snapshot_replay_keeps_original_receive_time(self):
        self.books.connection([self.symbol],2,True)
        delta = self.delta(11,bids=((.99,8),))
        self.books.delta(delta,2)
        self.clock.sleep(.1)
        self.assertTrue(self.books.install(self.symbol,self.books.token(self.symbol),{"lastUpdateId":10,"bids":[[.99,1]],"asks":[[1,1]]}))
        self.assertEqual(self.books.get(self.symbol).received_ms,delta.received_ms)

    def test_overflow_bounds_buffer_and_keeps_newest_replay(self):
        self.books.connection([self.symbol],2,True)
        for i in range(5000):
            self.books.delta(self.delta(i+1),2)
        self.assertLessEqual(len(self.books.slots[self.symbol].buffer),4096)
        self.assertGreater(self.books.slots[self.symbol].generation,2)

    def test_top_cache_matches_full_scan_after_deletions(self):
        rng = random.Random(3)
        for i in range(300):
            bid = rng.randint(900,990)/1000
            ask = rng.randint(1000,1100)/1000
            self.books.delta(self.delta(i+3,bids=((bid,rng.randint(0,20)),),asks=((ask,rng.randint(0,20)),)),1)
            s = self.books.slots[self.symbol]
            self.assertEqual(s.best_bid,max(s.bids))
            self.assertEqual(s.best_ask,min(s.asks))

    def test_crossed_or_negative_depth_is_never_ready(self):
        self.assertFalse(self.books.delta(self.delta(3,bids=((1.2,2),)),1))
        self.assertIsNone(self.books.get(self.symbol))

    def test_codec_matches_official_field_numbers(self):
        # Fixed wire fixture, independently encoded from MEXC's documented
        # wrapper 3/6/313 and depth 2/4/5, not our runtime descriptor serializer.
        raw = bytes.fromhex("1a0844454d4f5553445430b960ca131112090a032e3939120231322201312a0132")
        d = decode_depth(raw,12346)
        self.assertEqual((d.first,d.last,d.exchange_ms,d.received_ms),(1,2,12345,12346))
        self.assertEqual(d.bids,((".99","12"),))

    def test_malformed_protobuf_fails(self):
        with self.assertRaises(Exception):
            decode_depth(b"\xff\xff",10)


if __name__ == "__main__":
    unittest.main()
