from collections import deque
from types import SimpleNamespace
import time
import unittest
from mexc_v3.books import Books
from mexc_v3.codec import Envelope
from mexc_v3.demo import ReplayClock
from mexc_v3.feed import PublicFeed


class FeedTests(unittest.TestCase):
    def setUp(self):
        self.clock = ReplayClock()
        self.books = Books(["HOTUSDT","COLDUSDT"],self.clock)
        self.feed = PublicFeed(self.books,SimpleNamespace(offset_ms=0),[["HOTUSDT"],["COLDUSDT"]],self.clock)
        self.addCleanup(self.feed.close)
        for c in self.feed.channels:
            c.connected=True;c.epoch=1
            self.books.connection(c.symbols,1,True)
            self.books.install(c.symbols[0],self.books.token(c.symbols[0]),{"lastUpdateId":1,"bids":[[1,100]],"asks":[[2,100]]})

    def raw(self,symbol,version):
        e = Envelope(symbol=symbol,sent=self.clock.ms()-3)
        e.depth.first=str(version);e.depth.last=str(version)
        return e.SerializeToString()

    def test_quantum_leaves_room_for_cold_socket(self):
        hot,cold = self.feed.channels
        for i in range(100):self.feed.receive(hot,self.raw("HOTUSDT",i+2),1)
        self.feed.receive(cold,self.raw("COLDUSDT",2),1)
        self.assertEqual(self.feed.process_channel(hot),32)
        self.assertEqual(self.feed.process_channel(cold),1)
        self.assertEqual(self.books.get("COLDUSDT").version,2)
        self.assertEqual(len(hot.queue),68)

    def test_local_pause_drains_without_reconnecting(self):
        c = self.feed.channels[0]
        for i in range(40):self.feed.receive(c,self.raw("HOTUSDT",i+2),1)
        c.queue = deque((a,b,t-.2,e) for a,b,t,e in c.queue)
        self.feed.process_channel(c)
        self.assertTrue(c.catchup)
        self.assertTrue(c.connected)
        self.feed.process_channel(c)
        self.assertFalse(c.catchup)
        self.assertEqual(self.feed.totals["catchup_started"],1)
        self.assertEqual(self.feed.totals["catchup_completed"],1)

    def test_frame_limit_invalidates_connection_once(self):
        c = self.feed.channels[0]
        for _ in range(1025):self.feed.receive(c,self.raw("HOTUSDT",2),1)
        self.assertFalse(c.connected)
        self.assertIsNone(self.books.get("HOTUSDT"))
        self.assertEqual(self.feed.totals["receive_queue_full"],1)
        self.feed.recover(c,"connection_closed")
        self.assertEqual(sum(self.feed.totals.values()),1)

    def test_byte_limit_invalidates_before_large_frame_decode(self):
        c = self.feed.channels[0]
        self.feed.receive(c,b"x"*(4*1024*1024+1),1)
        self.assertFalse(c.connected)
        self.assertEqual(c.bytes,0)

    def test_first_message_after_idle_restarts_progress_timer(self):
        c = self.feed.channels[0]
        c.progress=time.monotonic()-100
        self.feed.receive(c,self.raw("HOTUSDT",2),1)
        self.assertLess(time.monotonic()-c.progress,1)

    def test_bad_frame_closes_without_publishing(self):
        c = self.feed.channels[0]
        self.feed.receive(c,b"\xff",1)
        self.feed.process_channel(c)
        self.assertFalse(c.connected)
        self.assertIsNone(self.books.get("HOTUSDT"))

    def test_late_recovery_from_old_epoch_cannot_close_new_connection(self):
        c = self.feed.channels[0]
        c.epoch=2
        self.books.connection(c.symbols,2,True)
        self.feed.recover(c,"late_old_failure",epoch=1)
        self.assertTrue(c.connected)
        self.assertEqual(self.feed.totals["late_old_failure"],0)


if __name__ == "__main__":unittest.main()
