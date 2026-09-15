from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from mexc_v3.codec import Envelope
from mexc_v3.model import D, Market
from mexc_v3.service import Service


class OfflineRest:
    def __init__(self,clock):
        self.clock=clock;self.offset_ms=0;self.clock_uncertainty_ms=1;self.time_checked_ms=clock.ms()
    def sync_time(self):self.time_checked_ms=self.clock.ms()
    def markets(self):
        def m(s,b,q):return Market(s,b,q,D(".01"),D(".0001"),D(".01"),D(1))
        return [m("DEMOUSDT","DEMO","USDT"),m("DEMOUSD1","DEMO","USD1"),m("USD1USDT","USD1","USDT")]
    def depth(self,symbol):
        bid,ask = {"DEMOUSDT":(".99","1"),"DEMOUSD1":("1.02","1.03"),"USD1USDT":(".9999","1.0001")}[symbol]
        return {"lastUpdateId":1,"bids":[[bid,"10000"]],"asks":[[ask,"10000"]]}
    def close(self):pass


def synthetic_connection(feed, channel):
    channel.epoch=1;channel.connected=True
    feed.books.connection(channel.symbols,1,True)
    version=1
    while not feed.done.wait(.005):
        version+=1
        for symbol in channel.symbols:
            e=Envelope(symbol=symbol,sent=feed.clock.ms()-2)
            e.depth.first=str(version);e.depth.last=str(version)
            feed.receive(channel,e.SerializeToString(),1)


class ServiceTests(unittest.TestCase):
    def test_public_dispatch_to_three_delayed_paper_journals_and_export(self):
        with tempfile.TemporaryDirectory() as tmp,patch("mexc_v3.service.Rest",OfflineRest),patch("mexc_v3.feed.PublicFeed.connect",synthetic_connection):
            service=Service(tmp)
            service.prepare()
            service.start()
            try:
                deadline=time.monotonic()+3
                while time.monotonic()<deadline:
                    totals=[p["coordinator"].journal.totals() for p in service.profiles]
                    if all(t["buys"] >= 1 and t["pending_accounting"] == 0 for t in totals):break
                    time.sleep(.01)
                status=service.status()
                self.assertEqual(status["mode"],"observe")
                self.assertEqual(status["depth"]["ready"],3)
                self.assertTrue(all(t["buys"]>=1 for t in totals),status["errors"])
                self.assertTrue(all("reference" in status["policy_comparison"] for _ in [0]))
                self.assertEqual(status["flow"]["reconnections"],0)
            finally:
                service.close()
            self.assertTrue((Path(tmp)/"last_report.json.gz").is_file())
            self.assertFalse((Path(tmp)/"STOP").exists())
            self.assertFalse((Path(tmp)/"live.db").exists())


if __name__ == "__main__":unittest.main()
