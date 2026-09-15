"""Deterministic offline demonstration using the same decision/execution engine."""
from .books import Books, Delta
from .config import Policy
from .decision import Decisions
from .execution import Coordinator
from .journal import Journal
from .model import D, ONE, Market, Route
from .simulation import PaperAdapter, Latency


class ReplayClock:
    def __init__(self, start=1789358300000):
        self.now = start
        self.on_advance = None

    def ms(self):
        return self.now

    def mono(self):
        return self.now/1000

    def sleep(self, seconds):
        self.now += round(seconds*1000)
        if self.on_advance:
            self.on_advance(self.now)


def fixture(clock=None):
    clock = clock or ReplayClock()
    buy = Market("DEMOUSDT","DEMO","USDT",D(".01"),D(".0001"),D(".01"),D("1"))
    sell = Market("DEMOUSD1","DEMO","USD1",D(".01"),D(".0001"),D(".01"),D("1"))
    route = Route(buy,sell)
    books = Books([buy.symbol,sell.symbol],clock)
    books.connection([buy.symbol,sell.symbol],1,True)
    for m,bid,ask in ((buy,".99","1"),(sell,"1.02","1.03")):
        books.install(m.symbol,books.token(m.symbol),{"lastUpdateId":1,"bids":[[bid,"10000"]],"asks":[[ask,"10000"]]})
    def update(now):
        for m in (buy,sell):
            version = books.slots[m.symbol].version
            books.delta(Delta(m.symbol,version+1,version+1,now-5,now-2,(),()),1)
    update(clock.ms())
    clock.on_advance = update
    return books,route,clock


def run(path):
    books,route,clock = fixture()
    policy = Policy()
    decisions = Decisions(books,policy,clock,lambda asset:(ONE,ONE))
    journal = Journal(path)
    adapter = PaperAdapter(books,{route.buy.symbol:route.buy,route.sell.symbol:route.sell},clock)
    co = Coordinator(decisions,adapter,journal,policy,clock)
    result = co.execute(decisions.evaluate(route,D(80)))
    report = {"synthetic":True,"source":"offline demo, not MEXC trading",
              "result":result,"totals":journal.totals(),"orders_sent_to_exchange":0}
    journal.close()
    return report
