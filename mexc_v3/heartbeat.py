"""MEXC application PING/PONG, distinct from RFC control frames (V2.4.13 REX)."""
import json
import time


class Heartbeat:
    def __init__(self, send, opened=None, phase=0):
        self.send = send
        self.opened = time.monotonic() if opened is None else opened
        self.last_pong = self.opened
        self.next_ping = self.opened+phase
        self.pings = self.pongs = 0

    def pong(self, now=None):
        self.last_pong = time.monotonic() if now is None else now
        self.pongs += 1

    def tick(self, now=None):
        now = time.monotonic() if now is None else now
        if now-self.last_pong > 45:
            return "application_pong_timeout"
        if now >= self.next_ping:
            try:
                self.send(json.dumps({"method":"PING"}))
            except Exception:
                return "application_ping_failed"
            self.pings += 1
            self.next_ping = now+15
        return None

    def run(self, stop, fail):
        while not stop.wait(.25):
            reason = self.tick()
            if reason:
                fail(reason)
                return
