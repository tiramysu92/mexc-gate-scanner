import json
import unittest
from mexc_v3.heartbeat import Heartbeat


class HeartbeatTests(unittest.TestCase):
    def test_json_ping_and_pong_follow_mexc_application_protocol(self):
        sent=[]
        h=Heartbeat(sent.append,opened=0)
        self.assertIsNone(h.tick(0))
        self.assertEqual(json.loads(sent[0]),{"method":"PING"})
        h.pong(1)
        self.assertIsNone(h.tick(15))
        h.pong(16)
        self.assertIsNone(h.tick(45))
        self.assertEqual(h.pings,3)

    def test_missing_pong_causes_recovery_even_with_other_messages(self):
        h=Heartbeat(lambda _:None,opened=0)
        for now in (0,15,30,45):self.assertIsNone(h.tick(now))
        self.assertEqual(h.tick(46),"application_pong_timeout")

    def test_connection_phases_spread_ping_bursts(self):
        sent=[]
        h=Heartbeat(sent.append,opened=0,phase=14)
        h.tick(0);h.tick(13)
        self.assertEqual(sent,[])
        h.tick(14)
        self.assertEqual(len(sent),1)

    def test_failed_ping_is_an_explicit_recovery_reason(self):
        def fail(_):raise OSError()
        self.assertEqual(Heartbeat(fail,opened=0).tick(0),"application_ping_failed")


if __name__ == "__main__":unittest.main()
