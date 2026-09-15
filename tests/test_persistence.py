from dataclasses import replace
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from mexc_v3.config import Policy
from mexc_v3.demo import fixture
from mexc_v3.decision import Decisions
from mexc_v3.execution import Coordinator
from mexc_v3.journal import Journal
from mexc_v3.live import initialize, legacy_snapshot
from mexc_v3.model import D, ONE
from mexc_v3.service import Service
from mexc_v3.simulation import PaperAdapter, restore_wallet


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_paper_wallet_is_derived_from_committed_effects_after_crash(self):
        b,r,c = fixture()
        j = Journal(self.root/"p.db")
        self.addCleanup(j.close)
        adapter = PaperAdapter(b,{r.buy.symbol:r.buy,r.sell.symbol:r.sell},c)
        adapter.wallet = restore_wallet(j,adapter.wallet)
        engine = Decisions(b,Policy(),c,lambda a:(ONE,ONE))
        co = Coordinator(engine,adapter,j,Policy(),c)
        co.execute(engine.evaluate(r,D(80)))
        j.set_meta("wallet",{"USDT":"99999"})  # Simulate an obsolete UI checkpoint.
        self.assertEqual(restore_wallet(j,{"USDT":"0"}),adapter.wallet)

    def test_two_process_owners_cannot_open_same_journal(self):
        j = Journal(self.root/"p.db")
        self.addCleanup(j.close)
        with self.assertRaises(RuntimeError):Journal(self.root/"p.db")

    def test_saved_policy_cannot_change_cap_or_freshness_on_restart(self):
        j = Journal(self.root/"p.db")
        self.addCleanup(j.close)
        Service.bind_policy(j,Policy())
        with self.assertRaises(ValueError):Service.bind_policy(j,replace(Policy(),entry_local_ms=200))

    def test_persistent_buy_quota_is_enforced(self):
        b,r,c = fixture()
        j = Journal(self.root/"p.db")
        self.addCleanup(j.close)
        policy = replace(Policy(),max_buys=1)
        e = Decisions(b,policy,c,lambda a:(ONE,ONE))
        adapter = PaperAdapter(b,{r.buy.symbol:r.buy,r.sell.symbol:r.sell},c)
        co = Coordinator(e,adapter,j,policy,c)
        co.execute(e.evaluate(r,D(80)))
        self.assertEqual(co.entry_block(),"buy_quota")
        self.assertEqual(co.execute(e.evaluate(r,D(80)))["status"],"blocked")

    def legacy(self, circuit=0, status="manual_closed", synthetic=False):
        path = self.root/"legacy.db"
        c = sqlite3.connect(path)
        c.executescript("""
        CREATE TABLE live_attempts_v240(attempt_id TEXT,mode TEXT,status TEXT,realized_pnl REAL);
        CREATE TABLE live_open_exposures_v246(status TEXT);
        CREATE TABLE live_state_v240(singleton INTEGER,circuit_open INTEGER,circuit_reason TEXT);
        """)
        c.execute("INSERT INTO live_attempts_v240 VALUES (?,?,?,?)",("synthetic_test" if synthetic else "real-history","live",status,-12.68668593664))
        c.execute("INSERT INTO live_state_v240 VALUES (1,?,?)",(circuit,"previous_event"))
        c.commit();c.close()
        return path

    def test_history_is_imported_without_arming_or_modifying_old_database(self):
        legacy = self.legacy(circuit=1)
        before = legacy.read_bytes()
        root = self.root/"v3"
        with patch("mexc_v3.live.scanner_processes",return_value=[]):
            auth = initialize(root,legacy,Policy())
        self.assertFalse(json.loads(auth.read_text())["enabled"])
        self.assertTrue((root/"STOP").exists())
        self.assertEqual(legacy.read_bytes(),before)
        j = Journal(root/"live.db",mode="live")
        self.addCleanup(j.close)
        self.assertEqual(j.meta("legacy_baseline")["pnl"],"-12.68668593664")
        self.assertEqual(j.meta("circuit"),"legacy:previous_event")

    def test_live_initialization_cannot_reset_existing_quota(self):
        legacy = self.legacy()
        root = self.root/"v3"
        with patch("mexc_v3.live.scanner_processes",return_value=[]):
            initialize(root,legacy,Policy())
            before = (root/"live.db").read_bytes()
            with self.assertRaises(ValueError):initialize(root,legacy,Policy())
        self.assertEqual((root/"live.db").read_bytes(),before)

    def test_old_open_exposure_blocks_new_live_journal(self):
        legacy = self.legacy(status="open_exposure")
        with self.assertRaises(ValueError):legacy_snapshot(legacy)

    def test_synthetic_fixture_cannot_become_real_history(self):
        legacy = self.legacy(synthetic=True)
        with self.assertRaises(ValueError):legacy_snapshot(legacy)

    def test_running_legacy_scanner_blocks_initialization(self):
        legacy = self.legacy()
        with patch("mexc_v3.live.scanner_processes",return_value=[123]):
            with self.assertRaises(ValueError):initialize(self.root/"v3",legacy,Policy())
        self.assertFalse((self.root/"v3"/"live.db").exists())


if __name__ == "__main__":unittest.main()
