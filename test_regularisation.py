"""Manual accounting checks with entirely synthetic records, no exchange access."""
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from test_public_flow import load_offline
import regulariser_cto as r

HERE=Path(__file__).resolve().parent
# These invented values exist only in the tests; production constants are never
# changed on disk. Expectations below are independent of reconcile's outputs.
SYNTHETIC_PARAMETERS=dict(ATTEMPT='synthetic_cto_attempt',QUANTITY=Decimal('10000'),
    GROSS=Decimal('8'),FEE=Decimal('0.004'),NET=Decimal('7.996'),
    EXPECTED_COST=Decimal('20'),SALE_TIME='2024-01-01T12:00:00+00:00',
    RESOLUTION='synthetic_manual_resolution')


class AccountingTests(unittest.TestCase):
    def setUp(self):
        parameters=patch.multiple(r,**SYNTHETIC_PARAMETERS)
        parameters.start();self.addCleanup(parameters.stop)
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'journal.db'
        m=load_offline(HERE/'app.py');m.LIVE_DB_PATH=str(self.path);m.initialize_live_journal()
        fixture=json.loads((HERE/'cto_journal_fixture_v2411.json').read_text())
        for table,rows in fixture.items():
            for row in rows:
                columns=list(row)
                m.live_db_connection.execute('INSERT INTO '+table+'('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',[row[k] for k in columns])
        m.live_db_connection.commit();m.live_db_connection.close()
        self.m=m;self.m.live_db_connection=None
    def read(self,sql):
        with sqlite3.connect(self.path) as con:return con.execute(sql).fetchall()
    def test_preview_changes_nothing(self):
        before=hashlib.sha256(self.path.read_bytes()).hexdigest()
        plan=r.reconcile(self.path)
        self.assertEqual(plan['realized_pnl_usdt'],'-12.004')
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(),before)
        self.assertFalse(plan['already_applied'])
    def test_apply_preserves_orders_circuits_and_exact_external_proceeds(self):
        orders=self.read('SELECT * FROM live_orders_v240')
        plan=r.reconcile(self.path,apply=True)
        self.assertTrue(Path(plan['backup']).is_file())
        self.assertEqual(orders,self.read('SELECT * FROM live_orders_v240'))
        self.assertEqual(self.read('SELECT status,units FROM live_open_exposures_v246'),[('closed',0.0)])
        self.assertEqual(self.read('SELECT circuit_open FROM live_state_v240'),[(1,)])
        self.assertEqual(self.read('SELECT active FROM live_circuit_events_v246 WHERE event_id=2'),[(1,)])
        status,net,loss=self.read('SELECT status,cash_output_usd,realized_pnl FROM live_attempts_v240')[0]
        self.assertEqual(status,'manual_closed');self.assertAlmostEqual(net,7.996);self.assertAlmostEqual(loss,-12.004)
        with sqlite3.connect(plan['backup']) as con:
            self.assertEqual(con.execute('SELECT status FROM live_attempts_v240').fetchone()[0],'open_exposure')
    def test_repeated_apply_cannot_double_count(self):
        r.reconcile(self.path,apply=True);second=r.reconcile(self.path,apply=True)
        self.assertTrue(second['already_applied'])
        self.assertEqual(self.read('SELECT COUNT(*) FROM live_manual_resolutions_v2411'),[(1,)])
    def test_changed_inventory_blocks_all_writes(self):
        with sqlite3.connect(self.path) as c:c.execute('UPDATE live_open_exposures_v246 SET units=1')
        before=hashlib.sha256(self.path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(RuntimeError,'quantité'):r.reconcile(self.path,apply=True)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(),before)
    def test_running_scanner_blocks_apply(self):
        with patch.object(r,'assert_scanner_stopped',side_effect=RuntimeError('Scanner encore actif')):
            with self.assertRaisesRegex(RuntimeError,'encore actif'):r.reconcile(self.path,apply=True)
    def test_sql_failure_rolls_back_accounting_and_audit(self):
        with sqlite3.connect(self.path) as c:
            c.execute("CREATE TRIGGER fail_close BEFORE UPDATE ON live_open_exposures_v246 BEGIN SELECT RAISE(ABORT,'fixture error'); END")
        with self.assertRaises(sqlite3.Error):r.reconcile(self.path,apply=True)
        self.assertEqual(self.read('SELECT status FROM live_attempts_v240'),[('open_exposure',)])
        self.assertEqual(self.read("SELECT name FROM sqlite_master WHERE name='live_manual_resolutions_v2411'"),[])
    def test_manual_loss_is_counted_in_bot_daily_stats(self):
        r.reconcile(self.path,apply=True)
        m=self.m;m.initialize_live_journal();m.local_day_bounds_ms=lambda x:(0,1,'2024-01-01')
        try:
            m.refresh_live_daily_stats()
            self.assertAlmostEqual(m.live_runtime['realized_pnl'],-12.004)
            self.assertEqual(m.live_runtime['losses'],1)
            self.assertEqual(m.live_runtime['completed'],1)
            self.assertEqual(m.live_runtime['open_exposures'],0)
        finally:m.live_db_connection.close()


if __name__=='__main__':unittest.main(verbosity=2)
