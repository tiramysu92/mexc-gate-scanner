"""Durable intent before POST; confirmation + next intent in one FULL commit."""
from contextlib import contextmanager
from pathlib import Path
import fcntl
import json
import sqlite3
import threading
from .model import D, ZERO, encode


def dumps(x):
    return json.dumps(x, default=encode, sort_keys=True, allow_nan=False)


class Journal:
    def __init__(self, path, mode="paper"):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.owner = open(str(path)+".lock", "a+")
        try:
            fcntl.flock(self.owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.owner.close()
            raise RuntimeError("Journal already owned by another process")
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY, status TEXT NOT NULL,
            created_ms INTEGER NOT NULL, data TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS orders(client_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL
            REFERENCES attempts(id), status TEXT NOT NULL, intent TEXT NOT NULL, receipt TEXT);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL,
            kind TEXT NOT NULL, data TEXT NOT NULL);
        """)
        old = self.meta("mode")
        if old and old != mode:
            self.close()
            raise RuntimeError("A journal cannot change mode")
        if old is None:
            self.set_meta("mode", mode)

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield self.db
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def meta(self, key):
        with self.lock:
            row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else None

    def set_meta(self, key, value):
        with self.transaction() as db:
            db.execute("INSERT INTO metadata VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, dumps(value)))

    def event(self, kind, data, now):
        with self.transaction() as db:
            db.execute("INSERT INTO events(ts_ms,kind,data) VALUES (?,?,?)", (now, kind, dumps(data)))

    def begin(self, attempt, plan, spec, now, evidence=None):
        with self.transaction() as db:
            if db.execute("SELECT 1 FROM attempts WHERE status NOT IN ('completed','unwind','no_fill','aborted','dust')").fetchone():
                raise RuntimeError("Unresolved attempt already present")
            db.execute("INSERT INTO attempts VALUES (?,?,?,?)", (attempt, "buy_pending", now,
                dumps({"route":plan.route.id,"plan":plan,"buys":0,"pnl":None,"unwinds":0,"entry_evidence":evidence})))
            db.execute("INSERT INTO orders VALUES (?,?,?, ?,NULL)", (spec.client_id, attempt, "PREPARED", dumps(spec)))

    def transition(self, attempt, receipt=None, next_spec=None, status="exposure", values=None):
        with self.transaction() as db:
            row = db.execute("SELECT data FROM attempts WHERE id=?", (attempt,)).fetchone()
            if row is None:
                raise ValueError("Unknown attempt")
            data = json.loads(row[0])
            data.update(values or {})
            if receipt is not None:
                change = db.execute("UPDATE orders SET status=?,receipt=? WHERE client_id=? AND attempt_id=? AND status='PREPARED'",
                    (receipt.status, dumps(receipt), receipt.client_id, attempt))
                if change.rowcount != 1:
                    raise ValueError("Confirmation cannot be applied twice")
            if next_spec:
                db.execute("INSERT INTO orders VALUES (?,?,?, ?,NULL)", (next_spec.client_id, attempt, "PREPARED", dumps(next_spec)))
            db.execute("UPDATE attempts SET status=?,data=? WHERE id=?", (status, dumps(data), attempt))

    def undispatched(self, client_id, reason):
        with self.transaction() as db:
            db.execute("UPDATE orders SET status='NOT_SENT',receipt=? WHERE client_id=? AND status='PREPARED'",
                       (dumps({"reason":reason}), client_id))

    def stop(self, reason):
        self.set_meta("circuit", reason)

    def totals(self):
        with self.lock:
            rows = self.db.execute("SELECT status,data FROM attempts").fetchall()
        pnl = ZERO
        buys = unwinds = open_count = pending_accounting = 0
        for row in rows:
            d = json.loads(row["data"])
            buys += int(d.get("buys", 0))
            unwinds += int(d.get("unwinds", 0))
            if d.get("pnl") is not None:
                pnl += D(d["pnl"])
            elif d.get("buys", 0):
                pending_accounting += 1
            open_count += row["status"] not in ("completed", "unwind", "no_fill", "aborted", "dust")
        return {"pnl":str(pnl),"buys":buys,"unwinds":unwinds,"open_exposures":open_count,
                "pending_accounting":pending_accounting,"circuit":self.meta("circuit")}

    def export(self):
        with self.lock:
            return {name:[dict(r) for r in self.db.execute("SELECT * FROM "+name)]
                    for name in ("metadata","attempts","orders","events")}

    def close(self):
        if self.owner.closed:
            return
        self.db.close()
        fcntl.flock(self.owner, fcntl.LOCK_UN)
        self.owner.close()
