"""One durable trade quota and narrowly reviewed SKL acknowledgement.

Only the stopped launcher may commit. No exchange request and no order here.
An existing quota is reused, never replaced to reset its counts or loss budget.
"""
import datetime as dt
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid

from relancer_mexc_v2411 import require
from regulariser_cto import inspect as inspect_cto, assert_scanner_stopped
from session_live_v2412 import LiveSession, TOTALS_SQL, totals

REVIEWED_SKL_TS = 1789414003722
REVIEWED_SKL_ERROR = 'protected_unknown_asset:SKL'
STABLES = frozenset(('USDT', 'USDC', 'USD1'))


def check_causes(causes, acknowledge_skl=False):
    if not causes:
        return
    require(acknowledge_skl and len(causes) == 1,
            'Circuit actif : seul l’incident SKL examiné peut être acquitté explicitement.')
    event = causes[0]
    require(event.get('reason') == 'protected_asset_detected' and
            event.get('error') == REVIEWED_SKL_ERROR and
            event.get('attempt_id') is None and event.get('ts_ms') == REVIEWED_SKL_TS,
            'Cause de circuit différente de l’incident SKL examiné : reprise refusée.')


def stable_balances(lv):
    """Strict preflight only: production inventory/order guards stay unchanged."""
    require(lv.get('api_ok') is True, 'Compte privé indisponible.')
    age = lv.get('account_cache_age_ms')
    limit = lv.get('account_cache_max_age_ms')
    require(type(age) in (int, float) and type(limit) in (int, float) and
            math.isfinite(age) and math.isfinite(limit) and 0 <= age <= min(limit, 10000),
            'Relevé des soldes absent ou ancien ; attendre un rafraîchissement.')
    allowed = lv.get('allowed_start_assets')
    require(isinstance(allowed, (tuple, list)) and allowed and set(allowed) <= STABLES,
            'Actifs de départ inattendus.')
    balances = lv.get('balances')
    require(isinstance(balances, dict) and balances, 'Soldes absents.')
    positive = False
    for asset, row in balances.items():
        require(isinstance(row, dict), 'Solde invalide.')
        values = [row.get(k) for k in ('free', 'locked', 'total')]
        require(all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in values),
                'Quantité de solde invalide.')
        free, locked, total = values
        require(abs(total-free-locked) <= 1e-8, 'Total de solde incohérent.')
        require(locked == 0, 'Un solde est encore verrouillé.')
        require(total == 0 or asset in allowed,
                'Actif hors stablecoins encore présent : '+str(asset))
        positive = positive or total > 0
    require(positive, 'Aucun capital disponible.')
    return balances


def inspect_journal(con):
    con.row_factory = sqlite3.Row
    require(inspect_cto(con)['already_applied'], 'Régularisation CTO absente ou incohérente.')
    unresolved = con.execute("""SELECT COUNT(*) FROM live_orders_v240 WHERE status IS NULL OR
        status NOT IN ('FILLED','CANCELED','PARTIALLY_CANCELED','REJECTED','EXPIRED',
        'rejected','test_validated','test_rejected','expired_before_submit')""").fetchone()[0]
    unfinished = con.execute("""SELECT COUNT(*) FROM live_attempts_v240 WHERE mode='live' AND
        status IN ('prepared','leg1_submitting','leg2_submitting','unwind_submitting',
        'reconciliation_required','accounting_pending','open_exposure')""").fetchone()[0]
    exposure = con.execute("SELECT COUNT(*) FROM live_open_exposures_v246 WHERE status='open' AND units>0").fetchone()[0]
    require(not (unresolved or unfinished or exposure), 'Ordre ou exposition non résolu.')
    causes = [dict(r) for r in con.execute('SELECT * FROM live_circuit_events_v246 WHERE active=1 ORDER BY event_id')]
    state = con.execute('SELECT * FROM live_state_v240 WHERE singleton=1').fetchone()
    require(state is not None and bool(state['circuit_open']) == bool(causes),
            'Circuit du journal absent ou incohérent.')
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    policies = {}
    if 'live_rearm_sessions_v2412' in tables:
        for row in con.execute('SELECT session_id,policy_json FROM live_rearm_sessions_v2412'):
            policy = json.loads(row['policy_json'])
            LiveSession(policy)
            require(policy['session_id'] == row['session_id'], 'Identité de session incohérente.')
            policies[row['session_id']] = policy
    return dict(totals=totals(con.execute(TOTALS_SQL).fetchone()), causes=causes,
                policies=policies, previous_state=dict(state))


def read_plan(db):
    with sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro', uri=True, timeout=3) as con:
        return inspect_journal(con)


def choose_policy(plan, active_id, budget):
    require(active_id in plan['policies'], 'Session précédente introuvable dans le journal.')
    quotas = [p for p in plan['policies'].values() if p.get('mode') == 'trade_quota']
    require(len(quotas) <= 1, 'Plusieurs quotas existent : examen manuel requis.')
    if quotas:
        policy = quotas[0]
        require(budget is None or budget == policy['additional_loss_usd'],
                'Le budget du quota existant ne peut pas être modifié.')
        status = LiveSession(policy).status(plan['totals'])
        require(not status['reason'], 'Quota existant bloqué : '+str(status['reason'])+'. Aucun compteur remis à zéro.')
        return policy, False
    require(type(budget) in (int, float) and math.isfinite(budget) and 0 < budget <= 5,
            'Nouvelle allocation : préciser un budget supplémentaire entre 0 et 5 USDT.')
    previous = LiveSession(plan['policies'][active_id]).status(plan['totals'])
    require(previous['reason'] in (None, 'session_expired') and
            previous['buys'] == 0 and previous['unwinds'] == 0 and abs(previous['pnl']) < 1e-8,
            'La session précédente a exécuté une opération : examen requis avant un nouveau quota.')
    policy = dict(session_id=uuid.uuid4().hex, mode='trade_quota',
                  created_ts_ms=int(time.time()*1000), expires_ts_ms=None,
                  additional_loss_usd=budget, max_buys=10, baseline=plan['totals'])
    LiveSession(policy)
    return policy, True


def check_session(lv, policy, current_totals):
    expected = LiveSession(policy).status(current_totals)
    actual = lv.get('session', {})
    keys = ('required', 'session_id', 'expires_ts_ms', 'additional_loss_usd', 'max_buys',
            'baseline_realized_pnl', 'pnl', 'buys', 'unwinds')
    require(all(k in actual and actual[k] == expected[k] for k in keys),
            'La session en mémoire diverge du journal.')
    require(actual.get('mode', 'timed') == policy.get('mode', 'timed'), 'Mode de session incohérent.')
    return expected


def commit_quota(db, root, expected, policy, is_new, acknowledge_skl, evidence):
    """Atomic, stopped-only: append quota/audit, acknowledge exactly one old cause."""
    root = Path(root).resolve(); db = Path(db).resolve()
    assert_scanner_stopped(root); assert_scanner_stopped(db.parent)
    stable_balances(evidence)
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    backup = db.with_name(db.stem+'_avant_quota_'+stamp+'.db')
    con = sqlite3.connect(db.as_uri()+'?mode=rw', uri=True, timeout=3)
    try:
        with open(backup, 'xb'):
            pass
        os.chmod(backup, 0o600)
        with sqlite3.connect(backup) as dest:
            con.backup(dest)
        con.execute('PRAGMA synchronous=FULL'); con.execute('BEGIN IMMEDIATE')
        current = inspect_journal(con)
        require(all(current[k] == expected[k] for k in ('totals', 'causes', 'policies')),
                'Le journal a changé pendant la préparation : aucune autorisation.')
        check_causes(current['causes'], acknowledge_skl)
        require(not LiveSession(policy).status(current['totals'])['reason'], 'Quota bloqué.')
        if is_new:
            require(not any(p.get('mode') == 'trade_quota' for p in current['policies'].values()),
                    'Un quota existe déjà ; aucune nouvelle allocation.')
            require(policy['baseline'] == current['totals'], 'Baseline du quota incorrecte.')
            con.execute('''INSERT INTO live_rearm_sessions_v2412
                (session_id,policy_json,acknowledgement_json,backup_path) VALUES(?,?,?,?)''',
                (policy['session_id'], json.dumps(policy), json.dumps(current), str(backup)))
        else:
            require(current['policies'].get(policy['session_id']) == policy, 'Quota existant modifié.')
        if current['causes']:
            event = current['causes'][0]
            changed = con.execute('''UPDATE live_circuit_events_v246 SET active=0
                WHERE event_id=? AND active=1 AND reason=? AND error=? AND ts_ms=? AND attempt_id IS NULL''',
                (event['event_id'], 'protected_asset_detected', REVIEWED_SKL_ERROR, REVIEWED_SKL_TS)).rowcount
            require(changed == 1, 'Incident SKL différent ou déjà acquitté.')
            con.execute('''CREATE TABLE IF NOT EXISTS live_circuit_ack_v2414(
                event_id INTEGER PRIMARY KEY, session_id TEXT NOT NULL,
                acknowledged_ts_ms INTEGER NOT NULL, evidence_json TEXT NOT NULL,
                backup_path TEXT NOT NULL)''')
            con.execute('INSERT INTO live_circuit_ack_v2414 VALUES(?,?,?,?,?)',
                (event['event_id'], policy['session_id'], int(time.time()*1000),
                 json.dumps(dict(cause=event, balances=evidence['balances'],
                                 account_cache_age_ms=evidence['account_cache_age_ms'])), str(backup)))
            con.execute('''UPDATE live_state_v240 SET circuit_open=0,circuit_reason=NULL,
                last_error=CASE WHEN last_error=? THEN NULL ELSE last_error END WHERE singleton=1''',
                (REVIEWED_SKL_ERROR,))
        con.commit()
        return backup
    except BaseException:
        con.rollback(); raise
    finally:
        con.close()
