#!/usr/bin/env python3
"""Collecte ciblée, sans import du bot, ordre, redémarrage ou écriture dans ses DB.
Python 3 standard uniquement. L'archive JSON conserve les absences/troncatures.
"""
import argparse
import ast
import datetime as dt
import hashlib
import json
import pathlib
import sqlite3
import time
import urllib.request
import zipfile

TABLES = {
    'decisions_v22': 'ts_ms',
    'decision_trace_v22': 'ts_ms',
    'decision_depth_samples_v234': 'decision_ts_ms',
    'decision_quality_v235': 'ts_ms',
    'decision_context_v234': 'decision_ts_ms',
    'live_admissions_v241': 'decision_ts_ms',
    'live_attempts_v240': 'created_ts_ms',
    'diagnostics_v247': 'ts_ms',
    'ws_latency_v22': 'ts_ms',
    'depth_sync_events': 'ts_ms',
}
TARGET_ROUTES = ('USD1>牛来>USDT', 'USDC>RAY>USDT', 'USDT>TIA>USDC',
                 'USDC>GRVT>USDT', 'USDC>SAGA>USDT', 'USDT>SKL>USDC',
                 'USDT>牛来>USD1')
TARGET_SYMBOLS = ('牛来USD1', '牛来USDT', 'RAYUSDC', 'RAYUSDT', 'TIAUSDT',
                  'TIAUSDC', 'GRVTUSDC', 'GRVTUSDT', 'SAGAUSDC', 'SAGAUSDT',
                  'SKLUSDT', 'SKLUSDC')
# Pas de payload privé d'ordre, réponse API brute, soldes ou configuration secrète.
OMIT = {'response_json', 'error', 'unwind_error', 'commissions_json',
        'leg1_client_id', 'leg2_client_id', 'unwind_client_id'}
LIVE_KEYS = ('configured_mode', 'effective_mode', 'v247_counts', 'bbo_age_ms',
             'exchange_age_ms', 'max_skew_ms', 'max_exchange_skew_ms',
             'leg2_bbo_age_ms', 'leg2_exchange_age_ms', 'mexc_clock_offset_ms',
             'last_route_exchange_age_ms', 'last_route_exchange_skew_ms',
             'last_route_exchange_age_t0_ms', 'last_route_exchange_skew_t0_ms',
             'last_leg2_exchange_age_ms', 'last_leg2_exchange_skew_ms',
             'entry_order_policy', 'circuit_open', 'circuit_reason', 'live_journal_path')
HEALTH_KEYS = ('scan_queue', 'db_queue', 'scan_queue_capacity', 'db_queue_capacity',
               'resync_queue', 'resync_pending', 'depth_book_age_p95_ms',
               'depth_book_age_max_ms', 'max_worker_message_age_ms',
               'ws_disconnects_5m', 'ws_disconnects_15m', 'ws_disconnects_60m')


def epoch(value):
    parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Indiquer le fuseau : Z ou +02:00')
    return int(parsed.timestamp() * 1000)


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def extract(path, start, end, limit):
    result = {'path': str(path), 'tables': {}}
    conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=2)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('PRAGMA query_only=ON')
        known = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table, timestamp in TABLES.items():
            if table not in known:
                result['tables'][table] = {'missing': True}
                continue
            cols = [r[1] for r in conn.execute('PRAGMA table_info(' + quote(table) + ')')]
            if timestamp not in cols:
                timestamp = next((k for k in ('ts_ms', 'decision_ts_ms', 'created_ts_ms') if k in cols), None)
            if timestamp is None:
                result['tables'][table] = {'error': 'colonne temporelle absente'}
                continue
            wanted = [k for k in cols if k not in OMIT and
                      (not k.endswith('_json') or k == 'payload_json')]
            params = [start, end]
            where = quote(timestamp) + '>=? AND ' + quote(timestamp) + '<=?'
            if 'route_id' in cols:
                where += ' AND route_id IN (' + ','.join('?' for _ in TARGET_ROUTES) + ')'
                params.extend(TARGET_ROUTES)
            if 'symbol' in cols:
                where += ' AND symbol IN (' + ','.join('?' for _ in TARGET_SYMBOLS) + ')'
                params.extend(TARGET_SYMBOLS)
            deadline = time.monotonic() + 8
            conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
            try:
                sql = ('SELECT ' + ','.join(map(quote, wanted)) + ' FROM ' + quote(table) +
                       ' WHERE ' + where + ' ORDER BY ' + quote(timestamp) + ' LIMIT ?')
                rows = [dict(r) for r in conn.execute(sql, params + [limit + 1])]
                result['tables'][table] = {'rows': rows[:limit], 'truncated': len(rows) > limit}
            except sqlite3.Error as exc:
                result['tables'][table] = {'error': str(exc)}
            finally:
                conn.set_progress_handler(None, 0)
        if 'meta' in known:
            try:
                result['meta'] = [dict(r) for r in conn.execute(
                    "SELECT key,value FROM meta WHERE key IN ('version','v247_diagnostics')")]
            except sqlite3.Error as exc:
                result['meta_error'] = str(exc)
    finally:
        conn.close()
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--db', action='append', help='Base à lire ; répétable. Sinon noms V247/V246 connus dans le répertoire courant.')
    ap.add_argument('--source', default='app.py', help='Version/hash du fichier disque, sans import ; pas une preuve de version en mémoire.')
    ap.add_argument('--port', type=int, default=8081)
    ap.add_argument('--no-status', action='store_true')
    ap.add_argument('--since', default='2026-09-14T01:10:00+02:00')
    ap.add_argument('--until', default='2026-09-14T01:45:00+02:00')
    ap.add_argument('--limit', type=int, default=12000)
    ap.add_argument('--recent-minutes',type=int,help='Collecter les N dernières minutes au lieu de la fenêtre fixe.')
    ap.add_argument('--out', help='Nouvelle archive ZIP ; un fichier existant ne sera pas écrasé.')
    args = ap.parse_args()
    if args.recent_minutes is not None:
        if args.recent_minutes<1 or args.recent_minutes>60:ap.error('--recent-minutes doit être compris entre 1 et 60')
        now=dt.datetime.now(dt.timezone.utc)
        args.until=now.isoformat();args.since=(now-dt.timedelta(minutes=args.recent_minutes)).isoformat()
    start, end = epoch(args.since), epoch(args.until)
    if start > end or args.limit < 1:
        ap.error('Fenêtre ou limite invalide')
    report = {'collected_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
              'window': {'since': args.since, 'until': args.until},
              'notes': ['Extraits ciblés, pas export complet.',
                        'Les tables sont lues séparément : pas de snapshot global atomique.',
                        'Statut HTTP actuel distinct des événements historiques.',
                        'Une erreur ou troncature interdit de conclure à une absence.']}
    source = pathlib.Path(args.source)
    if source.is_file():
        raw = source.read_bytes()
        info = {'path': str(source.resolve()), 'sha256': hashlib.sha256(raw).hexdigest()}
        try:
            for node in ast.parse(raw).body:
                if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'VERSION' for t in node.targets):
                    info['version_literal'] = ast.literal_eval(node.value)
        except (SyntaxError, ValueError, TypeError) as exc:
            info['parse_error'] = type(exc).__name__
        report['source_on_disk'] = info
    else:
        report['source_on_disk'] = {'missing': str(source)}
    if not args.no_status:
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open('http://127.0.0.1:%d/api/status' % args.port, timeout=8) as response:
                payload = response.read(8 * 1024 * 1024 + 1)
                if len(payload) > 8 * 1024 * 1024:
                    raise ValueError('Statut trop volumineux')
                status = json.loads(payload)
            report['current_status'] = {k: status.get(k) for k in
                ('version', 'ws', 'ws_expected', 'age_ms', 'depth_ready', 'scan_updates', 'fee_pct')}
            report['current_status']['public_flow'] = status.get('public_flow')
            report['current_status']['live'] = {k: status.get('live', {}).get(k) for k in LIVE_KEYS}
            report['current_status']['health'] = {k: status.get('health', {}).get(k) for k in HEALTH_KEYS}
        except Exception as exc:
            report['current_status'] = {'error': type(exc).__name__ + ': ' + str(exc)}
    paths = args.db or ['mexc_routes_v247.db', 'mexc_live_v247_test.db', 'mexc_live_v246.db']
    report['databases'] = []
    for path in dict.fromkeys(paths):
        p = pathlib.Path(path).resolve()
        if not p.is_file():
            report['databases'].append({'path': str(p), 'missing': True})
            continue
        try:
            report['databases'].append(extract(p, start, end, args.limit))
        except Exception as exc:
            report['databases'].append({'path': str(p), 'error': str(exc)})
    output = pathlib.Path(args.out or ('mexc_diagnostic_timestamps_' +
        dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f') + '.zip')).resolve()
    with zipfile.ZipFile(output, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('diagnostic.json', json.dumps(report, ensure_ascii=False, indent=2))
    print('Archive créée :', output)
    for db in report['databases']:
        count = sum(len(v.get('rows', [])) for v in db.get('tables', {}).values())
        print(pathlib.Path(db['path']).name, ':', db.get('error') or ('absente' if db.get('missing') else str(count) + ' lignes'))


if __name__ == '__main__':
    main()
