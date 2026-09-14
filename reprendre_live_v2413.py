#!/usr/bin/env python3
"""Nouvel essai LIVE explicite et limité sur le scanner V2.4.13.

La session expirée reste au journal. Une nouvelle session est créée lors du
redémarrage ; la reconstruction des carnets peut prendre plusieurs minutes.

--budget-perte-supplementaire est obligatoire avec --apply. Ce seuil ne
garantit pas la liquidation. Aucun ordre n'est envoyé par ce programme ;
retirer STOP autorise le scanner à en envoyer pendant la session limitée.
"""
import argparse
import datetime as dt
import fcntl
import gzip
import json
import os
from pathlib import Path
import select
import signal
import sqlite3
import subprocess
import sys
import time
import uuid

from relancer_mexc_v2411 import RestartError, require, status_reader, idle, own_stop_matches, LIMIT_KEYS
from regulariser_cto import inspect as inspect_cto, ATTEMPT, assert_scanner_stopped
from session_live_v2412 import LiveSession, TOTALS_SQL, totals

ROOT = Path('/home/ubuntu/mexc-gate-scanner')
PYTHON = '/home/ubuntu/mexc-venv/bin/python'
VERSION = '2.4.13-resync-generations'
OLD_VERSION = VERSION
PRECHECK = "import app,json; app._initialize_depth_codec(); print(json.dumps({'version':app.VERSION,'backend':app.PUBLIC_PROTOBUF_BACKEND,'session_required':app.LIVE_SESSION_REQUIRED,'session_id':app.LIVE_SESSION_ID}))"


def inspect_journal(con):
    con.row_factory = sqlite3.Row
    require(inspect_cto(con)['already_applied'], 'CTO doit être régularisé avant la reprise.')
    unresolved = con.execute("""SELECT COUNT(*) FROM live_orders_v240
        WHERE status IS NULL OR status NOT IN
        ('FILLED','CANCELED','PARTIALLY_CANCELED','REJECTED','EXPIRED',
         'rejected','test_validated','test_rejected','expired_before_submit')""").fetchone()[0]
    unfinished = con.execute("""SELECT COUNT(*) FROM live_attempts_v240 WHERE mode='live'
        AND status IN ('prepared','leg1_submitting','leg2_submitting','unwind_submitting',
          'reconciliation_required','accounting_pending','open_exposure')""").fetchone()[0]
    exposure = con.execute("SELECT COUNT(*) FROM live_open_exposures_v246 WHERE status='open' AND units>0").fetchone()[0]
    require(not (unresolved or unfinished or exposure), 'Ordre ou exposition non résolu : reprise refusée.')
    causes = [dict(r) for r in con.execute('SELECT * FROM live_circuit_events_v246 WHERE active=1 ORDER BY event_id')]
    allowed = {'residual_intermediate_asset', 'manual_close_review_required', 'unwind_failed'}
    require(all(r['attempt_id'] == ATTEMPT and r['reason'] in allowed for r in causes),
            'Une autre cause de circuit est active : elle doit être examinée séparément.')
    state = con.execute('SELECT * FROM live_state_v240 WHERE singleton=1').fetchone()
    require(state is not None, 'État LIVE absent.')
    require(not state['circuit_open'] or bool(causes), 'Circuit sans cause identifiable.')
    return dict(totals=totals(con.execute(TOTALS_SQL).fetchone()), causes=causes,
                previous_state=dict(state))


def read_plan(db):
    with sqlite3.connect(Path(db).resolve().as_uri() + '?mode=ro', uri=True, timeout=3) as con:
        return inspect_journal(con)


def allocate(db, root, session_id, budget, minutes, expected):
    """Called only between processes. Atomic audit + acknowledgement, no PnL edit."""
    assert_scanner_stopped(root)
    db = Path(db).resolve()
    assert_scanner_stopped(db.parent)
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    backup = db.with_name(db.stem + '_avant_reprise_' + stamp + '.db')
    con = sqlite3.connect(db.as_uri() + '?mode=rw', uri=True, timeout=3)
    try:
        with open(backup, 'xb'): pass
        os.chmod(backup, 0o600)
        with sqlite3.connect(backup) as dest: con.backup(dest)
        con.execute('PRAGMA synchronous=FULL')
        con.execute('BEGIN IMMEDIATE')
        current = inspect_journal(con)
        require(current['totals'] == expected['totals'] and current['causes'] == expected['causes'],
                'Le journal a changé pendant la préparation : reprise refusée.')
        created = int(time.time() * 1000)
        policy = dict(session_id=session_id, created_ts_ms=created,
                      expires_ts_ms=created + int(minutes * 60 * 1000),
                      additional_loss_usd=budget, max_buys=10, baseline=current['totals'])
        LiveSession(policy)  # Validate before acknowledging any historical cause.
        con.execute('''CREATE TABLE IF NOT EXISTS live_rearm_sessions_v2412(
            session_id TEXT PRIMARY KEY, policy_json TEXT NOT NULL,
            acknowledgement_json TEXT NOT NULL, backup_path TEXT NOT NULL)''')
        con.execute('INSERT INTO live_rearm_sessions_v2412 VALUES(?,?,?,?)',
                    (session_id, json.dumps(policy), json.dumps(current), str(backup)))
        for event in current['causes']:
            changed = con.execute('UPDATE live_circuit_events_v246 SET active=0 WHERE event_id=? AND active=1',
                                  (event['event_id'],)).rowcount
            require(changed == 1, 'Acquittement concurrent détecté.')
        con.execute("""UPDATE live_state_v240 SET circuit_open=0,circuit_reason=NULL,
                       consecutive_unwinds=0 WHERE singleton=1""")
        con.commit()
        return policy, backup
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


def public_ready(d):
    workers = d.get('public_flow', {}).get('workers') or []
    if not workers or not d.get('ws_expected') or d.get('ws') != d.get('ws_expected'):
        return False
    if not d.get('symbols') or d.get('depth_ready', 0) < .90 * d['symbols']:
        return False
    return all(w.get('active') is True and not w.get('catching_up') and
               isinstance(w.get('queue_age_ms'), (int, float)) and
               0 <= w['queue_age_ms'] <= 100 for w in workers)


def save_trace(root, traces, suffix='session'):
    stamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    file = Path(root) / f'bilan_live_{suffix}_{stamp}.json.gz'
    with gzip.open(file, 'xt', encoding='utf-8') as f:
        json.dump(traces, f, ensure_ascii=False)
    os.chmod(file, 0o600)
    print('Bilan à transmettre ici :', file, flush=True)
    return file


def session_journal(db, created_ts_ms):
    with sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=3) as con:
        con.row_factory=sqlite3.Row
        attempts=[dict(r) for r in con.execute('SELECT * FROM live_attempts_v240 WHERE mode=? AND created_ts_ms>=? ORDER BY created_ts_ms LIMIT 1001',('live',created_ts_ms))]
        result={'attempts':attempts[:1000],'truncated':len(attempts)>1000}
        ids=[r['attempt_id'] for r in attempts[:1000]]
        for table in ('live_orders_v240','live_observations_v247'):
            result[table]=[dict(r) for r in con.execute('SELECT * FROM '+table+' WHERE attempt_id IN ('+','.join('?' for _ in ids)+')',ids)] if ids else []
        return result


def compact(d):
    lv = d.get('live', {})
    return dict(ts_ms=int(time.time()*1000), version=d.get('version'),
        ws=d.get('ws'), ws_expected=d.get('ws_expected'), depth_ready=d.get('depth_ready'),
        symbols=d.get('symbols'), health=d.get('health'), public_flow=d.get('public_flow'),
        live={k: lv.get(k) for k in ('session','arm','busy','queued','completed','realized_pnl',
            'last_status','last_route','circuit_open','circuit_reason','open_exposures',
            'open_exposure_items','private_order_stream','v247_counts')})


def monitor(read, stop, root, session_id, traces, db, created_ts_ms):
    """Reporting is optional for expiry: the scanner enforces its own deadline."""
    next_line = 0; initial=None
    try:
        while True:
            d = read(); lv = d['live']; ss = lv.get('session', {})
            require(ss.get('session_id') == session_id, 'Session active différente.')
            require(initial is None or d.get('public_flow',{}).get('process_started_ms')==initial.get('public_flow',{}).get('process_started_ms'),
                    'Processus redémarré pendant la surveillance ; fin de cette collecte.')
            traces.append(compact(d))
            if initial is None: initial=compact(d)
            if time.monotonic() >= next_line:
                workers=d.get('public_flow',{}).get('workers',[])
                catching=sum(bool(w.get('active')) and bool(w.get('catching_up')) for w in workers)
                wait=max((float(w.get('queue_age_ms') or 0) for w in workers if w.get('active')),default=0)
                connections=int(d.get('health',{}).get('ws_reconnects',0))-int(initial.get('health',{}).get('ws_reconnects',0))
                print(f"WS {d.get('ws')}/{d.get('ws_expected')} | Depth {d.get('depth_ready')}/{d.get('symbols')} | "
                      f"rattrapages {catching} | attente max {wait:.1f} ms | reconnexions +{connections} | "
                      f"achats {ss.get('buys')}/10 | PnL session {ss.get('pnl')} USDT | "
                      f"état {lv.get('arm',{}).get('reason')}", flush=True)
                next_line = time.monotonic() + 15
            if ss.get('reason') or lv.get('circuit_open') or stop.exists():
                break
            time.sleep(3)
    finally:
        # Never kill a scanner that could still be confirming/exiting an order.
        try:
            with open(stop, 'x') as f: f.write('Fin de surveillance V2413 : nouvelles entrées suspendues.\n')
        except FileExistsError: pass
        # Capture settled PnL when expiry interrupts the middle of a route.
        deadline=time.monotonic()+30; settled=False
        while time.monotonic()<deadline:
            try:
                final=read()
                if initial and final.get('public_flow',{}).get('process_started_ms')!=initial.get('public_flow',{}).get('process_started_ms'): break
                traces.append(compact(final))
                if idle(final): settled=True; break
            except (OSError,ValueError): break
            time.sleep(.5)
        report={'samples':traces,'settled':settled}
        if initial and traces:
            last=traces[-1]
            report['flow_counts']={k:int(last.get('health',{}).get(k,0))-int(initial.get('health',{}).get(k,0))
                for k in ('ws_reconnects','ws_disconnects')}
            report['flow_counts'].update({k:int(last.get('public_flow',{}).get('totals',{}).get(k,0))-int(initial.get('public_flow',{}).get('totals',{}).get(k,0))
                for k in ('catchup_started','catchup_completed')})
            report['session']=last.get('live',{}).get('session')
            print('Bilan flux pendant la session :',report['flow_counts'],flush=True)
        try: report['journal']=session_journal(db,created_ts_ms)
        except Exception as exc: report['journal_error']=type(exc).__name__
        save_trace(root, report)
    print('Nouvelles entrées suspendues. Le scanner poursuit les confirmations et sorties déjà engagées.', flush=True)


def resume(pid, root=ROOT, python=PYTHON, budget=None, minutes=30, apply=False,
           drain_timeout=30, startup_timeout=600, stable_seconds=30, watch=True):
    require(budget is None or (0 < budget <= 5), 'Budget supplémentaire autorisé : entre 0 et 5 USDT.')
    require(not apply or budget is not None, 'Indiquer explicitement --budget-perte-supplementaire.')
    require(1 <= minutes <= 30, 'Durée autorisée : de 1 à 30 minutes.')
    root = Path(root).resolve(); os.chdir(root)
    with open(root / '.v248_restart.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        proc = Path('/proc') / str(pid)
        require(proc.stat().st_uid == os.getuid() and (proc/'cwd').resolve() == root,
                'PID hors du projet ou autre utilisateur.')
        identity = (proc/'stat').read_text().rsplit(')',1)[1].split()[19]
        args = [os.fsdecode(x) for x in (proc/'cmdline').read_bytes().split(b'\0') if x]
        require(args and args[0] == python and any(x in ('app.py',str(root/'app.py')) for x in args[1:]),
                'Python ou commande du scanner inattendu.')
        pidfd = os.pidfd_open(pid)
        try:
            require((proc/'stat').read_text().rsplit(')',1)[1].split()[19] == identity,
                    'Le PID a changé.')
            env = dict(os.fsdecode(x).split('=',1) for x in (proc/'environ').read_bytes().split(b'\0') if b'=' in x)
            require(env.get('TRADING_MODE','').lower() == 'live' and env.get('LIVE_ARMED') == '1',
                    'Environnement LIVE initial non armé.')
            read = status_reader(int(env.get('PORT','8081'))); before = read(); lv = before['live']
            require(before.get('version') == VERSION, 'Le scanner actif doit être V2.4.13.')
            require(lv.get('configured_mode') == 'live' and lv.get('cap_usd',999) <= 20 and
                    lv.get('max_concurrent') == 1 and lv.get('protected_bags') is True,
                    'Paramètres LIVE initiaux inattendus.')
            stop = Path(env.get('LIVE_STOP_FILE','/home/ubuntu/.config/mexc-bot/LIVE_STOP'))
            arm = Path(env.get('LIVE_ARM_FILE','/home/ubuntu/.config/mexc-bot/LIVE_ARMED'))
            require(stop.is_file() and not stop.is_symlink(), 'STOP doit être présent avant la reprise.')
            st = stop.stat(); stop_identity = (st.st_dev,st.st_ino); stop_token = stop.read_bytes()
            def unchanged_stop():
                try:
                    return own_stop_matches(stop,stop_identity,stop_token) and stop.stat().st_mtime_ns==st.st_mtime_ns
                except OSError: return False
            db = (root / lv['live_journal_path']).resolve()
            expected = read_plan(db)
            require(not expected['causes'] and not expected['previous_state']['circuit_open'],
                    'Circuit actif : aucun acquittement automatique dans cette reprise.')
            print('Historique réalisé conservé :', expected['totals']['pnl'], 'USDT', flush=True)
            print('Proposition : CTO exclu ; cap',lv['cap_usd'],'USDT ; 10 achats ;',minutes,'minutes ; premier secours = arrêt.',flush=True)
            if not apply:
                print('Contrôle seul : aucun processus, circuit ou armement modifié.',flush=True)
                return before
            print('Allocation supplémentaire explicitement demandée :',budget,'USDT (seuil d’arrêt, perte maximale non garantie).',flush=True)
            session_id = uuid.uuid4().hex
            env.update(LIVE_SESSION_REQUIRED='1',LIVE_SESSION_ID=session_id,
                       LIVE_MAX_CONSECUTIVE_UNWINDS='1')
            excluded = set(filter(None,env.get('LIVE_EXCLUDED_ASSETS','').upper().split(','))) | {'CTO'}
            env['LIVE_EXCLUDED_ASSETS'] = ','.join(sorted(x.strip() for x in excluded))
            check = subprocess.run([python,'-c',PRECHECK],cwd=root,env=env,stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20)
            require(check.returncode == 0, 'Prévalidation échouée ; ancien processus conservé.')
            prepared = json.loads(check.stdout)
            require(prepared.get('version') == VERSION and prepared.get('backend') in ('upb','cpp') and
                    prepared.get('session_required') is True and prepared.get('session_id') == session_id,
                    'Correctif de session absent ou invalide.')
            deadline = time.monotonic() + drain_timeout; quiet = 0
            while time.monotonic() < deadline:
                quiet = quiet+1 if idle(read()) else 0
                if quiet >= 2: break
                time.sleep(.3)
            require(quiet >= 2, 'Ordre ou confirmation en cours : ancien processus conservé.')
            require(unchanged_stop(), 'STOP a changé ; reprise annulée.')
            require(not select.select([pidfd],[],[],0)[0], 'Ancien processus déjà terminé.')
            signal.pidfd_send_signal(pidfd,signal.SIGTERM)
            require(bool(select.select([pidfd],[],[],15)[0]), 'Ancien processus encore actif ; aucun second scanner lancé.')
            policy, backup = allocate(db,root,session_id,budget,minutes,expected)
            print('Journal sauvegardé :',backup,flush=True)
            with open(root/'scanner_v2413_live.log','ab',buffering=0) as log:
                child = subprocess.Popen(args,cwd=root,env=env,stdin=subprocess.DEVNULL,stdout=log,
                    stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
            (root/'scanner_v2413_live.pid').write_text(str(child.pid)+'\n')
            print('Nouveau PID :',child.pid,'; contrôle des flux avant armement.',flush=True)
            traces=[]; deadline=time.monotonic()+startup_timeout; stable_start=None; reconnects=None; current=None; next_line=0; watch_started=False
            try:
                while time.monotonic()<deadline:
                    require(child.poll() is None,'Le nouveau scanner est arrêté ; STOP conservé.')
                    try: current=read()
                    except (OSError,ValueError): time.sleep(.5); continue
                    if current.get('version')!=VERSION: time.sleep(.5); continue
                    traces.append(compact(current)); cur=current['live']
                    if time.monotonic()>=next_line:
                        print('Préparation : WS',current.get('ws'),'/',current.get('ws_expected'),
                              '; Depth',current.get('depth_ready'),'/',current.get('symbols'),flush=True)
                        next_line=time.monotonic()+15
                    require(all(k in lv and k in cur and lv[k]==cur[k] for k in LIMIT_KEYS),
                            'Les limites habituelles ont changé ; STOP conservé.')
                    require(cur.get('session',{}).get('session_id')==session_id and
                            cur.get('max_consecutive_unwinds')==1 and 'CTO' in cur.get('excluded_assets',[]),
                            'Protections de session non confirmées ; STOP conservé.')
                    require(not cur.get('circuit_open'),'Un circuit est actif ; STOP conservé.')
                    require(not cur['session'].get('reason'),'Session bloquée ; STOP conservé.')
                    require(cur.get('open_exposures')==0,'Exposition persistante ; STOP conservé.')
                    cto=cur.get('balances',{}).get('CTO',{})
                    require(float(cto.get('free',0))+float(cto.get('locked',0))<=1e-8,
                            'Un solde CTO subsiste dans le sous-compte ; STOP conservé.')
                    r=current.get('health',{}).get('ws_reconnects')
                    stable=public_ready(current) and idle(current) and cur.get('api_ok') is True and cur.get('validated_routes',0)>0 and cur.get('arm',{}).get('private_ws_ready') is True
                    if not stable or reconnects is None or r!=reconnects: stable_start=None
                    reconnects=r
                    if stable and r is not None:
                        if stable_start is None: stable_start=time.monotonic()
                        if time.monotonic()-stable_start>=stable_seconds: break
                    time.sleep(2)
                else: raise RestartError('Flux non stabilisés dans le délai ; STOP conservé. Envoyer le bilan de démarrage.')
                require(unchanged_stop(),'STOP a été modifié ; reprise annulée.')
                require(child.poll() is None,'Scanner arrêté avant armement.')
                # Environment and exact cap are unchanged; authorize this process last.
                phrase=f"ENABLE MEXC V247 LIVE {float(cur['cap_usd']):g} USD\n"
                tmp=arm.with_name(arm.name+'.'+session_id)
                fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                with os.fdopen(fd,'w') as f: f.write(phrase);f.flush();os.fsync(f.fileno())
                os.replace(tmp,arm)
                require(unchanged_stop(),'STOP a changé avant son retrait.')
                stop.unlink()
                print('LIVE autorisé pour cette session. Fin au plus tard :',
                      dt.datetime.fromtimestamp(policy['expires_ts_ms']/1000,dt.timezone.utc).isoformat(),flush=True)
                if watch:
                    watch_started=True
                    monitor(read,stop,root,session_id,traces,db,policy['created_ts_ms'])
                return current
            except BaseException:
                try:
                    with open(stop,'x') as f: f.write('Reprise interrompue : nouvelles entrées suspendues.\n')
                except FileExistsError: pass
                if traces and not watch_started: save_trace(root,traces,'demarrage')
                raise
        finally: os.close(pidfd)


def detect_pid(root=ROOT, python=PYTHON):
    found=[]
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit(): continue
        try:
            if (proc/'cwd').resolve()!=root: continue
            cmd=(proc/'cmdline').read_bytes().split(b'\0')
            if cmd and os.fsdecode(cmd[0])==python and any(x==b'app.py' or x.endswith(b'/app.py') for x in cmd[1:]):
                found.append(int(proc.name))
        except OSError: continue
    require(len(found)==1,'Un scanner unique est requis.')
    return found[0]


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--pid',type=int)
    ap.add_argument('--apply',action='store_true')
    ap.add_argument('--budget-perte-supplementaire',type=float)
    ap.add_argument('--minutes',type=int,default=30)
    args=ap.parse_args()
    try:
        pid=detect_pid()
        require(args.pid is None or args.pid==pid,'PID fourni différent du scanner unique.')
        resume(pid,budget=args.budget_perte_supplementaire,minutes=args.minutes,apply=args.apply)
    except (RestartError,KeyboardInterrupt) as exc:
        print('STOP :',str(exc) or 'Interruption demandée.',file=sys.stderr);return 1
    except Exception as exc:
        print('STOP : contrôle interrompu ('+type(exc).__name__+'). Aucun autre processus arrêté.',file=sys.stderr);return 1
    return 0


if __name__=='__main__': raise SystemExit(main())
