#!/usr/bin/env python3
"""Quota LIVE sans échéance : 10 achats, budget explicite, premier secours = STOP.

Le premier lancement crée le quota ; les suivants le réutilisent sans remise
à zéro. --acquitter-skl ne vise que l'incident du 14 septembre déjà examiné.

--budget-perte-supplementaire est obligatoire pour le premier quota. Ce seuil ne
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
from collections import deque

from relancer_mexc_v2411 import RestartError, require, status_reader, idle, own_stop_matches, LIMIT_KEYS
from session_live_v2412 import LiveSession
from quota_live_v2414 import (read_plan, stable_balances, check_causes,
    choose_policy, commit_quota, check_session)

ROOT = Path('/home/ubuntu/mexc-gate-scanner')
PYTHON = '/home/ubuntu/mexc-venv/bin/python'
VERSION = '2.4.14-trade-quota-session'
OLD_VERSION = '2.4.13-resync-generations'
PRECHECK = "import app,json; from session_live_v2412 import POLICY_MODES; app._initialize_depth_codec(); print(json.dumps({'version':app.VERSION,'backend':app.PUBLIC_PROTOBUF_BACKEND,'session_required':app.LIVE_SESSION_REQUIRED,'session_id':app.LIVE_SESSION_ID,'quota_supported':'trade_quota' in POLICY_MODES}))"
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
        json.dump(list(traces) if isinstance(traces, deque) else traces, f, ensure_ascii=False)
    os.chmod(file, 0o600)
    print('Bilan à transmettre ici :', file, flush=True)
    return file


def session_journal(db, created_ts_ms):
    with sqlite3.connect(Path(db).resolve().as_uri()+'?mode=ro',uri=True,timeout=3) as con:
        con.row_factory=sqlite3.Row
        # Keep all actual buys in this ten-buy quota, even after many zero-fill
        # attempts; cap the latter to the latest 1000. SQL journal is untouched.
        params=('live',created_ts_ms)
        recent=[dict(r) for r in con.execute('SELECT * FROM live_attempts_v240 WHERE mode=? AND created_ts_ms>=? ORDER BY created_ts_ms DESC LIMIT 1000',params)]
        bought=[dict(r) for r in con.execute('SELECT * FROM live_attempts_v240 WHERE mode=? AND created_ts_ms>=? AND mid_acquired>0 ORDER BY created_ts_ms LIMIT 11',params)]
        selected={r['attempt_id']:r for r in recent+bought}
        counts=[dict(r) for r in con.execute('SELECT status,COUNT(*) AS count FROM live_attempts_v240 WHERE mode=? AND created_ts_ms>=? GROUP BY status',params)]
        result={'attempts':sorted(selected.values(),key=lambda r:r['created_ts_ms']),
                'status_counts':counts,'truncated':sum(r['count'] for r in counts)>len(selected)}
        ids=list(selected)
        for table in ('live_orders_v240','live_observations_v247'):
            result[table]=[]
            for offset in range(0,len(ids),400):
                chunk=ids[offset:offset+400]
                result[table].extend(dict(r) for r in con.execute('SELECT * FROM '+table+' WHERE attempt_id IN ('+','.join('?' for _ in chunk)+')',chunk))
        return result


def compact(d):
    lv = d.get('live', {})
    he=d.get('health') or {}
    health={k:v for k,v in he.items() if v is None or isinstance(v,(str,int,float,bool))}
    flow=d.get('public_flow') or {}
    flow={k:flow.get(k) for k in ('process_started_ms','protobuf_backend','totals')}
    flow['workers']=[{k:w.get(k) for k in ('worker_id','epoch','active','catching_up',
        'queue','queue_age_ms','queue_bytes','lag_ms')} for w in (d.get('public_flow') or {}).get('workers',[])]
    return dict(ts_ms=int(time.time()*1000), version=d.get('version'),
        ws=d.get('ws'), ws_expected=d.get('ws_expected'), depth_ready=d.get('depth_ready'),
        symbols=d.get('symbols'), health=health, public_flow=flow,
        live={k: lv.get(k) for k in ('session','arm','busy','queued','completed','realized_pnl',
            'last_status','last_route','circuit_open','circuit_reason','open_exposures',
            'open_exposure_items','private_order_stream','v247_counts')})


def make_report(traces, initial, db, created_ts_ms, settled=False):
    report={'samples':list(traces),'settled':settled,
            'sample_window_limit':1200,'monitor_started':initial}
    if initial and traces:
        last=traces[-1]
        report['flow_counts']={k:int(last.get('health',{}).get(k,0))-int(initial.get('health',{}).get(k,0))
            for k in ('ws_reconnects','ws_disconnects')}
        report['flow_counts'].update({k:int((last.get('public_flow',{}).get('totals') or {}).get(k,0))-int((initial.get('public_flow',{}).get('totals') or {}).get(k,0))
            for k in ('catchup_started','catchup_completed')})
        report['session']=last.get('live',{}).get('session')
    try: report['journal']=session_journal(db,created_ts_ms)
    except Exception as exc: report['journal_error']=type(exc).__name__
    return report


def checkpoint(root, session_id, report):
    require(isinstance(session_id,str) and len(session_id)==32 and all(c in '0123456789abcdef' for c in session_id),
            'Identifiant de bilan invalide.')
    target=Path(root)/('bilan_live_quota_'+session_id+'_courant.json.gz')
    temporary=target.with_name(target.name+'.tmp')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as raw:
        with gzip.GzipFile(fileobj=raw,mode='wb') as zipped:
            zipped.write(json.dumps(report,ensure_ascii=False).encode('utf-8'))
        raw.flush();os.fsync(raw.fileno())
    os.replace(temporary,target)
    return target


def monitor(read, stop, root, session_id, traces, db, created_ts_ms):
    """Bounded reporting; the scanner independently enforces durable quotas."""
    traces=deque(traces,maxlen=1200)
    next_line=0; next_checkpoint=0; initial=None
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
            if time.monotonic()>=next_checkpoint:
                checkpoint(root,session_id,make_report(traces,initial,db,created_ts_ms))
                next_checkpoint=time.monotonic()+60
            if ss.get('reason') or lv.get('circuit_open') or stop.exists():
                break
            time.sleep(3)
    finally:
        # Never kill a scanner that could still be confirming/exiting an order.
        try:
            with open(stop, 'x') as f: f.write('Fin de surveillance V2414 : nouvelles entrées suspendues.\n')
        except FileExistsError: pass
        # The tenth buy must still be allowed to finish its second leg/exit.
        deadline=time.monotonic()+30; settled=False
        while time.monotonic()<deadline:
            try:
                final=read()
                if initial and final.get('public_flow',{}).get('process_started_ms')!=initial.get('public_flow',{}).get('process_started_ms'): break
                traces.append(compact(final))
                if idle(final): settled=True; break
            except (OSError,ValueError): break
            time.sleep(.5)
        report=make_report(traces,initial,db,created_ts_ms,settled)
        if 'flow_counts' in report:
            print('Bilan flux pendant la session :',report['flow_counts'],flush=True)
        save_trace(root, report)
    print('Nouvelles entrées suspendues. Le scanner poursuit les confirmations et sorties déjà engagées.', flush=True)


def resume(pid, root=ROOT, python=PYTHON, budget=None, apply=False, acknowledge_skl=False,
           drain_timeout=30, startup_timeout=600, stable_seconds=30, watch=True):
    require(budget is None or (0 < budget <= 5), 'Budget supplémentaire autorisé : entre 0 et 5 USDT.')
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
            require(env.get('LIVE_RESET_CIRCUIT', '0') != '1' and env.get('LIVE_SESSION_REQUIRED') == '1',
                    'Session requise et absence de réinitialisation globale nécessaires.')
            read = status_reader(int(env.get('PORT','8081'))); before = read(); lv = before['live']
            require(before.get('version') in (OLD_VERSION, VERSION), 'Le scanner actif doit être V2.4.13 ou V2.4.14.')
            require(lv.get('configured_mode') == 'live' and 0 < lv.get('cap_usd',999) <= 20 and
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
            active_id = env.get('LIVE_SESSION_ID')
            require(active_id in expected['policies'], 'Session active absente du journal.')
            stable_balances(lv)
            check_causes(expected['causes'], acknowledge_skl)
            check_causes(lv.get('circuit_causes') or [], acknowledge_skl)
            require(bool(lv.get('circuit_open')) == bool(expected['causes']) and
                    len(lv.get('circuit_causes') or []) == len(expected['causes']),
                    'Circuits mémoire/journal divergents.')
            check_session(lv, expected['policies'][active_id], expected['totals'])
            policy, is_new = choose_policy(expected, active_id, budget)
            require(lv.get('open_exposures') == 0, 'Exposition non résolue.')
            print('Historique réalisé conservé :', expected['totals']['pnl'], 'USDT', flush=True)
            print('Quota', 'nouveau' if is_new else 'existant conservé', ': CTO exclu ; cap',
                  lv['cap_usd'], 'USDT ; 10 achats ; sans échéance ; premier secours = arrêt.', flush=True)
            if not apply:
                print('Contrôle seul : aucun processus, circuit ou armement modifié.',flush=True)
                return before
            print('Budget du quota :', policy['additional_loss_usd'],
                  'USDT (seuil d’arrêt, perte maximale non garantie).', flush=True)
            session_id = policy['session_id']
            env.update(LIVE_SESSION_REQUIRED='1',LIVE_SESSION_ID=session_id,
                       LIVE_MAX_CONSECUTIVE_UNWINDS='1')
            excluded = set(filter(None,env.get('LIVE_EXCLUDED_ASSETS','').upper().split(','))) | {'CTO'}
            env['LIVE_EXCLUDED_ASSETS'] = ','.join(sorted(x.strip() for x in excluded))
            check = subprocess.run([python,'-c',PRECHECK],cwd=root,env=env,stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20)
            require(check.returncode == 0, 'Prévalidation échouée ; ancien processus conservé.')
            prepared = json.loads(check.stdout)
            require(prepared.get('version') == VERSION and prepared.get('backend') in ('upb','cpp') and
                    prepared.get('session_required') is True and prepared.get('session_id') == session_id and
                    prepared.get('quota_supported') is True,
                    'Correctif de session absent ou invalide.')
            deadline = time.monotonic() + drain_timeout; quiet = 0
            while time.monotonic() < deadline:
                quiet = quiet+1 if idle(read()) else 0
                if quiet >= 2: break
                time.sleep(.3)
            require(quiet >= 2, 'Ordre ou confirmation en cours : ancien processus conservé.')
            final_before = read()
            require(final_before.get('version') == before.get('version') and
                    final_before.get('public_flow',{}).get('process_started_ms') == before.get('public_flow',{}).get('process_started_ms'),
                    'Processus changé avant la relance.')
            stable_balances(final_before['live'])
            check_causes(final_before['live'].get('circuit_causes') or [], acknowledge_skl)
            require(idle(final_before) and final_before['live'].get('open_exposures') == 0,
                    'Ordre ou exposition apparu avant la relance.')
            require(unchanged_stop(), 'STOP a changé ; reprise annulée.')
            require(not select.select([pidfd],[],[],0)[0], 'Ancien processus déjà terminé.')
            signal.pidfd_send_signal(pidfd,signal.SIGTERM)
            require(bool(select.select([pidfd],[],[],15)[0]), 'Ancien processus encore actif ; aucun second scanner lancé.')
            backup = commit_quota(db, root, expected, policy, is_new, acknowledge_skl, final_before['live'])
            print('Journal sauvegardé :',backup,flush=True)
            with open(root/'scanner_v2414_live.log','ab',buffering=0) as log:
                child = subprocess.Popen(args,cwd=root,env=env,stdin=subprocess.DEVNULL,stdout=log,
                    stderr=subprocess.STDOUT,start_new_session=True,close_fds=True)
            (root/'scanner_v2414_live.pid').write_text(str(child.pid)+'\n')
            print('Nouveau PID :',child.pid,'; contrôle des flux avant armement.',flush=True)
            traces=deque(maxlen=1200); deadline=time.monotonic()+startup_timeout; stable_start=None; reconnects=None; current=None; next_line=0; watch_started=False
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
                    check_session(cur, policy, expected['totals'])
                    require(not cur.get('circuit_open'),'Un circuit est actif ; STOP conservé.')
                    require(not cur['session'].get('reason'),'Session bloquée ; STOP conservé.')
                    require(cur.get('open_exposures')==0,'Exposition persistante ; STOP conservé.')
                    r=current.get('health',{}).get('ws_reconnects')
                    account_ready=cur.get('api_ok') is True and cur.get('account_cache_age_ms') is not None
                    if account_ready: stable_balances(cur)
                    stable=public_ready(current) and idle(current) and account_ready and cur.get('validated_routes',0)>0 and cur.get('arm',{}).get('private_ws_ready') is True
                    if not stable or reconnects is None or r!=reconnects: stable_start=None
                    reconnects=r
                    if stable and r is not None:
                        if stable_start is None: stable_start=time.monotonic()
                        if time.monotonic()-stable_start>=stable_seconds: break
                    time.sleep(2)
                else: raise RestartError('Flux non stabilisés dans le délai ; STOP conservé. Envoyer le bilan de démarrage.')
                require(unchanged_stop(),'STOP a été modifié ; reprise annulée.')
                require(child.poll() is None,'Scanner arrêté avant armement.')
                final=read(); final_plan=read_plan(db)
                require(all(k in lv and final['live'].get(k)==lv[k] for k in LIMIT_KEYS) and
                        final.get('version')==VERSION and
                        final.get('health',{}).get('ws_reconnects')==reconnects and
                        public_ready(final) and idle(final) and not final['live'].get('circuit_open') and
                        final['live'].get('open_exposures')==0 and
                        final['live'].get('arm',{}).get('private_ws_ready') is True,
                        'Les contrôles ont changé avant armement ; STOP conservé.')
                stable_balances(final['live']); check_session(final['live'],policy,final_plan['totals'])
                require(not final_plan['causes'] and final_plan['totals']==expected['totals'] and
                        final_plan['policies'].get(session_id)==policy,
                        'Le journal a changé avant armement ; STOP conservé.')
                # Environment and exact cap are unchanged; authorize this process last.
                phrase=f"ENABLE MEXC V247 LIVE {float(cur['cap_usd']):g} USD\n"
                tmp=arm.with_name(arm.name+'.'+session_id)
                fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
                with os.fdopen(fd,'w') as f: f.write(phrase);f.flush();os.fsync(f.fileno())
                os.replace(tmp,arm)
                require(unchanged_stop(),'STOP a changé avant son retrait.')
                stop.unlink()
                print('LIVE autorisé : quota de 10 achats, sans échéance horaire. Budget et compteurs persistants.',flush=True)
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
    ap.add_argument('--acquitter-skl',action='store_true')
    ap.add_argument('--bilan',action='store_true',help='Exporter la session sans modifier le scanner ni son armement')
    args=ap.parse_args()
    def interrupted(signum,frame):
        raise KeyboardInterrupt('Arrêt de la surveillance demandé.')
    for signum in (signal.SIGTERM,signal.SIGHUP):signal.signal(signum,interrupted)
    try:
        if args.bilan:
            require(not args.apply and not args.acquitter_skl and args.budget_perte_supplementaire is None,
                    '--bilan ne peut pas être combiné à une demande de reprise.')
            d=status_reader(8081)(); ss=d['live'].get('session',{})
            require(ss.get('session_id') and ss.get('required'), 'Session indisponible.')
            db=(ROOT/d['live']['live_journal_path']).resolve()
            with sqlite3.connect(db.as_uri()+'?mode=ro',uri=True) as con:
                row=con.execute('SELECT policy_json FROM live_rearm_sessions_v2412 WHERE session_id=?',(ss['session_id'],)).fetchone()
            require(row is not None,'Politique absente du journal.')
            policy=json.loads(row[0])
            save_trace(ROOT,{'samples':[compact(d)],'session':ss,
                'journal':session_journal(db,policy['created_ts_ms'])},'quota')
            return 0
        pid=detect_pid()
        require(args.pid is None or args.pid==pid,'PID fourni différent du scanner unique.')
        resume(pid,budget=args.budget_perte_supplementaire,apply=args.apply,acknowledge_skl=args.acquitter_skl)
    except (RestartError,KeyboardInterrupt) as exc:
        print('STOP :',str(exc) or 'Interruption demandée.',file=sys.stderr);return 1
    except Exception as exc:
        print('STOP : contrôle interrompu ('+type(exc).__name__+'). Aucun autre processus arrêté.',file=sys.stderr);return 1
    return 0


if __name__=='__main__': raise SystemExit(main())
