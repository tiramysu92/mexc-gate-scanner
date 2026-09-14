#!/usr/bin/env python3
"""Restart the known two-leg scanner, retaining its environment only in memory.

No API orders are submitted by this helper. An already armed LIVE configuration
is resumed after the new process is verified. Existing STOP files and circuit
states are retained. The scanner's own order guards remain authoritative.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time
import urllib.request
import uuid

ROOT = Path('/home/ubuntu/mexc-gate-scanner')
PYTHON = '/home/ubuntu/mexc-venv/bin/python'
VERSION = '2.4.10-fair-public-dispatch'
PRECHECK = "import app; app._initialize_depth_codec(); import json; print(json.dumps({'version':app.VERSION,'backend':app.PUBLIC_PROTOBUF_BACKEND}))"
LIMIT_KEYS = ('configured_mode', 'cap_usd', 'capital_limit_usd',
              'daily_loss_limit_usd', 'max_concurrent', 'entry_order_policy',
              'bbo_age_ms', 'exchange_age_ms', 'max_skew_ms',
              'max_exchange_skew_ms', 'leg2_bbo_age_ms',
              'leg2_exchange_age_ms', 'leg2_max_exchange_skew_ms',
              'fee_safety_pct', 'leg2_min_edge_pct', 'protected_bags',
              'live_journal_path')


class RestartError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise RestartError(message)


def status_reader(port):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def read():
        with opener.open(f'http://127.0.0.1:{port}/api/status', timeout=5) as r:
            d = json.load(r)
        require(isinstance(d, dict) and isinstance(d.get('live'), dict),
                'Statut local incomplet ; aucune relance.')
        return d
    return read


def own_stop_matches(path, identity, token):
    try:
        st = path.stat()
        return (st.st_dev, st.st_ino) == identity and path.read_bytes() == token
    except OSError:
        return False


def idle(d):
    lv = d['live']
    return (lv.get('arm', {}).get('stop_file') is True and
            lv.get('busy') is False and lv.get('queued') == 0 and
            lv.get('private_order_stream', {}).get('pending_orders') == 0)


def restart(pid, root=ROOT, python=PYTHON, drain_timeout=30,
            stop_timeout=15, startup_timeout=60):
    root = Path(root).resolve()
    os.chdir(root)
    with open(root / '.v248_restart.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RestartError('Une autre relance est déjà en cours.')
        proc = Path('/proc') / str(pid)
        require(proc.stat().st_uid == os.getuid(), 'Le processus appartient à un autre utilisateur.')
        require((proc / 'cwd').resolve() == root, 'PID hors du projet 2-leg ; arrêt refusé.')
        identity = (proc / 'stat').read_text().rsplit(')', 1)[1].split()[19]
        args = [os.fsdecode(x) for x in (proc / 'cmdline').read_bytes().split(b'\0') if x]
        require(args and args[0] == python and
                any(a == 'app.py' or a == str(root / 'app.py') for a in args[1:]),
                'Le PID ne correspond pas au Python et à app.py attendus.')
        require(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'),
                'Ce système ne permet pas le ciblage stable du PID ; aucune relance.')
        pidfd = os.pidfd_open(pid)
        try:
            require((proc / 'stat').read_text().rsplit(')', 1)[1].split()[19] == identity,
                    'Le PID a été réutilisé ; aucune relance.')
            # Never print or persist this mapping: it includes the API credentials.
            raw = (proc / 'environ').read_bytes().split(b'\0')
            env = dict(os.fsdecode(x).split('=', 1) for x in raw if b'=' in x)
            require(not select.select([pidfd], [], [], 0)[0], 'Le processus est déjà terminé.')
            read = status_reader(int(env.get('PORT', '8081')))
            before = read()
            require(before.get('version') == '2.4.9-local-catchup',
                    'La version active n’est pas V2.4.9 ; aucune relance automatique.')
            require(before['live'].get('configured_mode') == env.get('TRADING_MODE', 'shadow').strip().lower(),
                    'Le statut HTTP ne correspond pas à cet environnement.')
            check = subprocess.run([python, '-c', PRECHECK], cwd=root, env=env,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, timeout=20)
            require(check.returncode == 0, 'Prévalidation Python échouée ; ancien processus conservé.')
            try:
                prepared = json.loads(check.stdout)
            except (ValueError, UnicodeError):
                raise RestartError('Réponse de prévalidation invalide ; ancien processus conservé.')
            require(prepared.get('version') == VERSION and prepared.get('backend') in ('upb', 'cpp'),
                    'V2.4.10 et Protobuf natif doivent être présents avant l’arrêt.')

            stop = Path(env.get('LIVE_STOP_FILE', '/home/ubuntu/.config/mexc-bot/LIVE_STOP'))
            arm = Path(env.get('LIVE_ARM_FILE', '/home/ubuntu/.config/mexc-bot/LIVE_ARMED'))
            old_arm = before['live'].get('arm', {})
            renew_arm = (before['live'].get('configured_mode') == 'live' and
                         old_arm.get('env_armed') is True and
                         old_arm.get('arm_file_ok') is True and
                         old_arm.get('arm_file_fresh') is True and
                         old_arm.get('stop_file') is False)
            arm_info = None
            if renew_arm:
                st = arm.stat()
                arm_info = ((st.st_dev, st.st_ino), arm.read_bytes())
            token = ('V2410 restart ' + uuid.uuid4().hex + '\n').encode()
            owned = None
            try:
                fd = os.open(stop, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass  # A pre-existing stop is not ours to remove.
            else:
                with os.fdopen(fd, 'wb') as f:
                    st = os.fstat(f.fileno())
                    owned = (st.st_dev, st.st_ino)
                    f.write(token)
                    f.flush()
                    os.fsync(f.fileno())
            try:
                print('Nouvelles entrées suspendues ; attente de la fin des ordres en cours.', flush=True)
                deadline = time.monotonic() + drain_timeout
                quiet = 0
                while time.monotonic() < deadline:
                    quiet = quiet + 1 if idle(read()) else 0
                    if quiet >= 2:
                        break
                    time.sleep(0.3)
                require(quiet >= 2, 'Exécution ou confirmation encore en cours ; arrêt annulé.')
                require(stop.exists(), 'Le fichier de suspension a été retiré ; arrêt annulé.')
                require((proc / 'cwd').resolve() == root, 'Le processus cible a changé ; arrêt annulé.')
                signal.pidfd_send_signal(pidfd, signal.SIGTERM)
                require(bool(select.select([pidfd], [], [], stop_timeout)[0]),
                        'Ancien processus encore présent ; aucun second scanner lancé.')
                print('Ancien processus terminé. Démarrage de V2.4.10.', flush=True)
                logfile = root / 'scanner_v2410_live.log'
                with open(logfile, 'ab', buffering=0) as log:
                    child = subprocess.Popen(args, cwd=root, env=env, stdin=subprocess.DEVNULL,
                                             stdout=log, stderr=subprocess.STDOUT,
                                             start_new_session=True, close_fds=True)
                (root / 'scanner_v2410_live.pid').write_text(str(child.pid) + '\n')
                print(f'Nouveau PID : {child.pid} ; journal : {logfile.name}', flush=True)
                deadline = time.monotonic() + startup_timeout
                current = None
                while time.monotonic() < deadline:
                    require(child.poll() is None, 'Le nouveau processus s’est arrêté ; consulter scanner_v2410_live.log.')
                    try:
                        candidate = read()
                    except (OSError, ValueError, RestartError):
                        candidate = None
                    if candidate and candidate.get('version') == VERSION:
                        current = candidate
                        break
                    time.sleep(0.5)
                require(current is not None, 'Démarrage non confirmé dans le délai ; entrées laissées suspendues.')
                require(current.get('public_flow', {}).get('protobuf_backend') in ('upb', 'cpp'),
                        'Backend natif non confirmé ; entrées laissées suspendues.')
                require(all(k in before['live'] and k in current['live'] and
                            before['live'][k] == current['live'][k] for k in LIMIT_KEYS),
                        'Paramètres LIVE différents ; entrées laissées suspendues.')
                require(current['live'].get('arm', {}).get('stop_file') is True,
                        'Suspension non confirmée sur le nouveau processus.')
                if renew_arm:
                    with open(arm, 'rb') as f:
                        st = os.fstat(f.fileno())
                        require((st.st_dev, st.st_ino) == arm_info[0] and f.read() == arm_info[1],
                                'Le fichier d’armement a changé ; reprise automatique annulée.')
                        os.utime(f.fileno(), None)
                if owned:
                    require(own_stop_matches(stop, owned, token),
                            'Le fichier de suspension a changé ; il est conservé.')
                    stop.unlink()
                result = read()
                lv = result['live']
                print('Version :', result['version'])
                print('Protobuf :', result.get('public_flow', {}).get('protobuf_backend'))
                print('Mode :', lv.get('configured_mode'))
                print('Armement :', lv.get('arm', {}).get('reason'))
                print('Circuit ouvert :', lv.get('circuit_open'))
                print('WS :', result.get('ws'), '/', result.get('ws_expected'))
                print('Depth prêts :', result.get('depth_ready'), '/', result.get('symbols'))
                return result
            except BaseException:
                if stop.exists():
                    print('Relance interrompue ; nouvelles entrées suspendues. Conserver le journal pour diagnostic.',
                          file=sys.stderr, flush=True)
                raise
        finally:
            os.close(pidfd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pid', type=int, required=True, help='PID V2.4.9 relevé sur ce serveur')
    args = parser.parse_args()
    try:
        restart(args.pid)
    except RestartError as e:
        print('STOP :', e, file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Interruption demandée ; vérifier le statut du scanner avant toute autre relance.', file=sys.stderr)
        return 1
    except Exception as e:
        # Suppress raw exception content: subprocess/environment diagnostics may contain secrets.
        print('STOP : contrôle interrompu (' + type(e).__name__ + '). Aucun arrêt forcé supplémentaire.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
