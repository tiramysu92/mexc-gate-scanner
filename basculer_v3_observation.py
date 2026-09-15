#!/usr/bin/env python3
"""Transition V2.4.14 vers V3 en observation, sans réarmement."""
import fcntl
import json
import os
from pathlib import Path
import select
import signal
import sqlite3
import subprocess
import time
import urllib.request


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def idle(status):
    live = status.get("live") or {}
    return (status.get("version") == "2.4.14-trade-quota-session"
            and live.get("arm", {}).get("stop_file") is True
            and live.get("busy") is False and live.get("queued") == 0
            and live.get("private_order_stream", {}).get("pending_orders") == 0
            and live.get("open_exposures") == 0)


def transition(pid=126190, old=Path("/home/ubuntu/mexc-gate-scanner"),
               new=Path("/home/ubuntu/mexc-gate-scanner-v3"),
               python="/home/ubuntu/mexc-v3-venv/bin/python", drain_seconds=45):
    old, new = Path(old).resolve(strict=True), Path(new).resolve(strict=True)
    with open(old / ".v248_restart.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        proc = Path("/proc") / str(pid)
        require(proc.stat().st_uid == os.getuid(), "Le PID appartient à un autre utilisateur.")
        require((proc / "cwd").resolve(strict=True) == old, "Le PID ne correspond plus à la V2.")
        argv = (proc / "cmdline").read_bytes().split(b"\0")
        require(any(a in (b"app.py", os.fsencode(old / "app.py")) for a in argv), "PID inattendu.")
        identity = (proc / "stat").read_text().rsplit(")", 1)[1].split()[19]
        pidfd = os.pidfd_open(pid)
        try:
            require((proc / "stat").read_text().rsplit(")", 1)[1].split()[19] == identity, "PID réutilisé.")
            # Seuls le port et les chemins utiles sont retenus ; aucune clé copiée.
            env = {}
            for item in (proc / "environ").read_bytes().split(b"\0"):
                key, sep, value = item.partition(b"=")
                if sep and key in (b"PORT", b"LIVE_STOP_FILE", b"LIVE_DB_PATH"):
                    env[os.fsdecode(key)] = os.fsdecode(value)
            port = int(env.get("PORT", "8081"))
            url = f"http://127.0.0.1:{port}"
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            def read():
                with opener.open(url + "/api/status", timeout=5) as response:
                    return json.load(response)
            require(read().get("version") == "2.4.14-trade-quota-session", "Statut V2 inattendu.")
            subprocess.run([python, "verifier_v3.py"], cwd=new, check=True, timeout=60)
            stop = Path(env.get("LIVE_STOP_FILE", "/home/ubuntu/.config/mexc-bot/LIVE_STOP"))
            if not stop.is_absolute():
                stop = old / stop
            stop.parent.mkdir(parents=True, exist_ok=True)
            try:
                fd = os.open(stop, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                require(stop.is_file() and not stop.is_symlink(), "Fichier STOP invalide.")
            else:
                with os.fdopen(fd, "w") as stream:
                    stream.write("Transition V3 observation : nouvelles entrées suspendues.\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            stop_stat = stop.stat()
            stop_identity = (stop_stat.st_dev, stop_stat.st_ino, stop_stat.st_mtime_ns)
            print("V2 : nouvelles entrées suspendues ; attente des exécutions en cours.", flush=True)
            deadline, quiet = time.monotonic() + drain_seconds, 0
            while time.monotonic() < deadline:
                status = read()
                quiet = quiet + 1 if idle(status) else 0
                if quiet >= 2:
                    break
                live = status.get("live") or {}
                print("Attente : busy=", live.get("busy"), "; expositions=", live.get("open_exposures"), flush=True)
                time.sleep(1)
            require(quiet >= 2, "Exécution ou exposition persistante : V2 conservée, V3 non lancée.")
            db = Path(env.get("LIVE_DB_PATH", "mexc_live_v246.db"))
            db = db if db.is_absolute() else old / db
            con = sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
            try:
                pending = con.execute("SELECT COUNT(*) FROM live_orders_v240 WHERE COALESCE(final_ts_ms,0)=0").fetchone()[0]
            finally:
                con.close()
            require(pending == 0, "Ordre non final dans le journal : V2 conservée, V3 non lancée.")
            current_stop = stop.stat()
            require((current_stop.st_dev, current_stop.st_ino, current_stop.st_mtime_ns) == stop_identity
                    and idle(read()), "Les contrôles ont changé : V2 conservée.")
            signal.pidfd_send_signal(pidfd, signal.SIGTERM)
            require(bool(select.select([pidfd], [], [], 15)[0]), "V2 encore présente : V3 non lancée.")
            print("V2 arrêtée ; démarrage V3 en observation sur le port", port, flush=True)
        finally:
            os.close(pidfd)
        child_env = os.environ.copy()
        child_env["MEXC_V3_LIVE"] = "0"
        for key in ("MEXC_API_KEY", "MEXC_API_SECRET"):
            child_env.pop(key, None)
        log_path = new / "scanner_v3_observe.log"
        with open(log_path, "ab", buffering=0) as log:
            child = subprocess.Popen([python, "-u", "app.py", "--mode", "observe",
                "--data-dir", "data_v3", "--host", "0.0.0.0", "--port", str(port)],
                cwd=new, env=child_env, stdin=subprocess.DEVNULL, stdout=log,
                stderr=subprocess.STDOUT, start_new_session=True)
        print("PID V3 :", child.pid, "| journal :", log_path.name, flush=True)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            require(child.poll() is None, "Démarrage V3 échoué : consulter scanner_v3_observe.log.")
            try:
                status = read()
            except (OSError, ValueError):
                time.sleep(1)
                continue
            require(status.get("version") == "3.0.0-candidate.1" and status.get("mode") == "observe"
                    and status.get("live") is None, "Statut V3 inattendu : consulter le journal.")
            print("V3 observation démarrée :", status["version"], flush=True)
            print("WS :", status["flow"]["connected"], "/", status["flow"]["total"],
                  "| Depth :", status["depth"]["ready"], "/", status["depth"]["total"], flush=True)
            print("Interface : ton adresse habituelle, port", port, flush=True)
            print("Surveillance :", python, "surveiller_v3.py --url", url, "--minutes 10 --interval 5", flush=True)
            return
        raise RuntimeError("PID V3 lancé mais interface non confirmée : consulter scanner_v3_observe.log.")


if __name__ == "__main__":
    try:
        transition()
    except Exception as exc:
        raise SystemExit("Transition interrompue : " + str(exc))
