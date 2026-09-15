#!/usr/bin/env python3
"""V3 entry point. Default: public observation and separate paper journals."""
import argparse
from pathlib import Path
import signal
import threading
from mexc_v3.config import Policy
from mexc_v3.journal import dumps


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode",choices=("observe","live","demo"),default="observe")
    parser.add_argument("--data-dir",default="data_v3")
    parser.add_argument("--policy")
    parser.add_argument("--host",default="127.0.0.1")
    parser.add_argument("--port",type=int,default=8083)
    parser.add_argument("--initialize-live-from",metavar="V2_DATABASE")
    args = parser.parse_args()
    policy = Policy.read(args.policy)
    if args.initialize_live_from:
        from mexc_v3.live import initialize
        print(initialize(args.data_dir,args.initialize_live_from,policy))
        print("Historique importé. LIVE désactivé, STOP conservé. Aucun ordre envoyé.")
        return
    if args.mode == "demo":
        from mexc_v3.demo import run
        print(dumps(run(Path(args.data_dir)/"demo.db")))
        return
    from mexc_v3.service import Service
    from mexc_v3.dashboard import server
    service = Service(args.data_dir,policy)
    service.prepare()
    if args.mode == "live":
        from mexc_v3.live import attach
        attach(service)
    http = server(service,args.host,args.port)
    http_thread = threading.Thread(target=http.serve_forever,daemon=True)
    def stop(*_):
        service.done.set()
    signal.signal(signal.SIGTERM,stop)
    signal.signal(signal.SIGINT,stop)
    service.start()
    http_thread.start()
    print(f"V3 {args.mode} · http://{args.host}:{args.port} · {len(service.routes)} routes",flush=True)
    try:
        service.done.wait()
    finally:
        # Closing a browser or a monitoring command never touches an arm file.
        http.shutdown()
        http.server_close()
        service.close()


if __name__ == "__main__":
    main()
