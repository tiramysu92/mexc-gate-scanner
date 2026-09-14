#!/usr/bin/env python3
"""Collecte complémentaire CTO: réponses d'ordres, confirmations et carnets.
Lecture seule du journal local; aucun import app.py ni appel MEXC.
"""
import argparse
import datetime as dt
import gzip
import json
from pathlib import Path
import re
import sqlite3
import time
from regulariser_cto import ATTEMPT


def clean(value):
    if isinstance(value,list):return [clean(x) for x in value]
    if isinstance(value,dict):
        return {k:clean(v) for k,v in value.items() if k.lower() not in
                ('apikey','api_key','api_secret','signature','listenkey','authorization','balances_json')}
    if isinstance(value,str):
        return re.sub(r'(?i)((?:signature|api[_-]?key|api[_-]?secret|listenkey)[\s\"\x27]*[:=][\s\"\x27]*)([^&\s\"\x27,}]+)',r'\1[MASQUE]',value)
    return value


def collect(path,out_dir='.'):
    path=Path(path).resolve()
    report={'attempt_id':ATTEMPT,'collected_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'database':str(path),'tables':{}}
    con=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2);con.row_factory=sqlite3.Row
    try:
        con.execute('PRAGMA query_only=ON');con.execute('BEGIN')
        deadline=time.monotonic()+10
        con.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        tables={r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        queries={
            'live_attempts_v240':('attempt_id=?',(ATTEMPT,)),
            'live_orders_v240':('attempt_id=?',(ATTEMPT,)),
            'live_observations_v247':('attempt_id=?',(ATTEMPT,)),
            'live_circuit_events_v246':('attempt_id=?',(ATTEMPT,)),
            'private_order_ws_events_v245':('client_order_id IN (SELECT client_order_id FROM live_orders_v240 WHERE attempt_id=?)',(ATTEMPT,)),
        }
        for table,(where,params) in queries.items():
            if table not in tables:
                report['tables'][table]={'missing':True};continue
            try:
                rows=[]
                for row in con.execute('SELECT * FROM '+table+' WHERE '+where+' LIMIT 101',params):
                    item=dict(row)
                    for key in list(item):
                        if key.endswith('_json') and isinstance(item[key],str):
                            try:item[key]=json.loads(item[key])
                            except ValueError:pass
                    rows.append(clean(item))
                report['tables'][table]={'rows':rows[:100],'truncated':len(rows)>100}
            except sqlite3.Error as exc:report['tables'][table]={'error':str(exc)}
    finally:con.close()
    stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    out=Path(out_dir).resolve()/('incident_cto_complet_'+stamp+'.json.gz')
    with gzip.open(out,'xt',encoding='utf-8') as f:json.dump(report,f,ensure_ascii=False,indent=2)
    return out


if __name__=='__main__':
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--db',default='mexc_live_v246.db');args=ap.parse_args()
    print('Fichier à renvoyer :',collect(args.db))
