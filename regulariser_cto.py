#!/usr/bin/env python3
"""Rapprochement du CTO vendu manuellement. Aucun appel MEXC ni réarmement.

Par défaut: contrôle en lecture seule. --apply exige le scanner arrêté,
sauvegarde le journal, conserve les ordres et les causes du circuit.
"""
import argparse
import datetime as dt
from decimal import Decimal
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

ATTEMPT='6ffdaa1aa4b545209d9058accda38e76'
QUANTITY=Decimal('19801.66')
GROSS=Decimal('7.28701088')
FEE=Decimal('0.00364350')
NET=GROSS-FEE
EXPECTED_COST=Decimal('19.97005331664')
SALE_TIME='2026-09-14T15:00:53+02:00'
RESOLUTION='cto_manual_sale_20260914_150053'


def require(ok,detail):
    if not ok:raise RuntimeError(detail)


def close(a,b):
    return abs(Decimal(str(a))-Decimal(str(b)))<=Decimal('0.00000001')


def inspect(con):
    con.row_factory=sqlite3.Row
    a=con.execute('SELECT * FROM live_attempts_v240 WHERE attempt_id=?',(ATTEMPT,)).fetchone()
    require(a is not None,'Trade CTO introuvable: aucune modification.')
    e=con.execute('SELECT * FROM live_open_exposures_v246 WHERE attempt_id=?',(ATTEMPT,)).fetchone()
    require(e is not None,'Exposition CTO introuvable: aucune modification.')
    tables={r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    audit=con.execute('SELECT * FROM live_manual_resolutions_v2411 WHERE resolution_id=?',(RESOLUTION,)).fetchone() if 'live_manual_resolutions_v2411' in tables else None
    if audit is not None:
        require(a['status']=='manual_closed' and e['status']=='closed' and close(e['units'],0)
                and close(a['realized_pnl'],NET-EXPECTED_COST) and close(a['cash_output_usd'],NET)
                and audit['attempt_id']==ATTEMPT and close(audit['net_usdt'],NET),
                'Une régularisation existe mais ses montants divergent.')
        return {'already_applied':True,'realized_pnl_usdt':str(NET-EXPECTED_COST)}
    require(a['status']=='open_exposure' and e['status']=='open','État CTO différent du diagnostic: aucune modification.')
    require(a['mode']=='live' and a['route_id']=='USDT>CTO>USD1' and a['mid_asset']=='CTO','Identité du trade incorrecte.')
    require(close(a['mid_acquired'],QUANTITY) and close(a['residual_mid'],QUANTITY)
            and close(e['units'],QUANTITY),'La quantité CTO a changé.')
    require(close(a['input_units'],EXPECTED_COST) and close(a['residual_cost_usd'],EXPECTED_COST),
            'Le coût du trade a changé.')
    for k in ('mid_used','output_units','unwind_input','unwind_output','cash_output_usd'):
        require(a[k] is not None and close(a[k],0),'Des ventes ont déjà été comptabilisées: '+k)
    orders=[dict(r) for r in con.execute('SELECT * FROM live_orders_v240 WHERE attempt_id=?',(ATTEMPT,))]
    buys=[r for r in orders if r['purpose']=='leg1']
    exits=[r for r in orders if r['purpose']!='leg1']
    require(len(buys)==1 and buys[0]['status']=='FILLED' and close(buys[0]['executed_qty'],QUANTITY),'Achat CTO non confirmé.')
    require(len(exits)==1 and exits[0]['purpose']=='unwind' and exits[0]['status']=='CANCELED'
            and close(exits[0]['executed_qty'],0) and close(exits[0]['cumulative_quote_qty'],0),
            'Ordre de sortie différent du diagnostic: régularisation refusée.')
    state=con.execute('SELECT * FROM live_state_v240 WHERE singleton=1').fetchone()
    require(state is not None and state['circuit_open']==1,'Circuit non ouvert: régularisation refusée.')
    return {'already_applied':False,'attempt_id':ATTEMPT,'quantity_cto':str(QUANTITY),
        'gross_usdt':str(GROSS),'fee_usdt':str(FEE),'net_usdt':str(NET),
        'cost_usdt':str(EXPECTED_COST),'realized_pnl_usdt':str(NET-EXPECTED_COST),
        'sale_time':SALE_TIME,'source':'Vente sur compte principal déclarée par utilisateur; captures IMG_2185 et IMG_2183(1). Retour USDT au sous-compte déclaré.',
        'before_attempt':dict(a),'before_exposure':dict(e)}


def assert_scanner_stopped(root):
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit() or int(proc.name)==os.getpid():continue
        try:
            if (proc/'cwd').resolve()!=root:continue
            args=(proc/'cmdline').read_bytes().split(b'\0')
            if any(x==b'app.py' or x.endswith(b'/app.py') for x in args):
                raise RuntimeError('Scanner encore actif: utiliser la relance V2.4.11 pour régulariser entre arrêt et redémarrage.')
        except (FileNotFoundError,ProcessLookupError):continue
        except PermissionError:
            # A process whose cwd cannot be read cannot be identified as ours.
            continue


def reconcile(path,apply=False):
    path=Path(path).resolve()
    require(path.is_file(),'Journal absent: aucune création de base.')
    if apply:
        for root in {Path.cwd().resolve(),path.parent}:assert_scanner_stopped(root)
    con=sqlite3.connect(path.as_uri()+('?mode=rw' if apply else '?mode=ro'),uri=True,timeout=3)
    backup=None
    try:
        if not apply:con.execute('PRAGMA query_only=ON')
        plan=inspect(con)
        if not apply or plan['already_applied']:return plan
        stamp=dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S')
        backup=path.with_name(path.stem+'_avant_cto_'+stamp+'_'+uuid.uuid4().hex[:8]+'.db')
        with open(backup,'xb'):pass
        os.chmod(backup,0o600)
        dest=sqlite3.connect(backup)
        try:con.backup(dest)
        finally:dest.close()
        con.execute('PRAGMA synchronous=FULL')
        con.execute('BEGIN IMMEDIATE')
        plan=inspect(con)
        require(not plan['already_applied'],'Régularisation concurrente détectée.')
        now=int(time.time()*1000);pnl=float(NET-EXPECTED_COST)
        con.execute('''CREATE TABLE IF NOT EXISTS live_manual_resolutions_v2411(
            resolution_id TEXT PRIMARY KEY,attempt_id TEXT NOT NULL UNIQUE,recorded_ts_ms INTEGER NOT NULL,
            sale_time TEXT NOT NULL,quantity TEXT NOT NULL,gross_usdt TEXT NOT NULL,
            fee_usdt TEXT NOT NULL,net_usdt TEXT NOT NULL,realized_pnl_usdt TEXT NOT NULL,
            evidence_json TEXT NOT NULL)''')
        con.execute('INSERT INTO live_manual_resolutions_v2411 VALUES(?,?,?,?,?,?,?,?,?,?)',
            (RESOLUTION,ATTEMPT,now,SALE_TIME,str(QUANTITY),str(GROSS),str(FEE),str(NET),str(NET-EXPECTED_COST),json.dumps(plan,ensure_ascii=False)))
        # Preserve bot order quantities/statuses: this sale happened elsewhere.
        con.execute('''UPDATE live_attempts_v240 SET status='manual_closed',updated_ts_ms=?,
            residual_mid=0,residual_value_usd=0,residual_cost_usd=0,unrealized_pnl=0,
            cash_output_usd=?,realized_cost_usd=?,realized_pnl=?,economic_pnl=? WHERE attempt_id=?''',
            (now,float(NET),float(EXPECTED_COST),pnl,pnl,ATTEMPT))
        con.execute('''UPDATE live_open_exposures_v246 SET status='closed',units=0,
            updated_ts_ms=?,closed_ts_ms=?,cash_output_usd=?,residual_cost_usd=0,
            last_value_usd=0,economic_pnl=?,resolution_note=? WHERE attempt_id=?''',
            (now,now,float(NET),pnl,RESOLUTION,ATTEMPT))
        con.execute('''INSERT INTO live_circuit_events_v246(ts_ms,day,attempt_id,reason,error,active)
            VALUES(?, '2026-09-14',?,'manual_close_review_required',?,1)''',
            (now,ATTEMPT,'CTO vendu manuellement; perte '+str(-Decimal(str(pnl)))+' USDT; réarmement non autorisé par cette opération'))
        reasons=[r[0] for r in con.execute('SELECT reason FROM live_circuit_events_v246 WHERE active=1 ORDER BY event_id')]
        con.execute('''UPDATE live_state_v240 SET circuit_open=1,circuit_reason=?,updated_ts_ms=?,
            last_status=CASE WHEN last_attempt_id=? THEN 'manual_closed' ELSE last_status END WHERE singleton=1''',
            (' | '.join(dict.fromkeys(reasons)),now,ATTEMPT))
        con.commit()
        plan.update(applied=True,backup=str(backup),circuit_open=True)
        return plan
    except Exception:
        if con.in_transaction:con.rollback()
        raise
    finally:con.close()


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--db',default='mexc_live_v246.db')
    ap.add_argument('--apply',action='store_true')
    args=ap.parse_args()
    try:
        result=reconcile(args.db,args.apply)
        print(json.dumps({k:v for k,v in result.items() if not k.startswith('before_')},ensure_ascii=False,indent=2))
    except Exception as exc:
        print('STOP :',str(exc));return 1
    return 0


if __name__=='__main__':raise SystemExit(main())
