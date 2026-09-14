#!/usr/bin/env python3
"""Collect public-flow evidence from localhost; no restart or arming changes."""
import argparse
import collections
import datetime
import gzip
import json
import os
from pathlib import Path
import time
import urllib.request


def collect(minutes=20,port=8081,root=None):
    root=Path(root or Path.cwd())
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    samples=[];errors=[];started=time.monotonic();deadline=started+minutes*60
    origin=None;first=None;next_print=0
    print('Surveillance des flux sur le processus actuel ; aucun réarmement.',flush=True)
    try:
        while time.monotonic()<deadline:
            tick=time.monotonic()
            try:
                with opener.open(f'http://127.0.0.1:{port}/api/status',timeout=5) as response:
                    raw=response.read(8*1024*1024+1)
                if len(raw)>8*1024*1024:raise ValueError('Statut trop volumineux')
                d=json.loads(raw);h=d['health'];p=d['public_flow'];lv=d['live']
                current=(d['version'],p.get('process_started_ms'))
                if origin is not None and current!=origin:
                    errors.append({'reason':'process_changed'});break
                origin=current
                sample={k:d.get(k) for k in ('version','ws','ws_expected','depth_ready','symbols','health','public_flow')}
                sample.update(elapsed_s=round(time.monotonic()-started,3),ts_ms=int(time.time()*1000),
                              live={k:lv.get(k) for k in ('arm','session','circuit_open','open_exposures')})
                if first is None:first=sample
                samples.append(sample)
                if time.monotonic()>=next_print:
                    active=[w for w in p.get('workers',[]) if w.get('active')]
                    wait=max((float(w.get('queue_age_ms') or 0) for w in active),default=0)
                    delta={k:int(h.get(k,0))-int(first['health'].get(k,0)) for k in ('ws_disconnects','ws_reconnects')}
                    print(f"{sample['elapsed_s']:5.0f}s | WS {d['ws']}/{d['ws_expected']} | Depth {d['depth_ready']}/{d['symbols']} | "
                          f"chutes +{delta['ws_disconnects']} | reconnexions +{delta['ws_reconnects']} | "
                          f"rattrapages {sum(bool(w.get('catching_up')) for w in active)} | file max {wait:.1f} ms",flush=True)
                    missing=h.get('depth_not_ready',[])
                    if missing:print('Carnets non prêts :',', '.join(x['symbol']+(' (en file)' if x.get('pending') else ' (hors file)') for x in missing[:8])+(' …' if len(missing)>8 else ''),flush=True)
                    next_print=time.monotonic()+15
            except (OSError,ValueError,KeyError,TypeError) as exc:
                errors.append({'elapsed_s':round(time.monotonic()-started,3),'error':type(exc).__name__})
                print('Statut indisponible :',type(exc).__name__,flush=True)
            time.sleep(max(0,min(5-(time.monotonic()-tick),deadline-time.monotonic())))
    except KeyboardInterrupt:print('Collecte interrompue ; sauvegarde du bilan partiel.',flush=True)
    report={'samples':samples,'errors':errors,'requested_minutes':minutes}
    if samples:
        last=samples[-1];first=samples[0]
        report['summary']={
            'window_seconds':last['elapsed_s']-first['elapsed_s'],
            'depth_min':min(s['depth_ready'] for s in samples),
            'depth_max':max(s['depth_ready'] for s in samples),
            'depth_final':last['depth_ready'],
            'full_depth_samples':sum(s['depth_ready']==s['symbols'] for s in samples),
            'valid_samples':len(samples),
            'flow_delta':{k:int(last['health'].get(k,0))-int(first['health'].get(k,0)) for k in ('ws_disconnects','ws_reconnects','depth_gap_events','depth_resync_failures')},
            'catchup_delta':{k:int(last['public_flow'].get('totals',{}).get(k,0))-int(first['public_flow'].get('totals',{}).get(k,0)) for k in ('catchup_started','catchup_completed','hard_recoveries')},
            'final_missing_books':last['health'].get('depth_not_ready',[])}
        events={json.dumps(e,sort_keys=True):e for s in samples for e in s['health'].get('public_ws_close_events',[])}
        selected=[e for e in events.values() if first['ts_ms']<=e['ts_ms']<=last['ts_ms']]
        report['summary']['close_reasons']=dict(collections.Counter(e.get('local_reason') or 'transport_without_local_reason' for e in selected))
        print('Bilan :',json.dumps(report['summary'],ensure_ascii=False),flush=True)
    stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
    target=root/('bilan_flux_v2413_'+stamp+'.json.gz')
    with gzip.open(target,'xt',encoding='utf-8') as out:json.dump(report,out,ensure_ascii=False)
    os.chmod(target,0o600)
    print('Fichier à transmettre ici :',target,flush=True)
    return target


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--minutes',type=float,default=20)
    parser.add_argument('--port',type=int,default=8081)
    args=parser.parse_args()
    if not 1<=args.minutes<=60 or not 1<=args.port<=65535:parser.error('Durée 1–60 min et port valide requis.')
    collect(args.minutes,args.port)


if __name__=='__main__':main()
