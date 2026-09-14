#!/usr/bin/env bash
cd /home/ubuntu/mexc-gate-scanner || exit 1
python3 -u - <<'PY'
import collections, datetime, gzip, json, os, time, urllib.request
from pathlib import Path

DUREE = 300
INTERVALLE = 3
URL = "http://127.0.0.1:8081/api/status"
OUVREUR = urllib.request.build_opener(urllib.request.ProxyHandler({}))

class Suivi:
    def __init__(self):
        self.precedent = None
        self.comptes = collections.Counter()
        self.par_ws = collections.defaultdict(collections.Counter)
        self.anciens = {}
        self.evenements = set()
        self.motifs = collections.Counter()
        self.durees = []
        self.lignes = []
        self.incidents = []
        self.changements = 0
        self.resets = 0
        self.origine_ms = 0

    def relever(self, d, secondes, requete_ms):
        h, p = d.get('health') or {}, d.get('public_flow') or {}
        workers = p.get('workers') or []
        if not workers or any(k not in h for k in ('ws_reconnects', 'ws_disconnects')):
            raise ValueError('Statut public incomplet')
        if any(k not in w for w in workers for k in ('epoch', 'catchup_count', 'catchup_completed')):
            raise ValueError('Compteurs V2.4.9 absents')
        compteurs = {k: int(h[k]) for k in ('ws_reconnects', 'ws_disconnects')}
        reset = self.precedent is not None and (
            d.get('version') != self.version or any(compteurs[k] < self.precedent[k] for k in compteurs))
        if reset:
            self.resets += 1
            self.anciens.clear()
        initial = self.precedent is None
        if initial:
            self.origine_ms = int(time.time() * 1000)
        else:
            for k, v in compteurs.items():
                self.comptes[k] += v if reset else v - self.precedent[k]
        self.version = d.get('version')
        self.precedent = compteurs
        for w in workers:
            wid, epoch = int(w['worker_id']), int(w['epoch'])
            a, b = int(w['catchup_count']), int(w['catchup_completed'])
            old = self.anciens.get(wid)
            if old is not None:
                if epoch != old[0] or a < old[1] or b < old[2]:
                    self.changements += 1
                    da, db = a, b
                else:
                    da, db = a - old[1], b - old[2]
            elif reset:
                da, db = a, b
            else:
                da = db = 0
            self.anciens[wid] = (epoch, a, b)
            c = self.par_ws[wid]
            c['debuts'] += da
            c['fins'] += db
            c['releves'] += 1
            c['en_rattrapage'] += int(bool(w.get('catching_up')) and bool(w.get('active')))
            if w.get('active'):
                c['attente_max_ms'] = max(c['attente_max_ms'], float(w.get('queue_age_ms') or 0))
        for e in (p.get('recent_recoveries') or []) + (p.get('recent_catchups') or []):
            cle = (self.resets, json.dumps(e, sort_keys=True))
            if cle in self.evenements:
                continue
            self.evenements.add(cle)
            if initial or int(e.get('ts_ms') or 0) <= self.origine_ms:
                continue
            if e.get('reason'):
                self.motifs[e['reason']] += 1
            if e.get('event') == 'resumed' and e.get('duration_ms') is not None:
                self.durees.append(float(e['duration_ms']))
        actifs = [w for w in workers if w.get('active')]
        arretes = [w for w in workers if not w.get('active')]
        def attente(groupe):
            return round(max(float(w.get('queue_age_ms') or 0) for w in groupe), 1) if groupe else None
        ligne = dict(secondes=round(secondes, 1), requete_ms=round(requete_ms, 1),
            version=self.version, ws=d.get('ws'), attendus=d.get('ws_expected'),
            carnets=d.get('depth_ready'), total=d.get('symbols'),
            rattrapages=sum(bool(w.get('active')) and bool(w.get('catching_up')) for w in workers),
            attente_max_ms=attente(actifs), attente_files_arretees_max_ms=attente(arretes),
            attente_scope='flux_actifs_uniquement',
            file_max=max((int(w.get('queue') or 0) for w in actifs), default=None),
            resync=h.get('resync_pending'), scan_queue=h.get('scan_queue'), db_queue=h.get('db_queue'))
        self.lignes.append(ligne)
        return ligne

def main():
    suivi = Suivi()
    debut = time.monotonic()
    prochain = 0
    traces = []
    print('Surveillance 5 minutes, un relevé toutes les 3 s. Ctrl+C : bilan anticipé.', flush=True)
    try:
        while time.monotonic() - debut < DUREE:
            t = time.monotonic()
            try:
                with OUVREUR.open(URL, timeout=min(5, max(.1, DUREE - (t - debut)))) as r:
                    brut = r.read(8 * 1024 * 1024 + 1)
                if len(brut) > 8 * 1024 * 1024:
                    raise ValueError('Statut trop volumineux')
                d = json.loads(brut)
                ligne = suivi.relever(d, time.monotonic() - debut, (time.monotonic() - t) * 1000)
                traces.append(dict(releve=ligne, public_flow=d.get('public_flow')))
                if time.monotonic() - debut >= prochain:
                    print(f"{ligne['secondes']:5.0f}s | WS {ligne['ws']}/{ligne['attendus']} | "
                          f"Depth {ligne['carnets']}/{ligne['total']} | "
                          f"rattrapage {ligne['rattrapages']} | attente actifs max {ligne['attente_max_ms']} ms | "
                          f"reconnexions +{suivi.comptes['ws_reconnects']}", flush=True)
                    prochain = time.monotonic() - debut + 15
            except Exception as e:
                suivi.incidents.append(dict(secondes=round(time.monotonic() - debut, 1), erreur=type(e).__name__))
                print('Relevé indisponible :', type(e).__name__, flush=True)
            time.sleep(max(0, min(INTERVALLE - (time.monotonic() - t), DUREE - (time.monotonic() - debut))))
    except KeyboardInterrupt:
        print('\nSurveillance interrompue, bilan partiel.')
    print('\n===== BILAN A RENVOYER =====')
    if not suivi.lignes:
        print('Aucun statut valide : impossible de compter les événements.')
        return
    ls = suivi.lignes
    print('Version :', suivi.version)
    print('Fenêtre entre relevés :', round(ls[-1]['secondes'] - ls[0]['secondes'], 1), 's')
    print('Relevés valides / erreurs :', len(ls), '/', len(suivi.incidents))
    print('Déconnexions / reconnexions :', suivi.comptes['ws_disconnects'], '/', suivi.comptes['ws_reconnects'])
    print('Rattrapages commencés / terminés observés :', sum(c['debuts'] for c in suivi.par_ws.values()), '/', sum(c['fins'] for c in suivi.par_ws.values()))
    print('Changements de génération / remises à zéro détectées :', suivi.changements, '/', suivi.resets)
    print('WS min / max / fin :', min(x['ws'] for x in ls), '/', max(x['ws'] for x in ls), '/', ls[-1]['ws'])
    print('Depth début / max / fin :', ls[0]['carnets'], '/', max(x['carnets'] for x in ls), '/', ls[-1]['carnets'])
    print('WS en rattrapage simultané max / fin :', max(x['rattrapages'] for x in ls), '/', ls[-1]['rattrapages'])
    print('Attente maximale échantillonnée, flux actifs uniquement :', max((x['attente_max_ms'] for x in ls if x['attente_max_ms'] is not None), default=None), 'ms')
    print('Motifs de récupération observés :', dict(suivi.motifs))
    if suivi.durees:
        ds = sorted(suivi.durees)
        print('Durées de rattrapage observées :', len(ds), 'fins ; moyenne / p95 / max ms :',
              round(sum(ds)/len(ds), 1), '/', ds[int((len(ds)-1)*.95)], '/', ds[-1])
    print('WS les plus touchés : ID | débuts | fins | % des relevés en rattrapage | attente max ms')
    for wid, c in sorted(suivi.par_ws.items(), key=lambda x: (x[1]['debuts'], x[1]['en_rattrapage']), reverse=True)[:10]:
        print(wid, '|', c['debuts'], '|', c['fins'], '|', round(100*c['en_rattrapage']/c['releves'], 1), '|', round(c['attente_max_ms'], 1))
    print('Les compteurs excluent le relevé initial. Les changements de génération peuvent faire manquer des rattrapages : totaux observés = minimum.')
    print('Les durées/motifs proviennent des événements encore visibles ; les historiques du bot sont limités à 200 entrées. Les maxima sont échantillonnés.')
    fichier = Path('surveillance_mexc_' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S_%f') + '.json.gz')
    try:
        with gzip.open(fichier, 'xt', encoding='utf-8') as f:
            json.dump(dict(releves=traces, erreurs=suivi.incidents, comptes=dict(suivi.comptes),
                par_ws=dict(suivi.par_ws), motifs=dict(suivi.motifs), durees_ms=suivi.durees,
                changements_generation=suivi.changements, resets=suivi.resets), f, ensure_ascii=False)
        print('Détails enregistrés :', fichier.resolve())
    except OSError as e:
        print('Fichier non enregistré :', type(e).__name__, '; conserver le bilan affiché.')
    print('===== FIN BILAN =====')

if __name__ == '__main__':
    main()
PY
