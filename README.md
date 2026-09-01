# Scanner MEXC ↔ Gate — SOL & SUI

Scanner **lecture seule** : aucune clé API, aucun ordre, aucun accès à tes fonds.

## Lancer localement
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```
Puis ouvre `http://127.0.0.1:8080`.

## Sur un VPS
Même procédure, puis ouvre `http://IP_DU_VPS:8080` depuis l'iPhone. Pour un usage permanent, mets ensuite un reverse proxy HTTPS (Caddy/Nginx) et limite l'accès par mot de passe/VPN.

## Réglages
Les valeurs sont des variables d'environnement :
```bash
export PAIRS=SOLUSDT,SUIUSDT
export MEXC_TAKER_FEE=0.0005
export GATE_TAKER_FEE=0.0010
export MIN_NET_SPREAD=0.0015
export POLL_SECONDS=1
python app.py
```
`0.0015 = 0,15 %`.

**Important :** renseigne les frais réellement affichés sur tes comptes. Le scanner ne modélise pas encore la profondeur complète pour une taille donnée ; v1 utilise le meilleur bid/ask. Avant trading réel, la v2 devra intégrer VWAP/profondeur, latence, risque de non-exécution et limites d'inventaire.
