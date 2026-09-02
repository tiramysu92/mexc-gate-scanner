# MEXC ↔ Gate Arbitrage Scanner V3

Scanner **lecture seule** : aucune clé API, aucun ordre.

## Ce qui change par rapport à V2

- Tailles : **250 / 500 / 1 000 / 2 000 USDT**
- Univers dynamique : jusqu'à **150 paires USDT communes** à MEXC et Gate
- Préférence aux marchés moins liquides mais encore tradables
- Paires V2 intéressantes épinglées : ARB, FET, SEI, NEAR, XRP, OP, SUI, DOGE, AAVE, RENDER, PEPE, ADA
- Gate BBO via WebSocket
- MEXC BBO récupéré en **un seul appel pour toutes les paires**, cible 100 ms
- Vérification exacte par carnet 100 niveaux uniquement lorsque le spread BBO devient intéressant
- Les ticks positifs sont regroupés en **événements**
- Le dashboard compte donc le **nombre d'opportunités par token**
- Base séparée : `scanner_v3.db`

## Pourquoi MEXC n'utilise pas directement son WebSocket ici ?

Le flux Spot WebSocket MEXC actuel est en Protocol Buffers. Pour rendre le déploiement V3 simple et robuste sur le VPS actuel, le scanner utilise l'endpoint public `ticker/bookTicker` qui renvoie **tous les symboles en un appel**, avec une cible de 100 ms. Cela supprime le défaut V2 où ~30 requêtes séquentielles donnaient ~40 secondes entre deux mesures d'une même paire.

La V3 affiche le `cycle MEXC` réel sur la page. Il faut se fier à cette valeur mesurée et non à la cible théorique de 100 ms.

Une V3.1 pourra passer le côté MEXC en WebSocket Protocol Buffers 100 ms/10 ms si l'on veut encore réduire la latence.

## Installation / mise à jour

Dans le dépôt :

```bash
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Dashboard : port 8080.

## Variables utiles

```bash
MAX_PAIRS=150
MEXC_BBO_INTERVAL=0.10
MIN_QUOTE_VOL_24H=100000
MAX_QUOTE_VOL_24H=75000000
PREFILTER_GROSS=0.0004
VERIFY_COOLDOWN_MS=250
EVENT_CLOSE_GAP_MS=1000
```

## Base SQLite

### `verified_checks`
Mesures exactes après lecture de la profondeur pour chacune des quatre tailles.

### `opportunities`
Une ligne = **une opportunité**, et non un tick.
Champs utiles :
- pair
- direction
- start_ms / end_ms
- duration_ms
- ticks
- peak_net / avg_net
- peak_profit
- best_size

### `universe_snapshots`
Pourquoi une paire a été sélectionnée : volumes 24 h et score de volatilité/liquidité.

## Important

Le profit affiché est un **profit théorique après frais de trading configurés**, mais avant risque de latence/exécution, retrait, rééquilibrage, etc. Ce scanner sert à identifier les marchés à tester avant tout bot d'exécution.
