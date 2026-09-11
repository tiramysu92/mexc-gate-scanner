# MEXC Spot 3-leg Scanner — V3.0

Scanner autonome d'arbitrage triangulaire **spot uniquement** sur MEXC. Il est conçu pour tourner en parallèle du scanner 2-leg, sur le port `8083`, avec sa propre base SQLite et son propre cache de marchés.

Il n'utilise aucune clé API, n'envoie aucun ordre, et n'intègre ni futures ni perpetuals.

## Périmètre

Le scanner découvre automatiquement toutes les routes complètes de la forme :

```text
stablecoin de départ → actif A → actif B → stablecoin d'arrivée
```

Le stablecoin d'arrivée peut être :

- identique au stablecoin de départ : `USDT → A → B → USDT` ;
- différent : `USDT → A → B → USDC`.

Par défaut, les stablecoins suivis sont `USDT`, `USDC` et `USD1`. Les deux sens d'une paire intermédiaire sont construits lorsqu'ils existent.

## Hypothèses de frais et de latence

Chaque jambe paie `0,05 %` de frais taker :

```text
frais composés du cycle = 1 - (1 - 0,0005)^3 = 0,149925 %
```

L'edge affiché est déjà **net des trois frais**, du spread et de la profondeur consommée dans les simulations Depth.

| Profil | Jambe 1 | Jambe 2 | Jambe 3 |
| --- | ---: | ---: | ---: |
| FAST | +15 ms | +30 ms | +45 ms |
| TARGET | +25 ms | +50 ms | +75 ms |
| DEGRADED | +50 ms | +100 ms | +150 ms |

Les temps sont mesurés depuis la décision T0. Ainsi, par rapport au scanner 2-leg, chaque profil supporte bien une jambe d'exécution supplémentaire.

## Ce qui vient du retour d'expérience 2-leg

- décision immédiate à T0, sans attente artificielle de confirmation ;
- univers 3-leg séparé du 2-leg ;
- sélection de routes complètes avant la sélection des symboles ;
- tous les candidats 3-leg suivis par défaut (`FOLLOW_ALL_3LEG=1`) ;
- WebSockets limités à 20 symboles et équilibrés selon le volume quote 24 h ;
- PING/PONG applicatif MEXC, surveillance des sockets et reconnexion ;
- carnets Depth locaux avec snapshots REST, contrôle des versions et resynchronisation sérialisée ;
- rejet des décisions sur carnets trop âgés ou désynchronisés ;
- une seule entrée par événement, cooldown de 5 secondes et réarmement après retour neutre pendant 1 seconde ;
- taille dynamique, sans plafond arbitraire de 2 000 USDT ;
- portefeuilles FAST, TARGET et DEGRADED indépendants, remis à `1 000 USDT` au changement de journée ;
- soldes `USDT`, `USDC`, `USD1` séparés et allocation cible `40 % / 10 % / 50 %` ;
- rééquilibrage autorisé uniquement si les profits déjà gagnés couvrent son coût ;
- captures Depth T0 à T0+5 s pour permettre un replay ultérieur.

## Politique Depth Quality initiale

Le seuil d'admission reste `+0,35 % net`. L'ajout de la troisième commission relève automatiquement le seuil brut nécessaire d'environ `0,05 %` par rapport au 2-leg.

- **A** : 50 % de la capacité BBO ; reste au-dessus de `+0,35 % net` après suppression du niveau 1 sur les trois jambes.
- **B+** : 33 % de la capacité ; résiste à une réduction de 50 % du BBO sur les trois jambes, avec edge stressé d'au moins `+0,75 %`, edge sans L1 d'au moins `-2 %`, et réserve cumulée L1–L3 d'au moins `×3` sur chaque jambe.
- **B−, C, D** : enregistrés pour l'analyse, mais non exécutés dans les portefeuilles shadow.

Ce réglage volontairement comparable au 2-leg sert de référence de départ. Il faudra décider de l'abaisser ou de le durcir après un premier export suffisamment long.

## Modèle d'exécution shadow

Pour chaque signal admis, le scanner relit réellement le carnet de chaque jambe au délai correspondant au profil. Les trois jambes sont donc évaluées séquentiellement, et chaque `depth_walk` applique sa propre commission de `0,05 %`.

Le modèle exige un remplissage complet de chaque jambe :

- trois remplissages complets : le cycle est comptabilisé dans le NAV et le PnL ;
- carnet indisponible ou remplissage partiel : `failed_leg_1`, `failed_leg_2` ou `failed_leg_3` est enregistré, sans fabriquer de PnL.

Cette convention donne une comparaison propre entre les trois profils et conserve les données nécessaires au replay. Elle ne prétend pas rendre trois ordres réels atomiques : avant de passer à un bot d'exécution, il faudra ajouter une politique IOC/FOK ou une gestion explicite des inventaires intermédiaires et des unwinds.

## Installation sur le serveur

Utiliser un répertoire séparé de celui du scanner 2-leg afin que les deux branches puissent rester actives simultanément.

```bash
cd /home/ubuntu
git clone --branch scanner-3leg <URL_DU_DEPOT> mexc-3leg-scanner
cd mexc-3leg-scanner

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Démarrage sur le port 8083

```bash
cd /home/ubuntu/mexc-3leg-scanner
source .venv/bin/activate

nohup env \
  PORT=8083 \
  DB_PATH=/home/ubuntu/mexc_3leg_v300.db \
  MARKET_CACHE=/home/ubuntu/mexc_3leg_markets_cache.json \
  python3 app.py \
  > /home/ubuntu/mexc_3leg_v300.log 2>&1 &
```

Contrôles :

```bash
tail -f /home/ubuntu/mexc_3leg_v300.log
curl -s http://127.0.0.1:8083/healthz
curl -s http://127.0.0.1:8083/api/status | python3 -m json.tool
```

Tableau de bord :

```text
http://IP_DU_SERVEUR:8083/
```

Si le serveur applique un pare-feu, ouvrir uniquement le port TCP `8083` depuis les adresses qui doivent consulter le tableau de bord.

## Lecture du log de démarrage

Une ligne de ce type doit apparaître :

```text
[3L V3.0.0-tri-depth-shadow] source=MEXC v3 exchangeInfo markets=... candidates=... selected_symbols=... selected_routes=... dropped=0 WS=... group<=20 grouping=mexc_quote_volume_24h fees=3x0.050% port=8083
```

Points à vérifier :

- `selected_routes == candidates` ;
- `dropped=0` ;
- `WS` atteint le nombre attendu ;
- `Depth prêts` monte progressivement jusqu'au nombre de marchés WS ;
- aucun accroissement continu de `drops scan`, `échecs resync` ou sockets silencieux.

Le bootstrap Depth est volontairement sérialisé afin d'éviter une tempête de requêtes REST et des erreurs `429`. Avec plusieurs centaines de marchés, la montée complète peut prendre quelques minutes.

## Principales variables d'environnement

| Variable | Défaut | Rôle |
| --- | ---: | --- |
| `PORT` | `8083` | Port du tableau de bord |
| `DB_PATH` | `mexc_3leg_v300.db` | Base SQLite indépendante |
| `TAKER_FEE` | `0.0005` | Frais appliqués à chaque jambe |
| `STABLES` | `USDT,USDC,USD1` | Stablecoins de départ/arrivée |
| `FOLLOW_ALL_3LEG` | `1` | Suit l'univers 3-leg complet |
| `MAX_WS_SYMBOLS` | `900` | Budget si `FOLLOW_ALL_3LEG=0` |
| `WS_GROUP_SIZE` | `20` | Symboles maximum par socket |
| `MIN_EVENT_NET_PCT` | `0.01` | Plancher de collecte des événements |
| `SHADOW_MIN_EDGE_PCT` | `0.35` | Plancher net d'admission A/B+ |
| `SHADOW_BBO_AGE_MS` | `150` | Âge maximal T0 pour l'admission |
| `SHADOW_MAX_SKEW_MS` | `50` | Désynchronisation maximale T0 |
| `EXEC_BOOK_MAX_AGE_MS` | `1000` | Fraîcheur maximale lors d'une jambe simulée |
| `SIM_CAPITAL` | `1000` | Capital de chaque profil, remis à zéro par journée |
| `SIM_MIN_TRADE_USD` | `10` | Taille minimale simulée |
| `PAPER_HARD_COOLDOWN_MS` | `5000` | Cooldown après une entrée |
| `PAPER_REARM_NEUTRAL_MS` | `1000` | Temps neutre avant réarmement |

## Base SQLite et export sûr

Tables principales :

| Table | Contenu |
| --- | --- |
| `opportunities_3leg` | événements agrégés et durée |
| `decisions_3leg` | signal T0, trois BBO, qualité et admission |
| `decision_quality_3leg` | stress tests A/B+/B−/C/D |
| `decision_depth_samples_3leg` | carnets multi-niveaux de chaque jambe jusqu'à T0+5 s |
| `shadow_attempts_3leg` | résultat séquentiel par profil et détails des trois fills |
| `shadow_state_3leg` | soldes et compteurs quotidiens |
| `shadow_rebalances_3leg` | coûts réels des rééquilibrages simulés |
| `route_snapshots_3leg` | meilleures routes observées périodiquement |
| `ws_latency_3leg` | latence de transport MEXC |
| `depth_sync_events_3leg` | gaps et resynchronisations de carnets |

Pour exporter la base pendant que le scanner tourne :

```bash
sqlite3 /home/ubuntu/mexc_3leg_v300.db \
  ".backup /home/ubuntu/mexc_3leg_v300_export.db"
```

Ne pas copier directement le fichier `.db` actif sans son WAL. La commande `.backup` crée un export cohérent.

## Création de la branche GitHub

```bash
git switch -c scanner-3leg
git add app.py README.md requirements.txt
git commit -m "Add standalone MEXC 3-leg depth scanner"
git push -u origin scanner-3leg
```

Les fichiers runtime suivants ne doivent pas être versionnés :

```gitignore
*.db
*.db-wal
*.db-shm
*.log
mexc_3leg_markets_cache.json
.venv/
__pycache__/
```

