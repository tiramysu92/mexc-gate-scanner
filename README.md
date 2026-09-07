# MEXC Spot Routes V2.3 — Depth Strict

Version préparée après audit de la V2.2 TopBook LocalAge.

## Objectif
Scanner spot-only MEXC, routes 2 jambes entre USDT/USDC/USD1, avec carnet local multi-niveaux reconstruit via snapshot REST + diff-depth WebSocket 10 ms. Aucun futures/perp.

## Corrections V2.3
- DB séparée: `mexc_routes_v23.db`.
- Admission Paper basée sur âge/skew **locaux de réception**, jamais sur `sendtime MEXC -> VPS` non corrigé.
- Snapshot REST `/api/v3/depth?limit=100` + deltas WS versionnés avant qu'un carnet soit `ready`.
- Exécution séquentielle: leg 1 vers +75 ms, leg 2 vers +150 ms, `depth_walk` multi-niveaux.
- Une fois leg 1 exécutée, le résultat est irréversible: succès, perte ou échec de leg 2 est compté.
- Si leg 2 est partielle/impossible: emergency unwind du reliquat via la leg 1 en sens inverse. Tout reliquat encore impossible à liquider est valorisé à zéro (hypothèse volontairement conservatrice).
- Nouvelle table `paper_execution_attempts` contenant aussi les forced unwinds et pertes; `paper_trades` reste la table legacy des 2-leg complètes.
- Anti-répétition économique: après une entrée, route désarmée; réarmement seulement après spread <=0% pendant 1000 ms continus + cooldown dur 5000 ms.
- Capital réservé à T0; profits/pertes restent dans les balances; rebalances et coûts conservés.
- WS latency brute conservée uniquement en diagnostic et échantillonnée toutes les 10 s/symbole pour éviter une DB gigantesque.

## Paramètres par défaut importants
- Capital Paper: 2000 USD, partagé entre USDT/USDC/USD1.
- Fee: 0.05% taker par leg.
- Signal min net: +0.01%.
- Paper local BBO age <=150 ms, local skew <=50 ms.
- 599/600 symboles max; 30 subscriptions max par WS.
- REST requis. Si les snapshots REST échouent, les carnets ne deviennent pas prêts et le Paper Bot ne doit pas trader.

## Avant démarrage sur VPS Asie
1. Vérifier `curl -4 -I https://api.mexc.com` et un GET `/api/v3/depth` => HTTP 200.
2. Vérifier NTP/chrony, mais ne pas utiliser le timestamp MEXC brut comme filtre de trading.
3. Lancer avec une DB V2.3 neuve.
4. Contrôler `depth_ready == symbols` avant d'interpréter les résultats.
5. Surveiller WS reconnects, queue/backlog, BBO local age p50/p95 et CPU/RAM.

Cette version reste PAPER/RESEARCH. Elle ne contient aucune fonction d'envoi d'ordre réel.
