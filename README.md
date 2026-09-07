# MEXC Spot 2-leg Scanner V2.3.1 Depth Strict

Version Paper/Research uniquement. Aucun envoi d'ordre réel.

## Objectif

Scanner les arbitrages spot 2-leg entre USDT / USDC / USD1 sur MEXC avec un carnet local multi-niveaux construit depuis :

- snapshot REST `/api/v3/depth?limit=100`
- deltas WebSocket `spot@public.aggre.depth.v3.api.pb@10ms@SYMBOL`
- suivi de version et resynchronisation si une séquence manque.

## Hypothèses Paper principales

- capital initial : 2 000 USD, réparti entre USDT / USDC / USD1
- frais taker : 0,05 % par jambe
- décision immédiate à T0 si edge net >= 0,01 %
- admission Paper : âge local <= 150 ms, skew local <= 50 ms
- leg 1 vers +75 ms, leg 2 vers +150 ms
- capital réservé à T0
- sizing initial volontairement conservateur : capacité BBO à T0
- exécution réelle simulée ensuite en parcourant le carnet multi-niveaux
- profits et pertes restent dans les balances et composent le capital

## Anti-répétition économique

Une route exécutée est désarmée. Elle ne peut être réarmée que si :

- le net revient à <= 0 % pendant 1 seconde continue ;
- et au moins 5 secondes se sont écoulées depuis la dernière entrée.

Une micro-coupure d'un spread persistant ne constitue donc pas une nouvelle opportunité Paper.

## Leg 1 irréversible / reliquat

Dès que la leg 1 est exécutée, le résultat ne peut plus disparaître des statistiques.

Si la leg 2 est partielle :

1. le moteur tente immédiatement un unwind via la première paire en sens inverse, en marchant le carnet multi-niveaux ;
2. si un reliquat subsiste faute de profondeur fraîche suffisante, il devient une `open_exposure` persistante ;
3. cette exposition reste dans la NAV à une valeur de marché non nulle ;
4. un worker retente sa liquidation toutes les 100 ms dès qu'un carnet Depth frais le permet ;
5. à fermeture, `paper_execution_attempts` est mis à jour avec le résultat final (`forced_unwind_later`).

Aucun reliquat n'est artificiellement valorisé à zéro.

## Base SQLite

Base par défaut : `mexc_routes_v23.db`.

Tables importantes :

- `opportunities`
- `decisions_v22` / `decision_trace_v22` (noms conservés pour compatibilité historique)
- `execution_trials`
- `paper_execution_attempts` — table autoritaire pour toutes les legs 1 engagées
- `paper_open_exposures`
- `paper_trades` — seulement les routes 2-leg entièrement complétées directement
- `paper_rebalances`
- `paper_state`

Le flux de latence brute MEXC->VPS reste diagnostique ; il n'est pas utilisé pour l'admission des trades car les horloges ne sont pas directement comparables sans calibration.
