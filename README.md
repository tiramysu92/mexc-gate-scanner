# MEXC Spot 2-Leg Scanner V2.2 Depth

Deux vues en parallèle :

1. **RAW / research** — sans capital initial. Le flux marché provient du carnet local MEXC maintenu par `aggre.depth @10ms`; timestamps MEXC/VPS, âge/skew, traces T0→300 ms et matrice de latence restent enregistrés. Le PnL brut sert de référence de capacité du signal.
2. **Paper bot 2 000 $** — 2 000 $ répartis au départ entre USDT/USDC/USD1. Capital réservé à T0, une entrée max par événement, profits/pertes réinjectés dans les balances, rebalance dynamique. L'exécution est désormais **séquentielle et multi-niveaux** : le leg 1 marche le carnet vers la moitié de la latence totale, puis le leg 2 marche le carnet à la latence totale avec exactement l'actif intermédiaire obtenu.

## Carnet local
- WebSocket MEXC `spot@public.aggre.depth.v3.api.pb@10ms@SYMBOL`
- snapshot REST `/api/v3/depth?limit=100`
- suivi des versions; resynchronisation si un gap est détecté
- BBO dérivé du carnet local, donc un seul stream/symbol
- dashboard : nombre de carnets `depth prêts`

Défauts paper : âge échange <=150 ms, skew <=50 ms, latence totale 150 ms (leg 1 ~75 ms, leg 2 ~150 ms), frais taker 0,05 %/leg, minimum 10 $.

Base : `mexc_routes_v22.db` — dashboard : port 8081.

Cette version reste un simulateur : elle n'envoie aucun ordre réel. Les vrais fills/ACK/API seront mesurés plus tard en shadow/live micro-capital.
