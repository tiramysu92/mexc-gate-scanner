# MEXC Spot Routes Scanner V2.0

Scanner d'observation **spot uniquement** pour routes MEXC 2-leg et 3-leg, avec simulation paper réaliste sur journée fixe Europe/Paris.

## Changements majeurs vs V1.1

- **Correction des 74 routes** : la V1.1 sélectionnait d'abord jusqu'à 600 marchés liés aux stablecoins puis ajoutait les marchés A/B seulement s'il restait de la place. Le quota était donc presque entièrement consommé avant les cross-pairs nécessaires aux routes `stable → A → B → stable`. V2 construit d'abord **toutes les routes candidates** à partir du graphe complet MEXC, puis choisit des **routes complètes** sous le budget de 600 symboles. Le dashboard affiche candidats/sélectionnés 2-leg et 3-leg pour auditer le résultat.
- **Taille dynamique** : montant maximal exécutable au meilleur bid/ask sur tous les legs, minimum 10 USD, pas de paliers fixes.
- **Frais** : 0,05 % taker par leg par défaut.
- **Journée fixe** : compteurs 00:00 → maintenant, Europe/Paris. La base garde l'historique.
- **Durée / flashs** : les opportunités 0 ms/1 tick sont enregistrées mais ne sont **pas créditées** à la simulation.
- **Simulation d'exécution** : une opportunité doit survivre au moins 100 ms et 2 ticks (configurable) avant d'être paper-tradée.
- **Rééquilibrage intelligent** : stablecoin→stablecoin via carnet réel, seulement si le coût est financé par les profits déjà réalisés depuis le dernier rééquilibrage et si l'opportunité actuelle justifie économiquement ce coût.
- **Profit-bank** : après un rééquilibrage, le compteur de profit disponible pour financer le prochain repart à zéro.
- **Marks stablecoin corrigés** : USDC/USD1 sont valorisés au mid contre USDT **sans frais fictifs**. Les frais ne sont appliqués qu'aux vrais trades.
- **Données de recherche** : snapshots des meilleures routes proches du seuil et quotes stablecoin périodiques, pour permettre de meilleurs backtests ultérieurs.

## Base

Par défaut : `mexc_routes_v2.db` (séparée de V1.1 pour conserver une baseline propre).

Tables principales :
- `opportunities`
- `paper_trades`
- `paper_rebalances`
- `paper_state`
- `route_snapshots`
- `stable_quotes`
- `meta`

## Variables utiles

- `PORT=8081`
- `TAKER_FEE=0.0005`
- `STABLES=USDT,USDC,USD1`
- `MIN_EXEC_USD=10`
- `MAX_WS_SYMBOLS=600`
- `SIM_CAPITAL=2000`
- `SIM_EXEC_LATENCY_MS=100`
- `SIM_MIN_CONFIRM_TICKS=2`
- `REBALANCE_MAX_PROFIT_SHARE=0.80`
- `REBALANCE_EDGE_MULT=1.25`

## Lancement

```bash
pip install -r requirements.txt
python app.py
```

Dashboard : `http://<IP_VPS>:8081`

## Interprétation du rendement journalier

Le rendement principal du dashboard n'est plus une somme théorique des spreads. Il reflète le **paper wallet** :
- soldes réellement disponibles par stablecoin ;
- opportunités confirmées seulement après la latence configurée ;
- tailles BBO réellement disponibles ;
- frais taker par leg ;
- rééquilibrages et leur coût ;
- impossibilité de rééquilibrer si le profit passé ne finance pas le coût.

Cela reste une simulation : un vrai bot aura encore du risque de latence, de fill partiel et de changement du carnet entre les legs.
