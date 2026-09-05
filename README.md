# MEXC Spot Routes Scanner V1.0

Scanner d'observation **spot uniquement** pour MEXC. Il tourne indépendamment du scanner MEXC↔Gate V3.4, par défaut sur le port **8081**, et écrit dans `mexc_routes.db`.

## Ce qu'il scanne

- routes 2 legs : `stable 1 -> crypto A -> stable 2` ;
- routes 3 legs : `stable 1 -> crypto A -> crypto B -> stable 2` ;
- les sens inverses sont construits automatiquement quand les marchés existent ;
- stables par défaut : USDT, USDC, USD1 ;
- tailles : 250 / 500 / 1000 / 2000 USD-equivalent ;
- frais conservateurs : 0,05 % taker **par leg** ;
- prix utilisés : meilleur bid/ask WebSocket MEXC, avec contrôle de la quantité disponible au top-of-book.

La liste des cryptos n'est pas figée. Le scanner découvre les marchés MEXC, garde tous les marchés reliés aux stablecoins, puis choisit dynamiquement les actifs les plus connectés pour les routes 3 legs. La limite de symboles WebSocket est configurable.

## Journée fixe

Le dashboard compte les opportunités de **00:00 à maintenant en Europe/Paris**. À minuit les compteurs du dashboard repartent à zéro, mais la base SQLite n'est jamais effacée.

## Simulation 24 h / journée fixe

Le dashboard rejoue les opportunités de la journée avec un capital paper par défaut de 2 000 $ réparti entre les stablecoins. Une route `USDT -> A -> USDC` débite le bucket USDT et crédite le bucket USDC. Si les opportunités ne viennent que dans ce sens, l'USDT finit par manquer et la simulation arrête naturellement de prendre ces trades. Un sens inverse ultérieur reconstitue le bucket USDT.

Cette simulation évite donc l'hypothèse irréaliste d'un capital infini dans chaque stablecoin. Elle reste indicative : aucune exécution réelle, latence d'ordre, rejet d'ordre ou variation entre les legs n'est simulée.

## Installation

Dans un nouveau dossier sur le VPS :

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Dashboard : `http://IP_DU_VPS:8081`

Le scanner MEXC↔Gate peut continuer à tourner sur le port 8080.

## Variables utiles

```bash
PORT=8081
TAKER_FEE=0.0005
STABLES=USDT,USDC,USD1
SIZES=250,500,1000,2000
SIM_CAPITAL=2000
MIN_NET_PCT=0.01
MAX_WS_SYMBOLS=600
MAX_BRIDGE_ASSETS=80
```

## Découverte des marchés et WAF

Le scanner tente d'abord `/api/v3/exchangeInfo`. Si le WAF MEXC renvoie 403, il tente l'ancien endpoint public MEXC v2 pour récupérer la liste des symboles. Une liste réussie est mise en cache dans `mexc_markets_cache.json` pour les démarrages suivants.

## Limites V1

- observation seulement, aucune clé API et aucun ordre ;
- BBO/top-of-book uniquement : une taille est rejetée si le meilleur niveau n'a pas assez de quantité ;
- le rendement paper est une simulation, pas un rendement réalisable garanti ;
- le risque d'exécution séquentielle (leg 1 exécuté mais leg 2/3 dégradé) devra être modélisé avant tout bot live ;
- les stablecoins sont valorisés contre USDT lorsqu'une paire directe suivie existe, sinon le scanner utilise temporairement l'hypothèse de parité 1:1.
