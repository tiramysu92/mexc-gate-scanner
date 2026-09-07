# MEXC Spot 2-leg Scanner V2.2 TopBook (nuit OVH)

Version temporaire sans snapshot REST, conçue pour fonctionner malgré le blocage REST MEXC sur le VPS OVH.

- Spot 2-leg uniquement.
- WebSocket MEXC BookTicker 10 ms.
- **Niveau 1 du carnet uniquement** : best bid/ask + quantité disponible au meilleur prix.
- Aucun snapshot REST et aucune profondeur multi-niveaux inventée.
- Timestamps MEXC + réception VPS, âge BBO, skew, trajectoires jusqu'à +300 ms.
- Recherche brute sans capital + matrice âge/latence.
- Paper bot 2 000 USD : 666.67 par USDT/USDC/USD1, capital réservé à T0, profits/pertes composés, rebalances.
- Paper bot séquentiel : leg 1 vers +75 ms, leg 2 vers +150 ms, chacun limité à la quantité réellement visible au **premier niveau BBO** à cet instant.
- Si le niveau 1 du leg 2 ne peut pas absorber toute la quantité obtenue au leg 1, l'exécution est classée comme non résolue/risque ; aucune profondeur cachée n'est supposée.

Cette version sert à collecter cette nuit. La V2.2 Depth multi-niveaux restera la cible pour le nouveau VPS où REST MEXC est accessible.
