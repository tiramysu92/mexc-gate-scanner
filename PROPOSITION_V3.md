# Proposition V3 — préparation du 15 septembre 2026

**La V3 est une base modulaire exécutable, livrée en `3.0.0-candidate.1`, avec observation publique, simulations et adaptateur LIVE.** Le fonctionnement réel sur le serveur et les remplissages MEXC ne sont pas encore validés par cette livraison.

L’objectif est de rendre chaque décision, refus, exécution et perte explicable, puis d’optimiser sur des mesures comparables. Il ne suffit pas d’augmenter le nombre de signaux A : il faut connaître la taille réalisable, le délai, le résultat des deux ordres et le coût d’une sortie dégradée.

## Ce qui est proposé et écrit

| Sujet | Choix V3 |
|---|---|
| Organisation | Modules distincts pour carnets, transport, décision, exécution, journal, simulations et interface ; `app.py` devient un point d’entrée court. |
| Parcours des deux jambes | Plan dimensionné avant l’achat ; confirmation de L1 ; comparaison de L2 et du retour sur le marché d’achat, à quantité vendable identique. |
| Sortie après achat | Le seuil positif d’une nouvelle entrée ne s’applique plus à cette comparaison. Une sortie légèrement négative peut être préférable à un retour encore plus défavorable. |
| Prix et quantité | Ordres FOK avec prix limite et quantités arrondies ; le budget inclut une réserve de financement. Cette réserve n’est pas comptée une deuxième fois comme un frais. |
| Délais | Réutilisation du calcul lorsque carnets, valorisations, paramètres et ordre sont identiques ; temps écoulé, état du flux et financement restent vérifiés. |
| Confirmation | Une notification privée terminale vérifiée peut être utilisée avant le retour HTTP. Le polling REST se fait dans un autre worker et ne bloque pas cette notification. |
| Frais | Les événements privés de transactions peuvent apporter les frais exacts si leurs totaux correspondent à l’ordre. Sinon, réserve conservatrice puis rapprochement après la première sortie. |
| Écritures | Intention initiale durable avant tout POST ; confirmation et intention suivante dans une transaction SQLite unique. |
| Incertitude | Aucun nouvel ordre automatique si le résultat du précédent est inconnu ou contradictoire. Requête par identifiant enregistré, sans répétition du POST. |
| Flux | Parseur Protobuf natif, file bornée par connexion, traitement équitable, verrous par symbole et reconstruction protégée par génération. PING/PONG JSON MEXC conservé. |
| Restrictivité | Contrôles sur les marchés de la route ; un carnet étranger indisponible ne bloque pas cette route. Compteurs séparés pour signal brut, classe à la taille choisie et admission. |
| Continuité | Journaux V3 séparés ; import explicite et en lecture seule de l’historique V2 ; quota et arrêt persistants, indépendants d’une session de terminal. |

## Le parcours d’un trade

1. Le scanner observe un écart brut en tenant compte du cours USDT des devises de départ et d’arrivée. USD1 et USDC ne sont pas supposés valoir exactement un USDT.
2. Le moteur calcule une quantité abordable, son prix d’achat limite, la sortie correspondante et les frais. Il conserve la classe et tous les motifs de refus.
3. Avant un achat réel, les règles, les formes d’ordres et les frais doivent disposer d’une prévalidation récente ; le compte, le quota et les flux nécessaires doivent être utilisables.
4. Le journal enregistre l’intention. Le contrôle final vérifie le plan puis l’adaptateur signe la requête, après l’attente éventuelle de son canal HTTP.
5. Le moteur attend une confirmation terminale correspondant à l’identifiant client, au marché, au sens et à une quantité cohérente. Un timeout ne signifie jamais « aucun achat ».
6. À la quantité réellement acquise, diminuée des frais connus ou d’une réserve, il choisit la meilleure sortie autorisée entre les deux marchés. La confirmation et la nouvelle intention sont enregistrées ensemble.
7. Si la sortie est annulée ou partielle, une seconde sortie est possible uniquement après rapprochement terminal REST, vérification des frais consommés et du solde propre au trade. Au maximum deux ordres de sortie, avec une fenêtre de seconde tentative de 1,5 seconde.
8. Les frais exacts, flux de devises, quantités restantes, coûts résiduels et PnL sont rapprochés. Un résidu important, une incertitude ou le recours à un secours bloque les nouveaux achats.

La première candidate utilise **FOK pour l’achat et les sorties**, y compris le retour. Elle ne reproduit pas une vente MARKET inconditionnelle après l’échec CTO. Ce choix borne le prix demandé, mais peut augmenter les annulations si le carnet bouge. Son effet doit être mesuré ; un FOK ne garantit pas l’exécution.

Les preuves enregistrées comprennent les identités de carnets, les timestamps et jusqu’à 100 niveaux de chaque côté avant achat et sélection de sortie, les alternatives de sortie, les intentions, les réponses et les frais. Une troncature de profondeur est signalée. Ce relevé n’est pas une capture exhaustive de tous les messages du marché.

## Fraîcheur : une référence, deux comparaisons

Les trois politiques sont évaluées sur **les mêmes captures immuables de route et les mêmes timestamps**. Seule la référence commande les simulations exécutées et le mode réel par défaut.

| Politique | Entrée locale / MEXC | Sortie locale / MEXC | Projection statique |
|---|---:|---:|---|
| Référence | 50 / 100 ms | 100 / 150 ms | Budget 85 ms conservé |
| Sans projection | 50 / 100 ms | 100 / 150 ms | Désactivée pour la comparaison |
| Fenêtre 100/200 | 100 / 200 ms | 150 / 250 ms | Désactivée pour la comparaison |

Le skew MEXC maximal à l’entrée reste de 50 ms. La projection statique de 85 ms peut effectivement réduire la marge locale de sortie à environ 15 ms : **son coût est maintenant visible**. La candidate ne la supprime pas silencieusement en production. Une comparaison d’admission ne suffit pas à qualifier la rentabilité de la politique élargie.

Un dernier changement de prix ancien peut correspondre à un marché calme, mais un PONG ne prouve pas que toutes les mises à jour de ce carnet ont été reçues. L’âge MEXC, la continuité des versions, l’attente locale et l’état de transport restent distincts. Les exemples de flux retardés de plusieurs minutes restent refusés.

## Nouvelles simulations

Les anciennes configurations 15/30, 25/50 et 50/100 ms ne sont pas reprises comme promesses de vitesse.

| Profil | Délai utilisé |
|---|---|
| `observed_proxy` | Tirages parmi les notifications d’achat 69, 71, 58 et 69 ms ; de vente 48, 52, 74 et 52 ms |
| `stress_x1_5` | Mêmes échantillons multipliés par 1,5 |
| `stress_x2` | Mêmes échantillons multipliés par 2 |

Ce sont **quatre observations par sens**, issues de SAGA, SKL, 牛来 et CTO. Trois achats étaient MARKET et un FOK ; les quatre ventes étaient MARKET, dont une annulation CTO. Il n’existe pas encore de distribution empirique fiable du délai des ventes FOK.

Le carnet disponible au délai de notification sert de proxy de matching. L’heure exacte de matching n’est pas déduite d’un simple `order.time`. Les scénarios ne modélisent ni la priorité dans la file de l’exchange, ni sa liquidité cachée, ni la probabilité exacte d’une annulation. Ils utilisent le même moteur et les mêmes règles d’inventaire que le LIVE ; ils ne constituent pas des preuves de remplissage.

Les frais sont par défaut considérés inconnus à la confirmation dans les simulations. Chaque portefeuille démarre avec les derniers montants communiqués : 86,76158227044 USDT, 100 USD1 et 0,0486382 USDC. C’est une **configuration de simulation**, pas une interrogation de ton solde actuel. Chaque profil applique le plafond de 20 USDT, le quota de 10 achats et les règles d’arrêt. Son journal persiste ; les diagnostics d’admission continuent même si le profil a atteint son quota.

## Mesurer les refus sans faire disparaître les opportunités

Le tableau conserve les épisodes bruts positifs au meilleur prix, puis le classement à la taille calculée et l’admission. Les refus peuvent avoir plusieurs causes ; leurs compteurs ne doivent donc pas être additionnés comme des catégories exclusives.

Les classes V3 sont explicites : A à partir de 0,30 % et B+ à partir de 0,15 % de marge estimée au prix limite de la taille choisie. **Ce ne sont pas les mêmes définitions que les anciennes classes Shadow de robustesse.** Elles servent au diagnostic et ne sont pas des probabilités de succès. Le nombre brut en amont reste affiché pour ne pas transformer un reclassement en amélioration artificielle du taux d’acceptation.

Les épisodes sont séparés par 500 ms sans signal positif observé ; les compteurs du scanner concernent le processus courant. Les journaux des trades sont, eux, persistants. Les comparaisons de politique utilisent une capacité théorique de 20 USDT, distincte du financement disponible dans chacun des portefeuilles.

## Paramètres conservés

- Plafond de financement par achat : 20 USDT ; taille minimale : 10 USDT.
- Quota réel : 10 achats, sans échéance horaire ; budget d’arrêt supplémentaire : 5 USDT ; premier secours = arrêt des entrées.
- CTO exclu des admissions ; ses données peuvent rester visibles pour l’analyse.
- Une seule tentative réelle à la fois ; aucune vente automatique d’un bag préexistant.
- Résidus inférieurs ou égaux à 0,05 USDT enregistrés comme poussière avec leur coût séparé. Ils ne deviennent pas du PnL réalisé.
- Arrêt et quotas indépendants de la page web et du surveillant.

Le seuil de 5 USDT est un seuil d’arrêt, **pas une perte maximale garantie** après un achat déjà exécuté. La candidate peut conserver une exposition si aucun prix de sortie protégé n’est réalisable ; elle ne prétend pas rendre les deux ordres atomiques.

## Ce qui est validé et ce qui reste à mesurer

La validation locale couvre les carnets reconstruits, les cinq anciens cas périmés, l’expiration entre jambes, les annulations, les remplissages partiels, les contradictions WS/REST, les frais privés incomplets ou répétés, la transaction atomique, les reprises de journal, la conservation du quota et le parcours complet du flux public factice vers les trois profils.

Le microbenchmark local a mesuré environ **43,6 µs** pour une évaluation complète et **17,2 µs** pour un contrôle réutilisant un plan inchangé, sur 2 000 itérations. Cela mesure du CPU local, sans réseau et sans commit ; cela ne prédit pas la latence du serveur.

Restent à mesurer sur le serveur : débit soutenu à l’univers complet, délais de reconstruction, stabilité des PING/PONG, motifs et taux d’admission, frais effectivement reçus par le flux privé et résultat des ordres FOK. Le taux de remplissage LIVE et la rentabilité de V3 sont inconnus à ce stade.

Le prochain livrable d’exploitation est le rapport `surveillance_v3_…json.gz` obtenu en observation, avec ses compteurs de refus. Il permettra de décider de l’évolution des paramètres sur des résultats communs, au lieu d’ajouter une version pour chaque incident.

## Sources et continuité

- Base de code figée : V2.4.14, SHA-256 `47f953381159977b4ccf2a09eee3e03eadcbf45e4046a7ae0b0ba42ba9d5a102`.
- Historique communiqué : trois pertes V2.4.6, puis liquidation manuelle CTO ; total réalisé de référence **−13,144151092630048 USDT**. L’import réel lit la base source et n’invente pas ce solde.
- Les règles d’API et le caractère non exécutant de `/order/test` sont décrits dans la [documentation officielle MEXC Spot](https://mexcdevelop.github.io/apidocs/spot_v3_en/).
- Les descripteurs Protobuf ont été confrontés aux [schémas officiels MEXC](https://github.com/mexcdevelop/websocket-proto), notamment `PushDataV3ApiWrapper`, `PublicAggreDepthsV3Api`, `PrivateOrdersV3Api` et `PrivateDealsV3Api`.
