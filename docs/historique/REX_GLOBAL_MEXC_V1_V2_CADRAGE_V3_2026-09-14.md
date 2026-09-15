# MEXC — REX global V1–V2 et cadrage de la V3

Date de consolidation : 14 septembre 2026. Périmètre : recherche d’arbitrage puis scanner et micro-bot spot à deux jambes. Document de référence initial, à compléter avec les sources manquantes ; pas une nouvelle livraison du bot.

## 1. Décision de départ et conclusion

**Nous repartons sur une V3 en conservant les acquis et les preuves de la V1/V2. Aucune V2.4.15 n’est proposée.** L’utilisateur prévoit une nouvelle branche GitHub. Ce bilan ne crée pas cette branche, ne modifie pas le serveur et ne réarme pas le LIVE.

Le problème global n’est pas seulement un flux WebSocket lent ou une mauvaise seconde jambe. Le projet a progressivement réuni recherche, simulation, données temps réel, ordres réels, comptabilité et exploitation, sans disposer d’une référence commune suffisamment stable pour comparer les résultats et décider d’une mise en production.

Nous avons appris beaucoup de choses utiles. Mais les preuves étaient dispersées : rapports de replay, versions de code, captures, journaux, scripts de réparation et échanges. Une correction locale pouvait être validée, puis devenir implicitement une validation du système entier. **Cette extrapolation n’était pas justifiée.** Les tests réussis validaient des scénarios précis ; ils ne prouvaient ni la stabilité complète du serveur, ni l’exécution des sorties sur MEXC, ni un avantage économique durable.

Trois conclusions dominent :

1. **La rentabilité réelle n’est pas démontrée.** Les anciens résultats simulés dépendent fortement des hypothèses de capture, de liquidité, de taille et de frais. Les pertes réelles retrouvées doivent rester visibles.
2. **Les données et protections ont progressé.** Fraîcheur MEXC, reconstruction versionnée, files bornées, rattrapage, confirmation des ordres et comptabilité des résidus sont des acquis à préserver. Les supprimer avec une réécriture recréerait les anciens risques.
3. **La V3 doit changer la méthode de validation autant que l’organisation du code.** Une même décision économique, des preuves rejouables, des états d’exécution explicites et des critères de passage entre recherche, simulation et LIVE doivent remplacer l’enchaînement de correctifs.

## 2. Avons-nous tous les historiques depuis la V1 ?

**Non. Je ne dispose pas d’une transcription intégrale de toutes les discussions depuis la V1.** La recherche de conversations n’a pas retrouvé un ensemble complet. Elle a surtout renvoyé le dossier de reprise et des fichiers. Il serait trompeur de présenter ce bilan comme une récupération exhaustive des échanges.

En revanche, l’historique GitHub accessible de `mexc-spot-routes-v23` remonte au 1er septembre 2026. Les 68 commits retournés vont de `b61f6927` à `c8f4be68`. Ils permettent de retrouver des états anciens, mais les messages « Add files via upload » n’expliquent généralement pas les décisions. Une histoire Git n’est pas une histoire complète du raisonnement.

Ce bilan distingue quatre niveaux :

| Niveau | Ce que cela signifie | Exemples |
|---|---|---|
| Preuve directement relue | Code, journal, rapport ou capture réellement consulté | README anciens sur GitHub ; pertes V2.4.6 ; bilan V2.4.13 ; fonction de surveillance V2.4.14 |
| Résultat historique rapporté | Résultat conservé dans une analyse antérieure ; calcul non intégralement refait ici | Replays du 10 septembre ; benchmarks et suites de tests précédemment livrés |
| Déclaration utilisateur / sortie fournie | État ou action rapportés dans la conversation | Vente manuelle CTO ; déploiement V2.4.14 ; solde du sous-compte |
| Proposition V3 | Choix de conception ou critère futur, pas une capacité déjà livrée | Journal unique des décisions, séparation observation/contrôle, parcours de qualification |

### Couverture et lacunes

| Période | Couverture actuelle | Ce qui reste incomplet |
|---|---|---|
| Origine RIO / MetaMask, fin août | Contexte conservé dans le dossier de reprise | Conversations intégrales, toutes les hypothèses et opérations initiales |
| MEXC–Gate, début septembre | Historique Git ; README initial et Universe V3.4 relus | Réaudit de tous les états et de tous les résultats inter-plateformes |
| Routes spot V1, puis V1.1–V2.2 | Code V1 retrouvé ; transitions décrites dans le dossier de reprise | Arbitrages détaillés de chaque version intermédiaire |
| V2.3–V2.3.5 | README historiques et deux rapports de simulations relus | Recalcul intégral des anciens exports ; analyse finale du nouvel export de 741 Mo |
| V2.3.6–V2.4.5 | Sources et inventaire dans le dossier, historique Git | Toutes les transitions Depth Quality et études intermédiaires |
| V2.4.6–V2.4.14 | Journaux de pertes, diagnostics, sources locales, sorties de tests et de déploiement | Cause MEXC exacte de l’annulation du secours CTO ; cause précise du dernier STOP ; réconciliation exhaustive du compte avec tous les mouvements externes |

Le manque de certaines conversations ne bloque pas cette consolidation. Il interdit seulement d’inventer ce qui s’y serait décidé. Les faits retrouvés ultérieurement seront ajoutés avec leur source et les conclusions qu’ils changent.

## 3. Le fil directeur à conserver

### Objectif économique

L’objectif est d’augmenter le patrimoine en stablecoins après frais et coûts de fonctionnement de la stratégie, en limitant le temps passé exposé au token intermédiaire. Un grand nombre de signaux, un classement A ou un PnL Shadow vert ne constitue pas cet objectif.

Une route `USDT → token → USD1` termine dans un autre stablecoin. Sa performance dépend aussi de la valorisation du stablecoin de sortie, de la disponibilité des soldes et du coût éventuel de leur rééquilibrage. Un stock croissant d’USD1 peut diminuer les prochaines possibilités d’achat en USDT sans perte comptable immédiate.

### Contraintes et préférences établies

- Spot uniquement, sans futures ni perpétuels. Contrainte halal exprimée par l’utilisateur ; cela ne vaut pas validation religieuse de chaque actif.
- Deux jambes sur MEXC comme périmètre central. Le scanner trois jambes reste séparé.
- USDT, USDC et USD1 restent dans le périmètre à évaluer. Ne pas retirer USD1 sur la base d’une ancienne interrogation.
- Préserver les actifs préexistants et distinguer le capital dédié du reste du compte.
- Comptabiliser toutes les premières jambes exécutées, les frais, secours et résidus ; ne jamais effacer une perte avec un changement de jour, de version ou de quota.
- Mesurer les opportunités par événement, sens et token, en évitant de compter un spread persistant comme une succession de gains indépendants.
- Ne pas réintroduire une attente artificielle fixe de 100 ms. Une attente arbitraire, une latence observée et une limite de fraîcheur sont trois grandeurs différentes.
- Rééquilibrage réel désactivé dans les versions examinées. Toute réintroduction devra être une décision séparée, avec coûts et effets sur le portefeuille.
- Interface compacte, mais motifs de refus, états d’exécution et qualité des preuves accessibles.

### Des montants qui ne doivent plus être confondus

| Montant | Nature |
|---|---|
| 250 / 500 / 1 000 / 2 000 USDT | Tailles de recherche historiques, notamment MEXC–Gate |
| 2 000 USDT | Capital Paper de certaines premières versions routes |
| 1 000 USDT et plafond 350 USDT | Portefeuille de recherche Shadow ultérieur |
| 200 USDT | Capital LIVE configuré ; pas une lecture du solde disponible |
| 20 USDT | Plafond du micro-LIVE récent |
| Environ 186,80 USDT | Équivalent de patrimoine déclaré dans le sous-compte après les incidents et mouvements |
| 5 USDT | Seuil de perte supplémentaire du quota récent ; pas une perte maximale garantie |

## 4. Chronologie des apprentissages

Les dates Git ci-dessous sont en UTC. Une date de commit ne prouve pas la date d’activation sur le serveur.

| Étape | Ce qui a été recherché ou changé | Ce qu’il faut en retenir |
|---|---|---|
| Origine, fin août | RIO entre MEXC et MetaMask, puis MEXC–Gate | Les transferts et inventaires font partie de la faisabilité ; une différence de prix ne suffit pas |
| 1er septembre, scanner initial MEXC–Gate | Lecture seule SOL/SUI, meilleur bid/ask, polling configuré à 1 s | Le README initial signalait déjà les manques : profondeur à la taille, latence, non-exécution, inventaire |
| Universe V3–V3.4, début septembre | Univers jusqu’à 150 paires, vérification multi-tailles, regroupement en événements, passage WS et cache de découverte | Mesurer le cycle réel ; les cibles de fréquence ne sont pas des performances observées. Le README documente les ~40 s de mesures séquentielles de la V2 |
| Routes spot V1, 5 septembre | Code `VERSION=1.0`, USDT/USDC/USD1, tailles 250–2 000, capital simulé 2 000 | Nouvelle famille de produit, distincte de l’ancienne Universe V3 |
| V1.1–V2.2, 5–7 septembre | Évolution des routes, séparation deux/trois jambes, suppression d’attente artificielle | Transitions partiellement documentées : ne pas leur attribuer un résultat chiffré non retrouvé |
| V2.3 / V2.3.1, 7 septembre | Carnets REST + deltas versionnés, réservations, jambes séquentielles, secours, anti-répétition | Après un achat, l’exposition est irréversible. V2.3.1 remplace la valorisation forcée à zéro des résidus par un suivi persistant |
| V2.3.2–V2.3.5, 8–10 septembre | Depth, Shadow, replay, PnL signé, horizon de capture étendu | Première preuve nette que la rentabilité est très sensible au modèle de remplissage et de capture |
| V2.3.6 et suivantes | Classement Depth Quality, tests de liquidité dégradée | Une note de qualité doit rester liée à sa taille, ses données et ses hypothèses |
| V2.4.6, 13 septembre | Trois opérations réelles perdantes, dont deux secours | Le journal réel contredit toute assimilation du Shadow à une rentabilité acquise |
| V2.4.7 | Réévaluation à la taille abordable et contrôles de fraîcheur | Cinq signaux A périmés sont visibles en recherche sans admission LIVE correspondante retrouvée |
| V2.4.8 | Parseur Protobuf natif, réception séparée, file bornée, interface compacte | Accélérer un composant ne garantit pas le débit du système. Le déclenchement trop rapide des récupérations produit une boucle de reconstruction |
| V2.4.9 | Rattrapage local avec blocage des entrées plutôt que reconnexion immédiate | La distinction retard récupérable / flux irrécupérable est nécessaire ; les attentes de dizaines de secondes restent problématiques |
| V2.4.10 | Répartition plus équitable du traitement, travail par lots | Amélioration de traitement ; incident CTO malgré un signal initial frais |
| V2.4.11 | Marge temporelle avant achat, secours borné et confirmé, régularisation CTO | Protection de l’exposition et comptabilité progressent ; l’annulation MEXC initiale n’est pas expliquée |
| V2.4.12 | Session limitée, budget supplémentaire, exclusion CTO | Le risque de session doit s’ajouter à l’historique, sans le réinitialiser |
| V2.4.13 | Générations de demandes de resynchronisation et diagnostics de fermeture | Correction d’une course pouvant laisser un carnet non prêt sans reconstruction programmée |
| V2.4.14 | Quota persistant de dix achats, sans échéance horaire | L’absence d’échéance ne rend pas le système indépendant du terminal : la fin du surveillant écrit encore STOP |

**Attention aux noms :** Universe V3 et un scanner trois jambes V3.0 existaient déjà. La future branche doit indiquer clairement qu’il s’agit de la refonte du produit spot deux jambes, par exemple `mexc-spot-2leg-v3`.

## 5. Ce que les simulations nous ont réellement appris

Les résultats suivants proviennent des rapports du 10 septembre. Ils n’ont pas été intégralement recalculés pendant ce bilan.

### Jeu historique et sensibilité

Le replay V2.3.4 portait sur 840 décisions, 26 880 captures de jambes Depth et 23 553 captures de paires stablecoins, sur environ 21 h 36 du 9 septembre. Les 280 passages comptabilisés n’étaient pas 280 stratégies indépendantes.

| Expérience historique | Résultat rapporté | Interprétation correcte |
|---|---:|---|
| Ancien Shadow FAST / TARGET / DEGRADED | +23,19 / −57,62 / −86,04 USDT avant rééquilibrages | Comptabilité et resets quotidiens différents des replays continus ; ne pas comparer directement |
| TARGET, capital 1 000, scénario robuste sélectionné | +30,28 USDT | Résultat rétrospectif dépendant de ses hypothèses |
| Variante utilisant la capture suivante, remplissage 100 % | −27,15 USDT, une exposition non résolue | Le signe du résultat change avec la représentation de l’exécution |
| Variante capture précédente, remplissage 50 % | +12,45 USDT | Forte dépendance à la liquidité accessible |
| Référence V2.3.5, capital 1 000, FAST / TARGET / DEGRADED | +4,72 / +5,08 / +1,71 USDT | Candidat plus prudent, toujours simulé |
| Contrôle de fin de période V2.3.5, mêmes profils | +1,68 / +1,55 / +0,51 USDT | Contrôle limité ; pas une preuve sur des marchés ou journées indépendants |

La capture suivante est un stress de discrétisation, pas la vérité du moteur d’appariement. L’exposition marquée à zéro dans un scénario de stress est une convention conservatrice, pas une perte réalisée observée. Mais l’inversion du signe économique suffit à interdire la conclusion « le bot est rentable » à partir du scénario favorable seul.

La référence V2.3.5 utilisait notamment 25 % des quantités, un cap de 200 USDT, un spread maximal de 1 %, une perte de secours estimée au plus à 2 % et un edge net prudent minimal de 0,10 %. Ces paramètres ne sont pas automatiquement ceux à reprendre dans une V3 à 20 USDT.

### Ce qui manquait au modèle

- L’impact des ordres et la liquidité réellement accessible au moment de l’appariement.
- Les règles exactes de quantité, précision et minimum d’ordre dans certains replays anciens.
- Une représentation complète des échecs, délais de confirmation et sorties au-delà de la fenêtre capturée.
- Des latences privées mesurées et distribuées, au lieu d’un seul calendrier théorique.
- Une validation indépendante des seuils choisis après observation du même jeu de données.

Les profils FAST 15/30 ms, TARGET 25/50 ms et DEGRADED 50/100 ms sont **des échéances cumulées depuis T0**, pas deux délais à additionner. Les anciennes 300 ms concernaient l’horizon de capture. Ces définitions doivent figurer dans les données et l’interface V3.

Les résultats Shadow +38,51 / +34,18 / +18,52 affichés sur la capture récente ne prouvent pas une performance de la session V2.4.14. Cette session indiquait 0 achat sur 10 et un PnL de session nul. Les portefeuilles et périodes ne sont pas les mêmes.

## 6. Bilan des opérations réelles retrouvées

### Registre des pertes

| Opération | Résultat | PnL réalisé de référence |
|---|---|---:|
| V2.4.6 — USDC>SAGA>USDT | Deux jambes achevées ; perte malgré statut completed | −0,339909820839992 |
| V2.4.6 — USDT>SKL>USDC | Secours ; carnet de sortie trop ancien | −0,033223976435001745 |
| V2.4.6 — USDT>牛来>USD1 | Secours ; edge de sortie devenu insuffisant | −0,0843313587150547 |
| V2.4.10 — USDT>CTO>USD1 | Achat, L2 non soumise, secours annulé, puis revente manuelle | −12,68668593664 |
| **Total historique consolidé** | **Valeur conservée dans la reprise V2.4.14** | **−13,144151092630048 USDT** |

Les trois premières valeurs sont celles du journal relu. CTO combine le coût d’achat enregistré et le produit de vente manuel fourni. Le total est un historique comptable du bot régularisé, pas une réconciliation indépendante de tous les mouvements du compte MEXC.

### CTO : chaîne causale établie

1. Achat exécuté de **19 801,66 CTO** à 0,001008 : coût brut 19,96007328 USDT, commission 0,00998003664 ; coût total **19,97005331664 USDT**.
2. Confirmation privée WS reçue 69 ms après le début du POST ; durée HTTP d’environ 53,35 ms.
3. La seconde jambe n’est **pas envoyée** : âge local 105 ms, au-delà de 100 ms ; âge MEXC 122 ms et skew 97 ms enregistrés. Ce n’est pas une annulation d’un ordre L2 par MEXC.
4. Le bot soumet un secours MARKET de 19 762,05 CTO sur CTOUSDT. Il revient **CANCELED, quantité exécutée nulle**. Le reliquat réel reste 19 801,66 CTO.
5. Le circuit bloque les nouvelles entrées, mais le stock reste exposé. L’utilisateur transfère ensuite le CTO au compte principal et le revend.
6. Vente fournie : 7,28701088 USDT bruts, frais 0,0036435 ; net **7,28336738 USDT**. Perte réalisée : **−12,68668593664 USDT**.

### Ce qu’il ne faut pas conclure à tort

- La baisse journalière de 51 % affichée n’est pas le rendement exact de cette opération. La perte se calcule avec ses achats, ventes et frais.
- Le prix 0,001144 présent dans l’événement terminal du secours ne prouve pas qu’un ordre limite à ce prix a été envoyé : la requête enregistrée est MARKET sans prix.
- Le carnet local de secours montrait assez de quantités sur les premiers niveaux, mais il précède l’appariement. Il ne démontre pas que ces quantités étaient accessibles à l’ordre.
- La cause précise de l’annulation demeure inconnue. Protection de prix, liquidité ou règle d’échange ne doivent pas être présentées comme une explication établie sans preuve supplémentaire.
- Le petit BBO de sortie donnait une capacité de seulement ~0,00283 USDT et un edge d’environ 5,07 %. Cependant, la qualité était bien recalculée à la taille de ~20 USDT, avec un edge normal d’environ 3,84 %. Cet incident ne démontre donc pas une application naïve du BBO à toute la taille.

**Enseignement :** une entrée fraîche et théoriquement profitable n’assure pas la disponibilité de la sortie après confirmation de l’achat. Un arrêt des nouvelles entrées n’élimine pas une exposition déjà acquise. La V3 doit traiter ces deux responsabilités séparément.

## 7. Registre consolidé des incidents et exigences associées

| ID | Problème / preuve | Cause établie ou limite | Exigence V3 |
|---|---|---|---|
| REX-01 | Cycles réels très supérieurs à la fréquence annoncée dans l’ancien scanner | Requêtes séquentielles décrites dans le README Universe | Mesurer toute la chaîne ; ne jamais remplacer une mesure par la cadence configurée |
| REX-02 | PnL corrigé avec `max(0, profit_bank - coût)` dans l’ancien modèle | Un compteur pouvait perdre son signe économique | Grand livre signé ; budget de rééquilibrage distinct du PnL |
| REX-03 | Résultats de replay changent de signe selon la capture | Sensibilité forte, pas origine unique de toutes les pertes | Stress de remplissage et de temps ; résultats hors sélection ; pas d’accès à des données futures |
| REX-04 | Messages récemment reçus, mais timestamps MEXC vieux de plusieurs minutes | Retard différencié entre flux ; emplacement exact de l’attente non toujours établi | Temps d’émission, réception et traitement distincts ; horloge calibrée avec incertitude |
| REX-05 | Signaux A périmés visibles en recherche | Recherche et admission LIVE appliquaient des filtres différents | Chaque résultat affiche mode, taille, fraîcheur, motif et version de politique |
| REX-06 | V2.4.8 : récupérations massives `receive_queue_age` | Réaction trop agressive au retard local ; effet sur la reconstruction observé | Bloquer l’admission pendant un rattrapage ; reprendre seulement après cohérence vérifiée ; borner mémoire et durée |
| REX-07 | V2.4.9 : attentes de 23–32 s ; WS et carnets instables | Capacité complète insuffisamment validée ; cause non réduite à un seul compteur | Test de charge avec réception, carnet, scan, stockage et interface actifs ensemble |
| REX-08 | File pleine : 1 024 messages, ~2 279 ms, dernier lag MEXC 7 ms | Engorgement local établi pour cet épisode | Répartition équitable, débit et temps de service par connexion ; traitement explicite du débordement |
| REX-09 | Carnets bloqués à 654/656 ; course de resynchronisation reproduite | Nouvelle demande perdue lors de la fin d’une reconstruction dans l’ancien code | Générations, rejet des travaux obsolètes, reconstruction toujours planifiée si nécessaire |
| REX-10 | Achat CTO puis L2 non soumise | Carnet devenu trop ancien après confirmation L1 | Budget de temps incluant confirmation et contrôle final avant achat ; évaluer le risque de sortie |
| REX-11 | Secours CTO annulé, zéro rempli | Annulation établie ; cause exchange inconnue | États d’ordre explicites ; réconciliation avant nouvelle vente ; pas de double vente sur état incertain |
| REX-12 | Stock CTO conservé après ouverture du circuit | Circuit d’entrée ne clôture pas l’exposition | Responsable d’exposition persistant, alerte et procédure d’intervention documentée |
| REX-13 | `protected_unknown_asset:SKL` bloque la reprise | Ancien inventaire hors suivi courant détecté | Registre d’inventaire propre au bot / préexistant / externe ; diagnostic explicite avant acquittement ciblé |
| REX-14 | Fixtures manquantes, puis tests échouant car scanner réel actif | Paquet incomplet ; test temporaire dépendant du répertoire de production | Installation depuis une copie propre ; tests isolés sans contourner le contrôle de production |
| REX-15 | Session sans échéance puis `stop_file` | Le surveillant V2.4.14 écrit STOP à sa fin ; déclencheur exact récent non retrouvé | Séparer observation et contrôle ; chaque arrêt conserve auteur, motif et événement déclencheur |
| REX-16 | Journée, quota, capital configuré et historique semblent contradictoires à l’écran | Agrégats de périmètres différents | Définitions visibles et rapprochement entre soldes, PnL historique, quotidien et quota |

Ces lignes ne signifient pas que tout est corrigé en production. Elles relient chaque apprentissage à une propriété que la V3 devra démontrer.

## 8. Les données de flux : acquis et limites

### Le changement de doctrine sur les timestamps

Le README V2.3 prescrivait une admission basée sur l’âge local, en écartant le temps brut MEXC→VPS faute d’horloges directement comparables. Cette précaution visait un vrai problème de mesure. Les incidents suivants ont montré la limite de la solution : un message ancien peut arriver maintenant et paraître localement frais.

Dans les pertes V2.4.6, les écarts réception–émission observés étaient d’environ 1,789 s sur SAGAUSDT, 126,548 s sur SKLUSDT et 50,405 s sur 牛来USDT, avec une autre jambe à 12–18 ms. Plus tard, certains flux dépassaient plusieurs minutes. Un décalage global d’horloge ne suffit pas à expliquer un très grand écart entre deux timestamps émis par la même source temporelle.

**La règle à conserver est double :** ne pas utiliser naïvement deux horloges non calibrées ; ne pas assimiler réception récente et donnée récente. Il faut représenter l’offset estimé, son incertitude, les timestamps par jambe et le temps passé dans chaque étage local.

### Bilan V2.4.13 relu

Fichier : `bilan_flux_v2413_20260914_183631_568489.json.gz`.

| Mesure | Valeur |
|---|---:|
| Durée observée | 1 195,029 s, pour 20 minutes demandées |
| Échantillons | 240 ; aucune erreur de collecte enregistrée |
| Carnets prêts min / max / fin | 2 / 656 / 656 |
| Échantillons avec tous les carnets prêts | 182 / 240 |
| Déconnexions / reconnexions pendant la fenêtre | 8 / 8 |
| Rattrapages commencés / terminés | 467 / 467 |
| Récupérations fortes | 8, motifs conservés `exchange_backlog` |
| Événements de trou de séquence / échecs de resync | 200 / 1 |
| Dernière séquence continue à 656/656 dans les échantillons | 470,008 s, soit environ 7 min 50 |

Ce relevé prouve une récupération vers l’ensemble des carnets et une période finale favorable. Il ne prouve pas l’absence de coupure entre deux échantillons, ni la stabilité sur plusieurs journées. Les compteurs de rattrapage et de reconnexion sont différents : un rattrapage terminé sans déconnexion n’est pas automatiquement un échec.

Pour la V3, une route est admissible selon ses propres deux carnets. Le nombre global de carnets prêts reste un indicateur opérationnel, mais `656/656` ne certifie ni leur fraîcheur à la décision ni la liquidité réellement exécutable. Inversement, un seul carnet indisponible ne justifie pas nécessairement de déclarer toutes les autres routes inutilisables : cette politique doit être explicite et testée.

## 9. Le dernier arrêt V2.4.14

La capture à 22:34 montre un scanner recevant des données, 27/27 WS, 656/656 carnets et des entrées bloquées par `stop_file`. Elle affiche zéro achat dans le quota récent. **Elle ne montre pas une nouvelle perte de trade dans ce quota.**

Le code de `reprendre_live_v2414.py` contient un `finally` dans `monitor()` qui crée STOP à la fin de la surveillance. Des interruptions ou une erreur de contrôle peuvent donc suspendre les entrées même sans échéance horaire. Les compteurs de quota peuvent être persistants tout en coexistant avec ce comportement.

Il manque le dernier rapport de surveillance, le contenu et l’horodatage du STOP, et la sortie du processus surveillant pour attribuer cet arrêt précis. On ne peut pas conclure « c’est le terminal » à partir de la seule capture.

Ce point révèle une incohérence d’usage : « sans échéance » a été compris comme fonctionnement autonome, alors que l’observateur participait encore à la politique d’arrêt. La V3 doit définir qui possède le contrôle, qui observe et ce qui se passe quand l’un disparaît.

## 10. Causes transversales : pourquoi les itérations ont fait perdre le fil

### A. Des niveaux de validation mélangés

Compiler, réussir des tests, reconstruire tous les carnets, observer une fenêtre stable, simuler un gain et réaliser un gain sont six faits distincts. Aucun ne remplace les suivants. Le nombre de tests est utile pour la non-régression, pas comme score de maturité économique.

### B. Des référentiels économiques différents

Capital, tailles, profils temporels, périodes, coûts et règles de remplissage changeaient entre recherche, replay, Shadow BOT et LIVE. Sans fiche d’expérience commune, les résultats semblaient se contredire ou progresser alors qu’ils ne mesuraient pas la même chose.

### C. Une architecture devenue difficile à raisonner

La réception, le calcul, les écritures, l’interface et la récupération interagissaient via files et verrous. Déplacer du travail pouvait soulager un étage mais reporter l’attente ailleurs. Les générations de reconstruction et la propriété des états devenaient aussi importantes que la vitesse du décodeur.

### D. L’exploitation ajoutée progressivement

Lanceurs par version, régularisations, fichiers STOP, circuits historiques, sessions temporisées puis quotas permanents ont constitué plusieurs mécanismes de contrôle. Leur interaction pouvait arrêter le système sans que l’écran explique clairement la chaîne de décision.

### E. La mémoire des décisions insuffisamment liée aux preuves

Les README accumulés et les commits d’upload conservent le code, mais peu le « pourquoi ». Les changements de doctrine, comme celui des timestamps, auraient dû être enregistrés avec leurs conditions et leurs limites.

### F. Notre méthode de collaboration à corriger

J’ai contribué à cette dispersion en livrant des correctifs successifs avec trop peu de consolidation globale entre les étapes. Les limites des tests étaient parfois indiquées, mais elles ne suffisaient pas à empêcher que la version suivante soit perçue comme une validation générale. La V3 doit rendre cette distinction vérifiable dans les livrables et les décisions de passage.

## 11. Ce que nous conservons, refondons ou retirons

| Conserver comme connaissance ou invariant | Refondre dans la V3 | Retirer du parcours normal |
|---|---|---|
| Profondeur à la taille réellement abordable | Un moteur commun de décision pour replay, Shadow et LIVE | Classe A sans taille, mode et fraîcheur visibles |
| Deltas ordonnés, générations et invalidation des travaux anciens | Propriété explicite des carnets et budgets de traitement | Réinitialisations destinées à retrouver artificiellement un état vert |
| Protobuf natif et mesures de coût | Mesures de performance de bout en bout | Déduction de capacité globale à partir du seul benchmark de décodage |
| PnL signé, réservations et expositions persistantes | Un grand livre et des vues dérivées cohérentes | Confusion entre banque de profits, PnL et capital disponible |
| Protection des actifs préexistants | Registre d’inventaire et rapprochement des mouvements externes | Vente implicite d’un ancien bag pour débloquer une route |
| Confirmation et réconciliation avant nouvelle sortie | Machine d’états d’exécution et responsable d’exposition | Retry aveugle d’un ordre au résultat incertain |
| Corpus des incidents et tests de non-régression | Tests installables et isolés, replays d’événements réels | Tests dépendant d’un scanner de production ou de fichiers non livrés |
| Quota, budget et traces d’acquittement | Un seul contrôleur de risque, observateur indépendant | Lanceur et procédure de reprise différents pour chaque petit correctif |

« Conserver » ne signifie pas copier tout le code sans examen. « Refondre » ne signifie pas changer automatiquement de langage, de serveur ou de bibliothèque. Ces choix doivent être motivés par une mesure ou une propriété impossible à garantir autrement.

## 12. Cadrage proposé pour la V3

### 12.1 Périmètre initial

Produit distinct : **MEXC spot deux jambes**. La V3 commence par un socle de données et de replay qualifié. L’exécuteur réel ne devient disponible qu’après les étapes ci-dessous. Le trois-jambes, les transferts inter-plateformes et le rééquilibrage automatique réel restent des sujets séparés.

Le portefeuille de référence doit utiliser un budget explicite et des soldes observés, avec une valorisation documentée de chaque stablecoin. Le cap de 20 USDT peut rester une limite d’essai héritée ; ce n’est pas une taille économiquement optimale démontrée.

### 12.2 Responsabilités logicielles

| Composant | Responsabilité | Interdit architectural |
|---|---|---|
| Acquisition et horloges | Recevoir, décoder, horodater, mesurer la santé | Présenter un vieux message comme récent après republication |
| Carnets | Appliquer séquences et snapshots, publier un état versionné cohérent | Rendre prêt un carnet reconstruit par un travail devenu obsolète |
| Moteur de décision | Calculer à la taille, avec frais, soldes, règles et risques de sortie | Utiliser une règle économique différente selon l’écran ou le mode sans l’indiquer |
| Adaptateur de simulation | Simuler délais, remplissages et erreurs avec hypothèses identifiées | Prendre les données futures ou supposer que le MARKET réussit toujours |
| Exécuteur réel | Soumettre, suivre, réconcilier, gérer la quantité réellement acquise | Resoumettre en cas de résultat incertain sans résoudre l’incertitude |
| Grand livre et inventaire | Tenir quantités, frais, réservations, apports/retraits, réalisés et résidus | Déduire le PnL uniquement de l’évolution du solde sans traiter les transferts |
| Contrôleur de risque | Bloquer les entrées, gérer quota et circuits, suivre les expositions | Confondre arrêt des entrées et fin de responsabilité sur un achat rempli |
| Interface et observateur | Expliquer états, résultats et causes | Modifier implicitement l’armement en fermant une simple consultation |

Un superviseur peut volontairement bloquer les entrées si un composant essentiel disparaît. Il doit alors être déclaré comme composant de contrôle, séparé du simple observateur, et testé comme tel.

### 12.3 Machine d’états d’une tentative

La spécification doit couvrir au minimum : candidat refusé ; capital réservé ; achat préparé ; achat soumis au résultat connu ou incertain ; achat confirmé ; sortie admissible ou impossible ; sortie soumise ; sortie partielle ; secours ; exposition résiduelle ; clôture réconciliée.

Chaque transition comporte un événement, un identifiant d’ordre, une quantité et un motif. Une temporisation HTTP n’est pas une preuve d’absence d’ordre. Une réponse terminale et ses remplissages doivent être rapprochés avant toute nouvelle vente. Une quantité non vendable sous le minimum reste un résidu comptable identifié.

L’envoi journalisé avant POST et la reprise après incident doivent être conçus ensemble : si le processus s’arrête entre enregistrement et réponse, le redémarrage réconcilie avant toute nouvelle action.

### 12.4 Données minimales d’une expérience

- Identifiant d’expérience, version Git, configuration effective sans secrets, empreinte du jeu de données.
- Mode et portefeuille ; période UTC ; unité de chaque champ et définition des horizons temporels.
- Horodatages émission, réception, décodage, application, décision, mise en file, POST, réponse, événement privé et réconciliation REST.
- Horloge monotone pour les durées locales ; estimation d’offset et incertitude pour les comparaisons inter-horloges.
- Versions et état de chaque carnet utilisé ; prix et quantités parcourus ; règles de précision et minimums ; frais supposés puis payés.
- Décision et motifs de refus ; quantités réservées, soumises, remplies et restantes.
- Réponses POST, événements WS et confirmations REST conservés séparément. Une réponse finale normalisée ne doit pas écraser les preuves précédentes.
- Mouvements externes distingués des trades ; résultat réalisé et valeur de liquidation prudente des expositions encore ouvertes.

Les données publiques nécessaires aux incidents peuvent être conservées par fenêtres. Le volume et la durée de rétention sont dimensionnés ; une collecte illimitée ne doit pas ralentir le chemin d’exécution.

### 12.5 Tableau de bord de décision

L’écran principal répond à cinq questions : les deux carnets concernés sont-ils utilisables ? Pourquoi une route est-elle acceptée ou refusée ? Un ordre ou une exposition réclame-t-il une action ? Quel est le résultat net du portefeuille réel ? Quel composant a autorisé ou arrêté les entrées ?

Les Shadows restent des expériences séparées. Chaque tableau indique période, capital, taille, frais et modèle de remplissage. Les détails se déplient. Les compteurs quotidiens, historiques et de quota portent leur périmètre dans leur libellé.

## 13. Plan de qualification V3

Ces étapes sont des propositions de travail. Elles ne constituent pas un réarmement autorisé par ce document.

| Étape | Livrable | Condition de passage |
|---|---|---|
| 0 — Référence figée | Version V2.4.14 identifiée, historique, configuration expurgée, index des preuves et présent REX | Aucun historique économique perdu ; inconnues listées ; état de production clairement distingué des tests |
| 1 — Contrat de fonctionnement | Définitions des métriques, invariants, états d’ordre, règles d’inventaire et d’arrêt | Chaque incident REX relié à une exigence et à une vérification ; contradictions résolues explicitement |
| 2 — Données et charge | Rejeu déterministe, scénarios de séquence, charge complète et reprise après interruption | Pas de saut de delta silencieux, publication obsolète ou reconstruction perdue ; aucune admission sur un carnet invalidé |
| 3 — Décision et économie | Replay avec même moteur que la cible LIVE, coûts et latences mesurés, périodes de validation séparées | Résultats complets incluant pertes et expositions ; sensibilité présentée ; paramètres figés avant la période de validation |
| 4 — Exécution simulée hostile | Scénarios d’annulation, réponses tardives, partiels, frais en base, perte réseau, redémarrage | Aucun ordre dupliqué sous incertitude ; aucune quantité préexistante vendue ; toutes les expositions retrouvées |
| 5 — Observation serveur | Rapport de flux et décisions avec le chemin complet actif, ordres désactivés | Capacité démontrée dans des périodes variées ; files bornées et retours à l’état sain expliqués ; pas seulement un instant à 656/656 |
| 6 — Essai réel explicite | Protocole, capital et budget approuvés, rapprochement ordre par ordre, arrêt et intervention vérifiés | Les protections sont qualifiées ; le risque d’annulation du secours est explicitement pris en compte |
| 7 — Évaluation | Comparaison prévisions/remplissages, PnL net, fréquence des secours, exposition et incertitude | Décider de poursuivre, modifier une hypothèse ou abandonner ; pas de promotion automatique parce qu’un quota est terminé |

### Mesures à fixer avant les essais, pas après lecture des résultats

Pour les flux : distribution des délais par étage et par connexion, fraction de temps où une route est admissible, temps de rattrapage, pertes de messages, durée de reconstruction, taux et causes de fermeture, charge CPU/mémoire et coût des écritures.

Pour l’exécution : délai décision→POST→confirmation, écart prix prévu/prix rempli, taux de L2 envoyées et remplies, secours, refus et états incertains, durée d’exposition, valeur et coût des résidus.

Pour l’économie : PnL net après tous les frais, résultat par route et sens, besoins d’inventaire, rééquilibrage, drawdown incluant les expositions, sensibilité aux paramètres. Un échantillon de dix achats peut révéler un défaut, mais ne démontre pas une rentabilité durable.

Une proposition pratique est d’observer d’abord une heure complète, puis une journée couvrant plusieurs régimes, en distinguant démarrage et régime établi. Ce sont des fenêtres de diagnostic proposées, pas une garantie de qualité ni un seuil statistique universel. La durée seule ne suffit pas si aucune situation représentative n’a été rencontrée.

La découverte de critères économiques défavorables doit pouvoir conduire à ne pas lancer le LIVE. L’objectif n’est pas de rendre l’activation inévitable en assouplissant les conditions.

## 14. Organisation de la nouvelle branche

L’utilisateur créera la branche. Nom proposé : **`mexc-spot-2leg-v3`**. Conserver l’historique existant et identifier le point de départ V2.4.14 ; éviter de supprimer les anciennes preuves pour obtenir une arborescence visuellement propre.

Organisation documentaire proposée :

| Emplacement | Contenu |
|---|---|
| `README.md` | Périmètre actuel, état de qualification, procédure unique et lien vers ce REX |
| `docs/REX_GLOBAL.md` | Ce bilan, corrigé de façon traçable lorsque de nouvelles preuves arrivent |
| `docs/SPEC_V3.md` | Contrat de fonctionnement approuvé avant implémentation |
| `docs/decisions/` | Une note par choix important : problème, options, décision, preuve, conséquences |
| `docs/experiences/` | Fiches comparables des essais, y compris les résultats négatifs |
| `tests/fixtures/` | Scénarios anonymisés et bornés tirés des incidents ; jamais la base réelle active |
| `CHANGELOG.md` | Changements regroupés par problème résolu, avec limites de validation |

Les gros exports et les informations de compte restent dans un stockage de preuves adapté. Le dépôt contient leurs références et empreintes, pas de clés API ni de journaux privés bruts publiés par inadvertance.

Une livraison regroupe une évolution cohérente. Son compte rendu indique : problème, hypothèse, changement, tests, observation serveur, effet économique connu ou inconnu. La numérotation ne sert plus de mesure de progrès.

## 15. Questions ouvertes et prochaines actions

### Priorité immédiate : consolider la référence, avant de coder

1. Compléter la couverture des discussions V1–V2.3 et l’étude de l’export étendu de 741 Mo, si ces éléments peuvent être retrouvés. Une source nouvelle doit modifier une conclusion précise ; inutile de redemander toutes les captures sans cible.
2. Récupérer le dernier bilan V2.4.14 et l’origine du STOP pour fermer le diagnostic d’exploitation. Cela ne remet pas en cause les pertes déjà établies.
3. Confronter ce REX aux intentions de l’utilisateur : ce qu’il veut automatiser, ce qu’il accepte comme inventaire de stablecoins, et les interventions humaines prévues en cas de résidu.
4. Écrire la spécification V3 à partir des exigences REX, puis implémenter le socle données/replay. Le premier livrable V3 est une base qualifiable, pas une reprise immédiate des achats.

### Inconnues qui doivent rester visibles

- Cause exacte de l’annulation du MARKET CTO. La robustesse ne peut pas dépendre d’une explication non confirmée.
- Rentabilité hors du jeu historique ayant servi à choisir les seuils.
- Distribution complète des latences et remplissages réels par marché.
- Coût de maintien des inventaires et de rééquilibrage à la taille réelle du compte.
- Cause de chaque fermeture WS : les événements conservés ne couvrent pas forcément toutes les coupures historiques.
- Exhaustivité du rapprochement entre journaux du bot, transactions MEXC et mouvements manuels.

## 16. Sources et traçabilité

### Sources GitHub relues dans cette consolidation

- [Historique de la branche existante](https://github.com/tiramysu92/mexc-gate-scanner/commits/mexc-spot-routes-v23) : 68 commits retournés, du 1er au 14 septembre 2026 ; tête observée `c8f4be68ab1734eb989ce0fb654f067b7e0ffc6c`. L’historique d’une branche ne garantit pas la présence de tous les essais hors dépôt.
- [README du scanner initial MEXC–Gate](https://github.com/tiramysu92/mexc-gate-scanner/blob/b61f6927/README.md) : limites BBO, profondeur, latence et inventaires déjà explicites.
- [README Universe V3.4](https://github.com/tiramysu92/mexc-gate-scanner/blob/938c6691/README.md) : évolution REST/WS, cache d’univers, événements et tailles.
- [Début du code Routes V1](https://github.com/tiramysu92/mexc-gate-scanner/blob/dd3a9c63/app.py) : `VERSION=1.0`, configuration spot et capitaux. Lecture ciblée du début du fichier, pas audit intégral de cette version.
- [README V2.3](https://github.com/tiramysu92/mexc-gate-scanner/blob/1b10c209/README.md) : profondeur stricte, doctrine d’âge local, secours et anti-répétition.
- [README V2.3.1](https://github.com/tiramysu92/mexc-gate-scanner/blob/9c89085e/README.md) : exposition persistante et valorisation non artificiellement nulle.

### Sources locales et données utilisateur

| Source | Utilisation / limite |
|---|---|
| `Dossier_reprise_Arbitrage_MEXC_2026-09-14.md` | Chronologie et décisions antérieures ; document de synthèse, pas transcription intégrale |
| `Rapport_simulations_MEXC.md` | Comparaisons de replay et leurs hypothèses ; résultats historiques non intégralement recalculés ici |
| `Rapport_V235_reference.md` | Référence prudente et contrôle de fin de période |
| `mexc_v246_live_losses_20260913_133204.db` | Trois lignes LIVE et PnL relus en SQLite en lecture seule |
| `Diagnostic_CTO_MEXC_2026-09-14.md` et export CTO reproduit par l’utilisateur | Chaîne d’achat, garde L2, secours et inconnues ; pas de réponse officielle à l’annulation |
| Captures de vente CTO et mouvements décrits par l’utilisateur | Produit manuel, frais et sortie externe ; absence de réaudit complet du compte |
| `bilan_flux_v2413_20260914_183631_568489.json.gz` | Agrégats relus et dernière période complète recalculée sur les 240 échantillons |
| `v2413_work/README_V2413.md` | Course de resynchronisation reproduite, correction et limites d’attribution au serveur |
| `v2414_work/reprendre_live_v2414.py` | Fonction `monitor()` relue : STOP créé dans le bloc de fin de surveillance |
| Sorties serveur V2.4.8–V2.4.14 et dernières captures dans cette conversation | Installation, tests, armement puis STOP rapportés ; pas une interrogation actuelle du serveur |

Les fixtures `synthetic_cto_attempt`, datées de 2024, sont des données de test. Elles ne sont pas un second incident réel. Les préfixes historiques `v247` dans les identifiants et les noms de tables conservés ne suffisent pas à identifier la version du processus ; pour CTO, l’observation indique V2.4.10.

### Statut de ce document

Ce REX consolide ce qui est accessible au 14 septembre. Il ne prétend pas avoir réaudité chaque ligne de toutes les versions, reproduit tous les anciens replays ou retrouvé toutes les discussions. Les exigences V3 sont proposées pour transformer les leçons établies en propriétés vérifiables. Aucun changement de code du bot, migration comptable, déploiement ou ordre réel n’a été effectué pour produire ce bilan.
