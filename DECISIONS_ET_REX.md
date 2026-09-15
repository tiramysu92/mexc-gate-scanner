# Registre des décisions et retours d’expérience

Ce fichier relie un fait historique à une règle de V3 et à sa vérification. Une modification de règle doit mettre à jour sa ligne ; une nouvelle version ne remplace pas la preuve précédente.

| Fait ou incident | Décision V3 | Vérification |
|---|---|---|
| Carnets reçus récemment mais émis plusieurs minutes auparavant | Horloge exchange et horloge de réception distinctes ; continuité obligatoire | `test_five_old_exchange_cases_remain_rejected`, `test_fresh_receive_does_not_rejuvenate_exchange_time` |
| V2.4.8 : tempête de reconnexions lors d’un rattrapage local | Pause d’admission pendant le rattrapage ; reconnexion sur débordement ou absence durable de progrès | `test_local_pause_drains_without_reconnecting`, limites de file |
| V2.4.9–10 : contention et connexions chaudes | Un dispatcher équitable, quantum 32, reconstruction par symbole | `test_quantum_leaves_room_for_cold_socket` |
| Réponse REST obsolète après une nouvelle invalidation | Jeton connexion + génération ; une ancienne réponse ne peut pas publier | Tests de génération et ancien flux |
| V2.4.13 : keepalive MEXC | PING JSON toutes les 15 s, phases réparties, délai PONG 45 s ; pas de minuterie RFC PING concurrente | `tests/test_heartbeat.py` |
| CTO : achat confirmé puis L2 locale 105 ms | Nouvelle sélection de sortie après réception ; seuil de nouvelle entrée distinct de l’objectif de sortie | Cas CTO et sortie légèrement négative |
| CTO : ordre MARKET annulé sans explication | Pas de conclusion inventée sur MEXC ; sorties FOK protégées, deux tentatives maximum après rapprochement | Annulations répétées, contradiction, timeout |
| A/B+ de recherche différentes de l’admission réelle | Épisodes bruts, classe V3, admission et causes séparés | Compteurs et captures communes de comparaison |
| Soldes anciens SAGA/SKL/牛来 | Inventaire préexistant réservé ; contrôle des actifs extérieurs et poussières propres au bot | Test de préservation de 100 unités préexistantes |
| Frais retardés et quantités résiduelles | Réserve provisoire, frais privés exacts seulement sur couverture complète ; poussière et coût suivis | Tests de commissions privées, simulation avec réserve |
| Perte manuelle CTO | Import en lecture seule du journal source, aucun effacement du réalisé | Tests d’import, refus des fixtures synthétiques |
| Arrêt lors de la fermeture d’un surveillant | Surveillant uniquement en lecture ; `STOP` appartient au contrôle d’entrées | Intégration observation, code du surveillant sans écriture de contrôle |
| Tests dépendant du scanner réel ou de fichiers non livrés | Fichiers autonomes, base temporaire par test, réseau interdit, manifeste du paquet | `verifier_v3.py`, extraction du ZIP et exécution des tests |
| Profils de latence devenus irréalistes | Huit échantillons historiques explicitement rares et scénarios de stress séparés | `simulation.py`, provenance ci-dessous |

## Provenance des délais

| Marché / rôle historique | Type | Notification privée depuis POST |
|---|---|---:|
| SAGAUSDC / achat | MARKET | 69 ms |
| SKLUSDT / achat | MARKET | 71 ms |
| 牛来USDT / achat | MARKET | 58 ms |
| CTOUSDT / achat | FILL_OR_KILL | 69 ms |
| SAGAUSDT / L2 | MARKET | 48 ms |
| SKLUSDT / retour | MARKET | 52 ms |
| 牛来USDT / retour | MARKET | 74 ms |
| CTOUSDT / retour annulé | MARKET | 52 ms |

La source des trois premières paires est l’export V2.4.6 communiqué ; CTO vient du diagnostic réel fourni dans la conversation. Ces observations ne suffisent pas à estimer une probabilité de remplissage. La vitesse apparente d’un accusé de réception ne prouve pas un gain.

## Méthode de changement

1. Nommer le problème et conserver un cas reproductible ou une trace réelle.
2. Définir ce qui est mesuré et ce qui demeure hypothétique.
3. Modifier une politique identifiable, avec sa comparaison et son dénominateur.
4. Vérifier les risques concrets concernés : quantité, prix, incertitude, reprise, comptabilité ou retard.
5. Consigner le résultat observé sur le serveur avant de conclure à un gain de débit, de remplissage ou de rentabilité.

La documentation historique jointe conserve ses formulations et propositions datées. `PROPOSITION_V3.md` indique le périmètre effectivement écrit dans cette candidate ; une ancienne proposition de recherche ne doit pas être présentée comme une fonction déjà opérationnelle.
