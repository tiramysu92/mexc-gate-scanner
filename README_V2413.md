# V2.4.13 — demandes de resynchronisation conservées

Cette version corrige un blocage reproduit dans la V2.4.12. Elle améliore aussi les mesures des coupures WebSocket. Son installation conserve STOP : elle ne réarme pas le LIVE et n'alloue aucun budget supplémentaire.

## Ce que montre le relevé du 14 septembre 2026

| Observation fournie | Conclusion étayée |
|---|---|
| ACHUSDC et BEAMXUSDT : dernier événement `NOT_READY / version_or_book_invalid`, sans `RETRY` ni `READY` ultérieur dans le résultat | Deux demandes de reconstruction paraissent rester sans traitement. Au moment du fichier, ces événements datent d'environ 10,8 et 12,2 minutes. |
| Cinq `exchange_backlog`, retards de 1 055 à 1 119 ms | La réception mesure un retard par rapport au timestamp MEXC. Cela ne situe pas l'attente chez MEXC, sur le réseau ou avant le callback Python. |
| Un `receive_queue_full` sur le worker 27, 1 024 messages, environ 2 279 ms d'attente et dernier retard reçu de 7 ms | Un engorgement local est établi pour cet épisode. La cause du ralentissement du consommateur n'est pas déterminée par ces seuls maxima. |
| Plusieurs `Connection to remote host was lost.` | Une fermeture de transport est détectée. Le message ne donne pas à lui seul la cause initiale. |

Le fichier complet `diagnostic_ws_depth_20260914_174448_759219.json.gz` contient davantage de compteurs que le texte collé. Il reste utile pour analyser cet épisode antérieur à la correction.

## Défaut reproduit et correction

Le worker de resynchronisation reconstruit un carnet, le rend prêt puis attend 250 ms pour limiter le débit REST. Pendant cette pause, le symbole reste dans l'ensemble des demandes en cours. Si un nouveau delta invalide le carnet pendant la pause, la nouvelle demande constate que le symbole est déjà présent et n'ajoute rien dans la file. Le worker termine ensuite en retirant ce symbole. Les deltas suivants sont tamponnés, mais personne ne reconstruit ce carnet.

Deux tests reproduisent cette perte dans le code V2.4.12 : invalidation avant l'enregistrement du succès et invalidation pendant la pause. La V2.4.13 associe un compteur de génération à chaque demande. Le worker ne libère un symbole que si aucune demande plus récente n'est arrivée. Un succès devenu obsolète ne produit plus un événement READY qui masquerait l'invalidation récente. Une seule entrée par symbole reste en file ; les symboles en échec repassent en fin de file.

Ce défaut est cohérent avec les deux carnets du relevé, mais le diagnostic fourni n'observe pas directement l'entrelacement des threads sur le serveur. Il ne démontre pas que toutes les déconnexions ont cette même cause.

## Mesures supplémentaires

- `health.depth_not_ready` liste les carnets réellement non prêts avec leur symbole, version, état de réception, présence dans la file et motif de resynchronisation.
- `health.public_ws_close_events` conserve les 200 dernières fermetures. Chaque événement contient la durée de connexion, les PING/PONG récents, le dernier PING de contrôle reçu, les réponses d'abonnement, le motif local éventuel, le code de fermeture et l'état de la file.
- Le dernier événement d'une connexion reste disponible après sa réouverture, et les événements sont aussi envoyés au writer de la base de recherche, table `public_ws_close_events_v2413`. Les pertes éventuelles de la file DB restent visibles dans son compteur habituel.
- Les seuils d'admission, la capacité et la limite d'âge des files, la fréquence 10 ms et la cadence PING/PONG sont conservés. Aucun symbole n'est retiré pour améliorer artificiellement le compteur.

Le code de `websocket-client` répond déjà aux PING de contrôle par PONG. Le nouveau callback observe leur réception ; il ne double pas la réponse : [code officiel](https://websocket-client.readthedocs.io/en/latest/_modules/websocket/_core.html). La chaîne d'erreur de transport provient de la [lecture de socket](https://websocket-client.readthedocs.io/en/latest/_modules/websocket/_socket.html). Le mécanisme JSON PING/PONG MEXC est décrit dans la [documentation officielle](https://mexcdevelop.github.io/apidocs/spot_v3_en/#ping-pong-mechanism).

## Installation sur le serveur

Décompresser l'archive et placer ses fichiers à la racine de `tiramysu92/mexc-gate-scanner`, branche `mexc-spot-routes-v23`. Remplacer les fichiers homonymes. Conserver les bases de données, les anciens fichiers requirements et les fichiers d'armement. Cette version n'ajoute aucune dépendance Python.

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
/home/ubuntu/mexc-venv/bin/python verifier_v2413.py &&
/home/ubuntu/mexc-venv/bin/python relancer_mexc_v2413.py &&
/home/ubuntu/mexc-venv/bin/python surveiller_flux_v2413.py --minutes 20
```

Les tests utilisent des processus factices et des journaux temporaires. Des messages de circuit ouvert peuvent apparaître dans les scénarios simulés. Aucun test n'envoie d'ordre à MEXC. Les `&&` empêchent le redémarrage si une vérification échoue.

Le lanceur détecte le processus unique du projet, vérifie la version préparée, suspend les nouvelles entrées et attend la fin des ordres/confirmations avant d'arrêter le processus. Il conserve son environnement en mémoire et le relance en V2.4.13. Il conserve STOP et la date du fichier d'armement. Le circuit, la perte CTO comptabilisée et la session V2.4.12 éventuelle gardent leur état. Une échéance dépassée n'est pas prolongée.

Le journal est `scanner_v2413_live.log`. L'affichage initial de quelques carnets prêts correspond au début de la reconstruction. Les contrôles de démarrage du lanceur confirment uniquement que le nouveau processus répond ; ils ne valident pas une stabilité prolongée.

La surveillance commence sur ce processus et ne modifie aucun armement. Elle produit `bilan_flux_v2413_….json.gz`. L'envoyer dans la conversation. La durée de 20 minutes couvre la reconstruction et permet d'observer une période plus longue que les cinq minutes d'accalmie précédentes. Ce bilan ne prouve pas la rentabilité du bot.

## Validation livrée

Le vérificateur contrôle les empreintes du paquet puis exécute les suites existantes de flux, sorties, comptabilité, relance et session bornée, ainsi que les nouveaux scénarios de resynchronisation, de fermeture WS et de relance V2.4.13. Les nouvelles fermetures sont testées avec et sans callback `on_close`, et leur enregistrement dans une base SQLite temporaire est vérifié.

La comparaison syntaxique avec V2.4.12 confirme que les fonctions d'admission LIVE, de construction d'ordres, de sortie et de comptabilité ne sont pas modifiées. Les changements de `app.py` portent sur la resynchronisation, le suivi des fermetures publiques, le writer de recherche et les indicateurs de santé. Les coupures de transport et les engorgements éventuels restent à mesurer sur le serveur.
