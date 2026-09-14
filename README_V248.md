# MEXC V2.4.8 — flux publics et interface compacte

Le correctif agit sur le traitement local et empêche un flux actif de conserver durablement une file de données anciennes. Il conserve les marchés suivis, les abonnements à 10 ms et les règles privées V2.4.7. Les panneaux Shadow et BOT sont regroupés en tableaux compacts. Validation locale terminée ; le résultat sur le serveur reste à mesurer.

## Installation dans le dépôt existant

À la racine de `tiramysu92/mexc-gate-scanner`, au même niveau que l'ancien `app.py`, déposer le contenu de cette archive :

- `app.py` remplace le fichier V2.4.7 ;
- `test_public_flow.py`, `benchmark_public_flow.py`, `requirements_v248.txt` et ce document sont ajoutés ;
- `collecte_mexc_timestamps.py` remplace la version précédente du collecteur ;
- le dossier de reprise est fourni pour conserver l’analyse et les décisions entre les fils.

Conserver le commit V2.4.7 pour pouvoir revenir à ce fichier. Sur le serveur, depuis le même environnement Python que le scanner :

```bash
cd /home/ubuntu/mexc-gate-scanner
git pull --ff-only
python3 -m pip install -r requirements_v248.txt
python3 test_public_flow.py
```

La seule dépendance ajoutée est `protobuf==7.36.1`, version testée avec le backend natif `upb`. Le démarrage refuse un backend Protobuf en Python pur ou absent. Les dépendances existantes Flask, requests et websocket-client sont conservées.

Après succès des tests, redémarrer avec le lanceur habituel du scanner. Le mode exact de lancement du serveur (service ou nohup, chemin du venv) n'a pas été exporté : cette archive ne fournit donc pas une commande de suppression de processus supposée. Ne pas démarrer un deuxième scanner en parallèle.

Ce correctif conserve les paramètres d'armement existants : un lanceur déjà configuré et armé en LIVE le reste. Il ne supprime aucun historique et ne réinitialise aucun coupe-circuit. Les tests fournis sont hors ligne, ne démarrent pas le bot et n'émettent aucun ordre.

## Changements

1. Décodage natif des messages Depth, avec les champs du schéma officiel MEXC : `symbol=3`, `sendTime=6`, `publicAggreDepths=313`, versions 4/5 dans le corps. Le décodage des ordres privés reste inchangé.
2. Le callback public horodate dès son entrée, lit l'en-tête et met les octets dans une file bornée. Le décodage complet et la mise à jour des carnets s'exécutent dans un consommateur séparé par connexion.
3. Tous les deltas cohérents sont appliqués dans l'ordre. Publication BBO une fois par symbole et par petit lot, limité à 32 messages et une cible de travail de 2 ms. Cette cible n'est pas une garantie de temps réel : un message ou une attente de verrou peut durer davantage.
4. Le meilleur bid/ask est mis à jour avec les niveaux modifiés. Un parcours complet n'est requis que si le meilleur niveau a été supprimé. Aucune profondeur n'est tronquée par cette optimisation.
5. Une file pleine, une attente de plus de 100 ms ou un retard d'émission supérieur à 1 000 ms persistant sur au moins trois messages d'un symbole pendant au moins 200 ms déclenche la récupération de sa connexion. Le contrôle utilise l'âge de messages reçus ; il ne reconnecte pas simplement parce qu'un marché calme n'a pas bougé.
6. La connexion est immédiatement marquée inutilisable, ses carnets sont invalidés, puis elle est fermée et reconstruite avec snapshot REST et deltas WS cohérents. Un identifiant de génération empêche un ancien consommateur ou une ancienne réponse REST de remplacer le nouveau carnet. Backoff et rythme REST existants sont conservés.
7. Les observations A/B+ dont les timestamps MEXC sont périmés restent dans le dataset mais sont refusées pour une nouvelle admission Shadow de recherche. La classe économique et la taille restent disponibles ; `eligible` et le motif indiquent le refus temporel.
8. Un panneau « Flux publics V2.4.8 », le statut HTTP et les checkpoints `diagnostics_v247` exposent le backend, le débit, les files, les coûts de réception/décodage/application, l'attente du verrou et l'historique récent des récupérations.

Les seuils de récupération 100/1 000 ms ci-dessus ne changent PAS les seuils d'admission LIVE : local 50 ms, MEXC 100 ms, skew 50 ms. Les garde-fous L2, FOK, frais, limites 20/200/5 USD et verrous d'armement ne sont pas assouplis.

## Interface plus compacte

- Deux tableaux côte à côte sur grand écran : 3 lignes pour les Shadows de recherche, 6 lignes pour les simulations de latence BOT. Ils s'empilent sur un écran étroit.
- Profil, délais L1/L2, PnL, trades, gagnés/perdus et NAV restent lisibles dans la synthèse. Sur mobile, la NAV est disponible dans les détails.
- Soldes, edge moyen, rééquilibrages, coûts, refus et expositions sont accessibles dans « Soldes, coûts et détails ».
- Une synthèse LIVE conserve en haut le mode, le motif d'armement ou de circuit, le PnL réalisé à quatre décimales, les trades, les expositions, les limites et l'état API.
- Les panneaux techniques et les métriques LIVE détaillées sont repliés par défaut ; le résumé des flux reste visible.
- L'ouverture d'un panneau n'est pas annulée par le rafraîchissement automatique toutes les trois secondes. Les latences restent mesurées depuis T0 pour la recherche et depuis l'admission fraîche pour les profils BOT.

Le compactage de l'interface ne change pas les calculs des portefeuilles. Le filtre temporel des nouvelles admissions de recherche, décrit plus haut, est un changement distinct. Les résultats historiques déjà stockés restent historiques ; ils ne sont pas recalculés rétroactivement.

## Mesures et tests

17 tests couvrent le décodage natif comparé au décodeur historique, l'ordre des deltas, le timestamp d'entrée conservé, les trous de version, les files pleines ou anciennes, les flux tardifs, les courses entre reconnexion et réponse REST, l'absence de timestamp MEXC sur REST, le blocage avant acquisition du verrou, un flux sain avec threads, les cinq cas de l'utilisateur et les limites privées.

Les essais de performance sont synthétiques, dans le runtime local. Avec 25 niveaux par côté : environ 87,5 µs par paquet pour le décodeur Python et 17,5 µs pour le natif, soit environ 5×. Avec 100 niveaux par côté : 329,1→65,6 µs. La lecture du seul en-tête prend environ 2 µs, mais le consommateur doit toujours effectuer le décodage complet.

Sur un carnet synthétique de 100 niveaux par côté et des changements de quantités ne supprimant pas le meilleur prix, fusion et lecture du BBO passent d'environ 4,86 à 0,80 µs. Le gain dépend des suppressions de niveaux. Ce n'est pas une mesure du PnL, de la latence d'exécution ou du débit global du serveur.

```bash
python3 benchmark_public_flow.py
```

L'import avec les vraies dépendances, la compilation Python et la syntaxe JavaScript du tableau de bord ont été vérifiés. Le rafraîchissement complet a également été exécuté avec un DOM de test : 3 + 6 lignes, mise à jour des valeurs, maintien des détails ouverts et absence de profils. Il ne s’agit pas d’une validation visuelle dans un navigateur réel. Aucun test d'ordre réel ni essai réseau MEXC du nouveau pipeline n'a été réalisé ici.

## Validation sur le serveur

Après reconstruction des carnets, vérifier la version `2.4.8-bounded-public-flow` et le backend `upb` ou `cpp`. Observer le panneau plusieurs minutes : les retards des messages actifs doivent rester bas au lieu de monter continuellement, les files doivent se vider et les récupérations ne doivent pas boucler. « Actif » signifie réception disponible, pas admission LIVE à lui seul.

Après dix minutes d'observation, exporter :

```bash
python3 collecte_mexc_timestamps.py --recent-minutes 10
```

Le collecteur utilise les noms de bases existants `mexc_routes_v247.db`, `mexc_live_v247_test.db`, `mexc_live_v246.db`. La version V2.4.8 conserve ces chemins afin de préserver la continuité. Pour des chemins personnalisés, utiliser `--db` répété ; pour un port personnalisé, `--port`.

Si les récupérations se répètent, cela signale un problème de débit ou d'amont qui persiste ; ce n'est pas un succès à masquer. Les nouvelles mesures serviront à isoler le coût restant par connexion. Le mécanisme ne peut pas garantir qu'un serveur insuffisamment dimensionné ou une source qui émet des données anciennes deviendra exploitable.

## Sources de protocole vérifiées

- [Enveloppe officielle MEXC](https://github.com/mexcdevelop/websocket-proto/blob/main/PushDataV3ApiWrapper.proto)
- [Depth agrégé officiel MEXC](https://github.com/mexcdevelop/websocket-proto/blob/main/PublicAggreDepthsV3Api.proto)
- [Fabrique de messages officielle Protobuf](https://github.com/protocolbuffers/protobuf/blob/main/python/google/protobuf/message_factory.py)
