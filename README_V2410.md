# MEXC V2.4.10 — traitement public équitable

## Diagnostic du relevé serveur du 14 septembre 2026

Fichier analysé : surveillance_mexc_20260914_030624_097982.json.gz.
100 relevés valides, sans erreur HTTP, sur environ 297 secondes.

- 33 déconnexions et 33 reconnexions. Les 33 nouvelles récupérations observées dans cette fenêtre portent le motif `receive_queue_full`.
- WS : 23 à 28 sur 29 ; aucun relevé à 29. Carnets prêts : 534 au début, 509 à la fin, maximum 617 sur 709.
- Dernier retard réception–émission des flux actifs : médiane 15 ms, p95 22 ms, maximum 34 ms. Ces statistiques portent sur le dernier message de chaque relevé, pas sur tous les paquets.
- Attente locale des flux actifs : médiane 74,88 ms, p95 2 647,12 ms, maximum 4 980,24 ms. Le maximum précédent d’environ 32 secondes incluait des files de connexions arrêtées.
- Au moins 9 023 débuts et 8 993 fins de rattrapage observés. Les changements de génération rendent ces décomptes incomplets.
- Files pleines à 1 024 messages, bien avant le plafond de 4 Mio de données brutes en file.

| Connexion | Messages reçus/s | Messages traités/s |
|---|---:|---:|
| 18 | 323,5 | 182,6 |
| 24 | 296,2 | 187,5 |
| 27 | 272,0 | 195,9 |

Débits calculés par différences de compteurs sur les intervalles où la connexion est active aux deux extrémités et reste dans la même génération. Ce ne sont pas des moyennes sur les périodes déconnectées.

L’accumulation locale est démontrée. Le code V2.4.9 utilise 29 consommateurs concurrents autour du même verrou de carnet ; le budget de lot de 2 ms inclut l’attente du verrou. Les lots observés ne contiennent qu’environ 1,1 message. Ces observations motivent le changement ci-dessous ; elles ne mesurent pas directement la saturation CPU ni le coût de chaque autre composant.

## Changement

Un seul consommateur public parcourt les connexions à tour de rôle. Il prend jusqu’à 32 messages déjà disponibles par connexion, les décode avant le verrou du carnet et applique le lot sous une seule acquisition externe du verrou. Il n’attend jamais qu’un lot se remplisse. Un seul watchdog public remplace les watchdogs par connexion.

La file reste limitée à 1 024 messages et 4 Mio bruts par connexion ; ce plafond d’octets concerne la file, pas toute la mémoire du processus. Un lot en cours peut contenir jusqu’à 32 messages supplémentaires. Le quantum de 32 messages ne constitue pas une garantie de durée maximale.

L’ordre des deltas, les générations des carnets, le blocage pendant le rattrapage, les contrôles de fraîcheur et les récupérations sur débordement/blocage sont conservés. Les demandes de resynchronisation et les écritures de mesures sont transmises après libération du verrou du lot.

Les routes, les abonnements à 10 ms, les seuils LIVE et les fonctions d’admission privées ne changent pas. Le changement ne constitue pas une nouvelle autorisation LIVE.

Le statut expose aussi des compteurs globaux de rattrapage/récupération conservés entre reconnexions, les compteurs de messages par symbole, les statistiques du consommateur, le temps CPU cumulé et le nombre de threads. Le collecteur inclus sauvegarde ces champs bruts ; son bilan des rattrapages par connexion reste un minimum observé.

## Vérifications locales

- 34 tests de flux passent, dont le service équitable entre une connexion chargée et une connexion peu active, le réveil sans attente de remplissage, le décodage hors verrou, les resynchronisations hors verrou et les protections existantes.
- Le scénario synthétique reprend les débits observés des 29 connexions et répartit les mises à jour sur 709 carnets. Il termine sans perte ni récupération. Il teste la chaîne publique isolée : les écritures DB et les notifications de scan y sont simulées, pas toute la charge du scanner.
- 7 tests de relance sur processus factices passent : attente de fin des ordres, conservation de l’environnement et de la suspension préexistante, refus de reprendre avec des limites différentes, maintien de la suspension après échec.
- Import réel et backend upb validés, syntaxe JavaScript validée ; 58 constantes LIVE_/PRIVATE_ et les fonctions de contrôle d’admission comparées à V2.4.9 sont inchangées.

Ces résultats ne valident pas encore la stabilité sur le serveur. La mesure après installation reste nécessaire.

## Installation

Décompresser le ZIP puis mettre son contenu à la racine de `tiramysu92/mexc-gate-scanner`, branche `mexc-spot-routes-v23`. Remplacer app.py et test_public_flow.py ; ajouter les autres fichiers. Conserver le dossier verification_relance et les requirements existants. Aucune nouvelle dépendance par rapport à V2.4.9.

Dernier PID fourni : 114285. Le script vérifie le processus et exige une V2.4.9 active ; si le PID a changé, il refuse au lieu de viser un autre processus.

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
/home/ubuntu/mexc-venv/bin/python test_public_flow.py &&
/home/ubuntu/mexc-venv/bin/python relancer_mexc_v2410.py --pid 114285
```

La relance suspend les nouvelles entrées, attend la fin des opérations en cours, conserve l’environnement existant et reprend uniquement l’armement déjà valide. Le journal est scanner_v2410_live.log. En cas d’échec, lire le message et ne pas supprimer manuellement le fichier de suspension.

Résultat attendu : version 2.4.10-fair-public-dispatch, Protobuf upb. Le nombre de carnets peut être faible au tout premier relevé de démarrage.

## Surveillance après relance

```bash
bash surveillance_mexc_5min.sh
```

Lecture seule de l’API locale pendant cinq minutes. Ctrl+C sauvegarde un bilan partiel. Renvoyer le fichier surveillance_mexc_....json.gz produit. Le maximum affiché porte désormais uniquement sur les files actives.

Critères à vérifier : fin des débordements répétés, reconnexions devenant occasionnelles, remontée puis stabilité des carnets prêts, attente active qui se résorbe et débits traités capables de suivre les débits reçus. Si les files débordent encore, ce correctif ne suffit pas : les nouveaux compteurs aideront à localiser la charge restante, sans élargir les seuils d’entrée.
