# MEXC V2.4.9 — rattrapage local sans boucle de reconnexions

## Problème confirmé sur V2.4.8

Les dix récupérations fournies par l'utilisateur sont toutes `receive_queue_age`.
Elles se produisent en 76 ms, avec des files de 9 à 78 messages et un retard du
dernier message de 6 à 19 ms. Les générations des connexions sont déjà 8 à 10.
Le déclencheur confirmé est le contrôle d'attente locale de 100 ms, qui coupait
immédiatement le socket et invalidait tous ses carnets. Ces relevés ne mesurent
pas le retard de chaque message en file et ne déterminent pas, à eux seuls,
la part du CPU, du GIL ou d'un verrou dans le ralentissement initial.

Une reproduction hors réseau sur V2.4.8 déclenche 29 récupérations pour une
attente locale de 200 ms. Le nouveau test à 29 connexions et verrou partagé
ralenti conserve toutes les versions des carnets et termine sans récupération.

## Correction

- Au-delà de 100 ms d'attente locale, le flux passe en **rattrapage**. Ses nouvelles
  entrées sont bloquées, mais la connexion et les carnets cohérents sont conservés.
- Le consommateur applique tous les deltas dans l'ordre, avec leurs horodatages
  initiaux. Aucun horodatage de réception ou MEXC n'est rajeuni pour rendre un
  carnet admissible. Le traitement peut continuer pendant la suspension des entrées.
- La suspension de rattrapage est levée en fin de lot lorsque l'attente restante
  ne dépasse plus 25 ms, ou lorsque la file est vidée. Les seuils de fraîcheur de
  chaque route restent obligatoires : vider la file ne rend pas un vieux carnet frais.
- Les publications BBO sont différées pendant le rattrapage et reprennent avec
  l'état final des carnets. Cela évite aussi de solliciter les scanners sur les
  états intermédiaires d'un flux retardé.
- La file brute est bornée à **1 024 messages et 4 Mio par connexion**. Ces limites
  concernent les octets en file, pas toute la mémoire du processus. Un débordement,
  une corruption, ou un consommateur sans progression depuis plus de 5 secondes
  alors qu'un message attend lui-même plus de 5 secondes invalide le flux.
- Le contrôle des messages émis avec un retard persistant supérieur à 1 seconde
  est conservé. Les reconnexions effectives sont espacées d'au moins 350 ms entre
  workers, en complément du backoff existant.
- La reconstruction d'un snapshot et le rejeu de ses deltas s'effectuent **hors du
  verrou Depth commun**. La publication est atomique, après contrôle de génération,
  des versions et de l'absence de nouveaux deltas non rejoués. Un snapshot ancien
  ne peut pas remplacer un carnet prêt dont la version est plus avancée.
- Le panneau des flux distingue **actif**, **rattrapage**, **reconnexion** et montre
  l'attente de la file. Le statut expose `recent_catchups`, `recent_recoveries`,
  les limites de mémoire et les temps de traitement utiles au diagnostic.

Un carnet « prêt » désigne un carnet reconstruit et cohérent. Pendant le rattrapage,
il peut rester prêt tout en étant temporairement inutilisable pour une entrée.

## Politique privée conservée

La comparaison syntaxique avec V2.4.8 confirme 65 paramètres privés identiques,
ainsi que les fonctions de contrôle de fraîcheur LIVE/Shadow. Les limites
LIVE 20 USD / capital 200 USD / perte journalière 5 USD, les seuils d'entrée
50 / 100 / 50 ms, la protection des autres actifs, les frais, la politique
d'ordres et les fichiers de journaux sont conservés. Aucun rééquilibrage réel
ou réarmement nouveau n'est ajouté.

## Installation sur la branche mexc-spot-routes-v23

Décompresser le paquet et déposer son contenu à la racine du dépôt GitHub
`tiramysu92/mexc-gate-scanner`, sur `mexc-spot-routes-v23`.

- Remplacer `app.py` et `test_public_flow.py`.
- Ajouter `relancer_mexc_v249.py`, `verification_relance/test_restart.py` et ce README.
- Conserver les anciens requirements et le collecteur de timestamps. Aucune
  dépendance supplémentaire par rapport à V2.4.8 n'est requise.

Le dernier PID relevé sur le serveur est **110229**. Le lanceur vérifie le PID,
son propriétaire, le projet, le Python et la version en mémoire avant tout arrêt.
Il refuse de cibler un processus qui ne correspond pas. Si le scanner a été
redémarré entre-temps, utiliser son nouveau PID.

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
/home/ubuntu/mexc-venv/bin/python test_public_flow.py &&
/home/ubuntu/mexc-venv/bin/python relancer_mexc_v249.py --pid 110229
```

Le lanceur reprend l'environnement de la V2.4.8 en mémoire sans l'afficher.
Il suspend les entrées, attend l'absence d'ordre ou de confirmation en cours,
termine le processus précis, puis démarre la V2.4.9. Il conserve un STOP
préexistant et les causes du coupe-circuit. L'armement existant est renouvelé
uniquement s'il était déjà valide. Il ne démarre aucun second scanner tant que
l'ancien n'est pas terminé. En cas d'échec après suspension, celle-ci est conservée.

Résultat attendu : `2.4.9-local-catchup`, backend `upb`, journal
`scanner_v249_live.log`, nouveau PID dans `scanner_v249_live.pid`.

## Validation terminée localement

- 28 tests du flux public : décodage, versions ordonnées, timestamp original,
  rattrapage partiel/complet, 29 connexions ralenties, file bornée en octets,
  blocage des entrées avant acquisition du verrou, consommateur bloqué,
  retour d'un flux calme, races entre snapshot/deltas/reconnexion, cinq anciens
  signaux périmés, et paramètres privés conservés.
- 7 tests de relance avec des processus HTTP factices : environnement repris,
  ordre occupé, STOP préexistant, absence d'armement, échec de prévalidation ou
  de démarrage, différence de limites.
- Import avec les dépendances réelles et backend natif `upb` ; syntaxe JavaScript
  du tableau de bord vérifiée. L'interface compacte V2.4.8 est conservée.

Le passage `/proc`/pidfd des tests de relance reste simulé car le runtime local
virtualise les PID. Aucun ordre réel ni essai réseau MEXC n'est exécuté par ces tests.
Le correctif traite le mécanisme de reconnexion constaté ; le débit soutenu du
serveur doit encore être observé. Une file restant en rattrapage signale une
charge persistante et ne doit pas être interprétée comme un flux exploitable.

## Après la relance

Les connexions doivent rester établies et les carnets reconstruits progresser.
Avec un seul worker REST limité à environ quatre snapshots/seconde, 709 carnets
demandent au minimum environ trois minutes, auxquelles s'ajoutent réseau et reprises.
Un rattrapage bref peut apparaître sans détruire les carnets ; il doit se terminer.

Après dix minutes :

```bash
cd /home/ubuntu/mexc-gate-scanner &&
/home/ubuntu/mexc-venv/bin/python collecte_mexc_timestamps.py --recent-minutes 10
```

Le collecteur V2.4.8 conserve déjà tout le champ `public_flow` du statut et des
checkpoints : il inclut donc les nouveaux événements de rattrapage sans modification.

## Traçabilité

Base V2.4.8 : SHA-256
`19217cd4fd51e185e80676c9ce4cd104ba9b15b3aec30594da5d227eb827dcab`.

Cet historique concerne les flux publics. Les pertes réelles V2.4.6 et les
résultats Shadow restent dans leurs journaux existants ; ils ne sont ni effacés
ni recalculés par cette mise à jour.
