# V2.4.12 — reprise LIVE limitée

Cette version prépare une session réelle pour mesurer le résultat du bot après
l'incident CTO. Elle ne démontre pas sa rentabilité et ne prétend pas résoudre
l'annulation MARKET côté MEXC. CTO est exclu des nouvelles entrées par le lanceur.

## Ce que la commande autorise

- Le plafond existant reste au maximum 20 USDT par entrée, avec une seule route en cours.
- La session dure au plus **30 minutes à compter de la préparation**, démarrage inclus,
  ou **10 achats exécutés**, selon la première limite atteinte.
- **5 USDT de perte nette supplémentaire** sont explicitement alloués par le paramètre
  `--budget-perte-supplementaire 5`. Une valeur inférieure, strictement positive, est possible.
  Ce budget s'ajoute aux pertes déjà enregistrées. Ce n'est pas une remise à zéro.
- Au premier recours à une sortie de secours, les nouvelles entrées sont bloquées,
  même si cette sortie réussit. Les confirmations et sorties déjà engagées continuent.
- Les seuils de fraîcheur, le calcul à la taille abordable, la politique FOK et
  les protections de V2.4.11 sont conservés. Les positions existantes restent protégées.
- **Le seuil de 5 USDT déclenche un arrêt des nouvelles entrées. Il ne garantit pas
  une perte maximale : un actif impossible à revendre peut perdre davantage.**

Le résultat historique reste dans le journal et dans l'interface. Un second indicateur
affiche le PnL de la session, calculé à partir de la variation du total réalisé du
journal LIVE. Les changements de jour et les redémarrages ne remettent pas à zéro
le budget de cette session. Les gains ou pertes Shadow ne participent pas au calcul.

## Installation sur la branche mexc-spot-routes-v23

Ajouter **tout le contenu** de `MEXC_V2412_reprise_live.zip` à la racine du dépôt,
en remplaçant les fichiers du même nom. Tous les fichiers sont à plat, y compris
la fixture fictive et les tests. Garder les requirements existants : aucune nouvelle
dépendance externe n'est ajoutée.

Sur le KVM, exécuter :

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
/home/ubuntu/mexc-venv/bin/python verifier_v2412.py &&
/home/ubuntu/mexc-venv/bin/python reprendre_live_v2412.py --apply --budget-perte-supplementaire 5 --minutes 30
```

**La dernière ligne autorise effectivement les ordres réels et le budget
supplémentaire indiqué.** Les tests précédents ne réarment rien. Les `&&` empêchent
la reprise si la mise à jour, l'intégrité du paquet ou les tests échouent.
Utiliser `verifier_v2412.py` et `SHA256SUMS_V2412.txt` pour ce paquet ; les anciens
manifestes décrivent les anciennes versions d'app.py.

Pour un contrôle préalable seul, sans changer le processus, le circuit ou l'armement :

```bash
/home/ubuntu/mexc-venv/bin/python reprendre_live_v2412.py
```

## Déroulement

Le script trouve le scanner unique V2.4.11 ou V2.4.12 et conserve son environnement
en mémoire, sans afficher les clés. STOP doit déjà être présent. Il vérifie la
régularisation CTO, l'absence d'exposition ouverte et d'ordre non résolu. Il refuse
de lever une autre cause de circuit non examinée.

Après la fin des ordres et confirmations éventuels, il arrête uniquement ce processus
avec son identifiant stable, sauvegarde SQLite, puis enregistre une allocation et
l'acquittement des causes CTO. Les événements historiques, ordres, montants et pertes
sont conservés ; leur acquittement est audité dans `live_rearm_sessions_v2412`.

Le nouveau scanner démarre avec STOP. Le lanceur exige ensuite **30 secondes de
relevés stables** : toutes les connexions publiques présentes, au moins 90 % des carnets
prêts, aucun flux actif en rattrapage, files âgées d'au plus 100 ms et aucune nouvelle
reconnexion entre les relevés. Le WS privé, l'API et des routes prévalidées doivent être
disponibles. Les contrôles plus stricts de chaque route restent applicables avant un achat.
Ce contrôle dure au maximum trois minutes ; son échec conserve STOP et fournit un bilan.

L'armement est renouvelé et STOP est retiré en dernier. Un STOP modifié par l'utilisateur
pendant la procédure est conservé. Toute autre erreur de démarrage conserve l'arrêt.

## Surveillance et résultat

Garder le terminal ouvert pour voir, toutes les quinze secondes, les WS, les carnets,
les rattrapages, l'attente maximale des files actives, les reconnexions et le PnL de session.
Les relevés sont pris toutes les trois secondes. Le scanner impose lui-même la date
limite et les limites de budget/achats : elles ne dépendent pas du terminal de surveillance.

Ctrl+C demande la suspension des nouvelles entrées et un bilan anticipé. À la fin, le
script remet STOP, laisse finir les confirmations/sorties engagées et exporte
`bilan_live_session_…json.gz`. Il attend jusqu'à trente secondes pour cette dernière
observation et indique `settled: false` si une exécution n'est pas encore terminée.

Le bilan contient les compteurs cumulés de flux, les ordres, les commissions et les
observations de la session. Les maxima de file restent des maxima **échantillonnés**.
Les compteurs de rattrapage V2.4.10+ survivent au remplacement d'une connexion ; un
redémarrage du processus termine cette surveillance plutôt que de mélanger deux sessions.
Transmettre le bilan dans la conversation ; il contient des données de trading privées.

Pour décider de la suite, examiner le PnL net, le taux d'achats suivis d'une deuxième
jambe exécutée, les frais, les secours, les expositions restantes et les retards.
Dix achats favorables ne suffisent pas à démontrer une rentabilité durable. Aucun signal
admissible pendant trente minutes est un résultat possible ; ne pas assouplir les seuils
uniquement pour provoquer un trade. Les profits affichés par Shadow ne remplacent pas
les exécutions réelles. L'endpoint de test MEXC vérifie une requête sans l'envoyer au moteur
de matching : [documentation officielle](https://mexcdevelop.github.io/apidocs/spot_v3_en/#test-new-order).

## Vérification locale

Les **94 tests ont réussi localement**, dont les 66 contrôles V2.4.11 conservés et
28 contrôles de session/reprise. Le vérificateur contrôle les empreintes puis lance les six suites dans un répertoire
temporaire isolé : flux publics, récupération des sorties, comptabilité, ancien
redémarrage, allocation de session et nouvelle reprise. Les nouveaux contrôles couvrent
notamment la perte déjà consommée, l'expiration, minuit, les horloges, l'acquittement
limité à CTO, le refus d'un ordre incertain, les flux instables, le STOP opérateur,
les positions restantes et la collecte après suspension. Les processus de reprise sont
factices ; les tests n'envoient aucun ordre et ne prouvent pas un remplissage chez MEXC.
