# Essai LIVE sur V2.4.13

Ce complément ajoute un lanceur et ses tests. Le scanner reste en V2.4.13 et ses seuils d'entrée restent inchangés. La session précédente, expirée, reste dans le journal. Le lancement avec --apply crée explicitement une nouvelle session limitée après redémarrage.

## Limites

- Plafond existant conservé, au maximum 20 USDT par opération, une route à la fois.
- Dix opérations ayant acquis l'actif intermédiaire au maximum.
- Échéance de trente minutes à partir de la création de session, reconstruction initiale comprise.
- Arrêt des nouvelles entrées dès la première sortie de secours, même bénéficiaire, ou lorsque le PnL réalisé de session atteint -5 USDT.
- Le seuil de 5 USDT ne garantit pas une perte maximale : une sortie qui ne s'exécute pas peut laisser une exposition plus importante.
- CTO exclu. Protection des avoirs préexistants et comptabilité historique conservées.
- Aucun circuit actif n'est acquitté par ce lanceur. Un circuit ou une exposition non résolue bloque la reprise.

## Lancer depuis le KVM

Ajouter les trois fichiers de cette archive à la racine du dépôt, branche mexc-spot-routes-v23. La V2.4.13 complète doit déjà être installée.

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
sha256sum -c SHA256SUMS_V2413.txt &&
/home/ubuntu/mexc-venv/bin/python test_reprise_v2413.py &&
/home/ubuntu/mexc-venv/bin/python reprendre_live_v2413.py --apply --budget-perte-supplementaire 5 --minutes 30
```

Le lanceur conserve STOP, attend la fin des ordres et confirmations, sauvegarde le journal, crée la nouvelle session puis relance le scanner avec son environnement. Il peut attendre jusqu'à dix minutes la reconstruction initiale ; cela ne prolonge pas l'échéance de session. Il exige ensuite trente secondes de stabilité, toutes les connexions actives, au moins 90 % des carnets prêts, aucune file en rattrapage et des files de moins de 100 ms. Chaque route doit toujours passer ses propres contrôles de fraîcheur avant un ordre.

La ligne « LIVE autorisé pour cette session » confirme le retrait de STOP. Rester devant le KVM pendant cet essai. Ctrl+C suspend les nouvelles entrées ; les confirmations et sorties déjà engagées continuent. L'échéance et les limites sont également appliquées dans le scanner, indépendamment du moniteur. Un échec de préparation conserve STOP.

Le journal du processus est scanner_v2413_live.log. Le bilan bilan_live_session_….json.gz contient les observations, les tentatives, les ordres et le PnL ; l'envoyer dans la conversation à la fin. Le journal SQL de session est limité à 1 000 tentatives et indique toute troncature. Le programme refuse la reprise si plusieurs scanners du projet sont détectés.

Les 14 tests exercent des processus et des journaux factices, sans requête de trading réelle. Ils couvrent notamment l'expiration de l'ancienne session, la préservation des pertes, les flux non prêts, un circuit actif, un ordre en cours, une version différente et les interruptions.

La validation d'ordre API seule ne vérifie pas une exécution : MEXC indique que /api/v3/order/test ne transmet pas l'ordre au moteur d'appariement ([documentation officielle](https://mexcdevelop.github.io/apidocs/spot_v3_en/#test-new-order)). Ce lancement vise à mesurer les exécutions et leur résultat réel ; la rentabilité n'est pas encore établie.
