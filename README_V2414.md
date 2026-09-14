# V2.4.14 — essai LIVE limité en achats, sans échéance horaire

Ce paquet met en œuvre l'accord du 14 septembre 2026 : remplacer l'essai expirant après trente minutes par un seul quota durable de dix achats. Il ne constitue ni une validation de rentabilité, ni une suppression des autres protections.

## Limites conservées

- Maximum **10 tentatives ayant réellement acquis l'actif intermédiaire**, et non dix simples signaux ou ordres annulés sans exécution. Le dixième achat peut terminer sa deuxième jambe ou sa sortie de secours ; seules les nouvelles entrées sont bloquées.
- Plafond actuel conservé, au maximum **20 USDT par opération**, une route à la fois.
- Arrêt des nouvelles entrées dès la **première sortie de secours**, même bénéficiaire, ou un PnL réalisé cumulé du quota atteignant **−5 USDT**. Les pertes historiques sont conservées, mais ne sont pas imputées une deuxième fois à la nouvelle allocation.
- **5 USDT est un seuil d'arrêt, pas une perte maximale garantie.** Une liquidation impossible peut laisser une exposition et provoquer une perte supérieure.
- CTO reste exclu. Les protections des avoirs préexistants, les circuits, les contrôles de fraîcheur et la logique des ordres/sorties ne sont pas assouplis.
- Aucune remise à zéro à minuit, après déconnexion, redémarrage ou nouvelle invocation du lanceur. Un quota terminé ou bloqué ne se renouvelle pas automatiquement.

Les anciennes politiques restent datées et expirables. Seule une politique explicitement marquée `trade_quota`, avec `expires_ts_ms: null`, n'a pas d'échéance. Un champ manquant ou une politique invalide bloque le LIVE.

## Alerte SKL examinée

Le dernier compte communiqué ne contenait plus que USD1, USDC et USDT. L'alerte mémorisée est précisément :

```text
reason: protected_asset_detected
error: protected_unknown_asset:SKL
attempt_id: null
ts_ms: 1789414003722
```

`--acquitter-skl` n'autorise l'acquittement que de cet événement, après vérification de soldes récents, sans actif non stable ni montant verrouillé, sans ordre/exposition non résolu et sans autre cause active. Un nouvel événement SKL, même avec le même texte, est refusé. L'absence d'identifiant numérique dans le statut mémoire n'est pas utilisée pour effacer tous les circuits : l'événement exact est retrouvé dans SQLite.

Le journal est sauvegardé avant toute modification. L'acquittement, son justificatif et la création éventuelle du quota sont transactionnels et réalisés **entre l'arrêt de l'ancien processus et le démarrage du nouveau**. Les ordres, les pertes, la régularisation CTO et les anciens événements ne sont pas supprimés. `LIVE_RESET_CIRCUIT=1` est refusé.

## Installation

Décompresser l'archive. Mettre **son contenu à la racine** de `tiramysu92/mexc-gate-scanner`, branche `mexc-spot-routes-v23`, en remplaçant les fichiers homonymes. Garder les bases, les fichiers requirements et les fichiers d'armement. Aucune nouvelle dépendance n'est nécessaire. Le fichier de fixture JSON est inclus à la racine ; ne pas créer de sous-dossier pour lui.

Dans le KVM :

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
/home/ubuntu/mexc-venv/bin/python verifier_v2414.py &&
/home/ubuntu/mexc-venv/bin/python reprendre_live_v2414.py --apply --budget-perte-supplementaire 5 --acquitter-skl
```

Le vérificateur utilise uniquement des bases temporaires et des processus HTTP factices. Il ne réarme pas le scanner et n'envoie aucun ordre réel. Les `&&` empêchent la reprise après une erreur de test. Après cette mise à jour, utiliser `verifier_v2414.py` : les anciennes empreintes de `app.py` ne correspondent plus à la nouvelle version.

Le lanceur exige STOP présent, un scanner unique V2.4.13 ou V2.4.14 et la session précédente enregistrée. Pour créer le premier quota, cette ancienne session ne doit avoir effectué aucun achat ni secours et son PnL doit être nul. Sinon, il demande un examen ; il n'alloue pas silencieusement un nouveau budget.

Il attend la fin des ordres et confirmations en cours, conserve l'environnement en mémoire, puis redémarre. Il peut attendre jusqu'à dix minutes la reconstruction des flux. Il exige trente secondes consécutives avec toutes les WS connectées, au moins 90 % des carnets prêts, aucune connexion en rattrapage et une attente de file inférieure ou égale à 100 ms. Ces contrôles ne remplacent pas les seuils stricts propres à chaque route. Les soldes et le journal sont vérifiés à nouveau avant le retrait de STOP.

Résultat attendu :

```text
LIVE autorisé : quota de 10 achats, sans échéance horaire. Budget et compteurs persistants.
```

Version affichée : `2.4.14-trade-quota-session`. Journal : `scanner_v2414_live.log`. L'interface indique « sans échéance horaire » et distingue les entrées autorisées des entrées bloquées.

## Surveillance et bilan

Garder le KVM ouvert pendant cet essai. Il affiche les WS, les carnets, les rattrapages, les reconnexions, les achats et le PnL. **Ctrl+C suspend les nouvelles entrées sans tuer le scanner ni interrompre une sortie engagée.** La fin du quota, un circuit ou une erreur de surveillance fait également conserver/créer STOP. Les signaux de fermeture du terminal et d'arrêt sont traités de la même manière lorsqu'ils parviennent au moniteur.

Le scanner impose lui-même les limites du quota, indépendamment de la surveillance. Une fermeture brutale de la machine ou un `SIGKILL` ne permet cependant pas de garantir l'écriture du bilan/STOP par le moniteur. Les compteurs et budgets restent persistants dans le journal.

Un fichier `bilan_live_quota_<id>_courant.json.gz` est actualisé environ chaque minute. Une fenêtre de 1 200 relevés au maximum est conservée en mémoire ; les compteurs restent comparés au début de la surveillance. Le journal SQL n'est pas purgé. Le bilan inclut les achats du quota, les 1 000 tentatives les plus récentes, leurs ordres/observations et les comptes par statut, avec indication d'une éventuelle troncature.

À l'arrêt de la surveillance, le programme attend jusqu'à trente secondes que la route engagée soit terminée, puis exporte `bilan_live_session_….json.gz`. `settled: false` signifie qu'il ne peut pas confirmer cette fin : ne pas réarmer. Envoyer le bilan dans la conversation pour examiner les exécutions et les pertes/gains nets, pas seulement le résultat Shadow.

Pour un export ponctuel en lecture seule depuis un autre terminal :

```bash
cd /home/ubuntu/mexc-gate-scanner &&
/home/ubuntu/mexc-venv/bin/python reprendre_live_v2414.py --bilan
```

Pour reprendre après une interruption volontaire, uniquement si le quota reste admissible et STOP est présent :

```bash
cd /home/ubuntu/mexc-gate-scanner &&
/home/ubuntu/mexc-venv/bin/python reprendre_live_v2414.py --apply
```

Cette commande **réutilise le même quota**. Elle ne remet ni le nombre d'achats, ni la perte, ni les secours à zéro. Si un contrôle échoue, conserver STOP et transmettre le message : ne pas contourner le garde-fou.

## Validation

Les suites couvrent la politique sans échéance, sa compatibilité avec les anciennes sessions datées, le dixième achat, le premier secours, le seuil de perte, le passage de minuit, le rechargement depuis SQLite et le refus de réinitialiser un quota bloqué. Elles exercent aussi la reprise avec processus factices, les soldes anciens ou résiduels, les limites modifiées, les échecs de démarrage, l'acquittement ciblé, les transactions annulées et les bilans bornés. Les suites existantes de flux publics, sorties, comptabilité et resynchronisation restent incluses.
