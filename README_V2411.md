# V2.4.11 — correction des défauts constatés sur CTO

Version : `2.4.11-guarded-exit-recovery`.
Base exacte : `app.py` V2.4.10, blob Git `7f550ddcfe48033cbb8f13b5c935199c75c3fce3`, vérifié sur la branche `mexc-spot-routes-v23` du dépôt `tiramysu92/mexc-gate-scanner`.

## Ce qui est démontré

Le 14 septembre 2026 à 05:58:23, heure de Paris, la route USDT → CTO → USD1 a acheté 19 801,66 CTO. La confirmation privée de l’achat a pris 69 ms depuis le début du POST. Au contrôle de la deuxième jambe, le carnet avait un âge local de 105 ms pour un plafond de 100 ms. Son âge MEXC était de 122 ms, sous son plafond de 150 ms. La vente vers USD1 a été bloquée avant envoi.

La vente MARKET de secours de 19 762,05 CTO a reçu le statut CANCELED avec zéro quantité exécutée. La différence de quantité vient de la réserve de frais de 0,20 % appliquée avant connaissance exacte de la commission d’achat ; cette réserve n’est pas une perte réalisée. Après réconciliation des frais, les 19 801,66 CTO restaient entièrement en position. Le circuit s’est ouvert pour residual_intermediate_asset.

Le bot n’a envoyé aucune autre sortie automatique. L’exposition est restée jusqu’à la vente manuelle sur le compte principal à 15:00:53, puis retour des USDT dans le sous-compte, selon l’utilisateur.

| Comptabilité de cette opération | USDT |
|---|---:|
| Achat, frais compris | 19,97005331664 |
| Vente manuelle brute | 7,28701088 |
| Frais de vente | 0,00364350 |
| Vente nette | 7,28336738 |
| Perte réalisée totale | −12,68668593664 |

L’annonce d’une baisse quotidienne d’environ 51 % est cohérente avec la volatilité observée mais ne constitue pas une explication technique de l’annulation du MARKET.

## Problèmes et corrections

### 1. Aucune marge de fraîcheur pour le délai d’achat

V2.4.10 contrôlait l’âge du carnet au moment de l’entrée, sans réserver le délai de confirmation de l’achat. Un carnet admissible pouvait donc dépasser le plafond de sortie pendant que l’achat s’exécutait.

V2.4.11 exige que l’âge actuel du carnet de sortie, augmenté du budget de confirmation et de préparation, reste sous les plafonds de deuxième jambe. Le budget vaut `max(75 ms, p95 des 200 dernières confirmations LIVE disponibles) + 10 ms`. Les confirmations sont restaurées depuis le journal au démarrage ; les appels TEST ne comptent pas. Sans historique, le budget est donc 85 ms. Le contrôle intervient avant admission, avant préparation de l’achat et juste avant POST.

Avec ce budget et le plafond local existant de 100 ms, il reste au plus 15 ms d’âge local admissible pour la sortie à l’entrée. C’est une restriction volontaire : elle réduit les admissions. Si les confirmations observées sont trop lentes, les nouvelles entrées peuvent être toutes bloquées. Les plafonds existants restent inchangés ; les 75/10 ms sont des marges initiales de prévention, pas une optimisation de rendement ni une garantie de fraîcheur future.

Le test CTO reproduit le manque de marge à partir des délais enregistrés (~31 ms d’âge estimé au POST + 85 ms de budget > 100 ms). Sans les carnets détaillés du serveur, il ne s’agit pas d’un replay complet du marché.

### 2. Une seule tentative de sortie de secours

V2.4.11 conserve la première sortie MARKET prévalidée et permet au maximum une seconde tentative immédiate. Conditions cumulatives :

- confirmation terminale REST du premier ordre, avec identité, statut et quantités identiques au résultat déjà enregistré ;
- aucun timeout ou résultat incertain ;
- commission connue si le premier ordre est partiellement exécuté ;
- quantité limitée au reliquat suivi et au solde libre, après réserve des tokens présents avant le trade ;
- nouvel identifiant d’ordre, préparation durable dans le journal, nouvelle soumission avant expiration de la fenêtre de 1 500 ms.

La fenêtre borne le démarrage d’une nouvelle tentative ; elle ne garantit pas une durée totale de sortie de 1 500 ms, car une requête déjà partie peut prendre plus de temps. Il n’y a pas de boucle de vente de fond après ces tentatives. Un solde externe ou un bag préexistant ne doit pas augmenter la quantité vendue.

Cette deuxième tentative peut échouer comme la première. Elle ne garantit pas une liquidation, et un MARKET ne garantit pas son prix. Elle reste inactive tant que le LIVE est suspendu.

### 3. Annulation sans exécution et exposition peu explicites

Le champ unwind_error pouvait rester vide quand le serveur confirmait une annulation sans vente. La nouvelle version conserve une erreur explicite avec le statut, la quantité exécutée et le reliquat. Une exposition persistante affiche un avertissement visible dans la synthèse LIVE : intervention requise, quantité du token, et fin des tentatives automatiques.

Cet avertissement est dans l’interface serveur ; ce n’est pas une notification envoyée au téléphone. Aucune notification externe n’est configurée par ce paquet.

### 4. Vente manuelle hors du journal du bot

Le script regulariser_cto.py reconnaît uniquement le trade documenté, contrôle les quantités, l’achat FILLED et la sortie CANCELED sans exécution. Il sauvegarde la base, conserve tous les ordres d’origine et les causes du circuit, écrit une trace de résolution manuelle, clôture l’exposition et comptabilise le produit net réel. Une seconde exécution ne double pas la perte. Un état différent entraîne un refus.

L’application compte désormais le statut manual_closed dans les résultats LIVE. Le transfert de retour en USDT n’est enregistré ni comme un nouveau profit ni comme un nouvel apport. Le script s’appuie sur les preuves fournies par l’utilisateur, pas sur un accès API au compte principal. Il ne change pas les soldes MEXC.

## Ce qui reste inconnu

La raison interne de l’annulation du MARKET CTO par MEXC n’est pas contenue dans le premier export. Ce correctif ne prétend pas l’avoir déterminée. Le collecteur complémentaire extrait les réponses d’ordre, les confirmations privées et les observations de carnets déjà conservées dans le journal. Ces données permettront de vérifier les états vus par le bot ; elles peuvent ne pas contenir le motif interne du moteur MEXC.

Si nécessaire, transmettre au support MEXC l’ordre de secours CTOUSDT `C02__728030797134475265027`, SELL MARKET, client `v247UWb545209d9058accda38e76`, créé le 14 septembre 2026 vers 03:58:24 UTC, quantité 19 762,05, annulé sans exécution. L’achat associé porte l’ID `C02__728030796866088960027`. Aucune demande au support n’a été envoyée par ce travail.

La validation /order/test vérifie un ordre sans l’envoyer au moteur d’appariement. Elle ne prouve donc pas qu’un MARKET sera exécuté : [documentation officielle MEXC](https://mexcdevelop.github.io/apidocs/spot_v3_en/#test-new-order). Les statuts privés distinguent bien annulé et exécuté : [états des ordres](https://mexcdevelop.github.io/apidocs/spot_v3_en/#spot-account-orders).

## Installation — nouvelles entrées maintenues suspendues

1. Décompresser le ZIP. Mettre son contenu à la racine du dépôt GitHub, branche mexc-spot-routes-v23. Conserver les sous-dossiers fixtures et verification_relance. Remplacer app.py et test_public_flow.py ; ajouter les autres fichiers. Aucun changement de dépendance par rapport à V2.4.10.
2. Sur le KVM, dans le répertoire habituel :

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
/home/ubuntu/mexc-venv/bin/python verifier_v2411.py &&
/home/ubuntu/mexc-venv/bin/python relancer_mexc_v2411.py --regulariser-cto
```

La relance détecte le processus V2.4.10 du projet et vérifie son identité. Elle précontrôle la régularisation, conserve le diagnostic complémentaire, crée ou conserve LIVE_STOP, attend la fin des opérations en cours, arrête ce processus, régularise CTO puis démarre V2.4.11. Elle garde LIVE_STOP et ne renouvelle pas le fichier d’armement. L’environnement, le journal et les paramètres existants sont conservés.

Le résultat attendu est une V2.4.11 active pour le scanner, mais des nouvelles entrées bloquées, une exposition CTO clôturée et les causes de circuit conservées. La perte apparaît dans les statistiques du 14 septembre ; un lancement ultérieur peut afficher le PnL du nouveau jour sans effacer cette perte historique.

Renvoyer la sortie du lanceur et le fichier `incident_cto_complet_....json.gz` créé dans le dossier du scanner. Le journal de lancement s’appelle scanner_v2411_live.log. Une sauvegarde `mexc_live_v246_avant_cto_....db` est conservée avant la régularisation.

Ne retire pas LIVE_STOP pour tester ce paquet. La reprise LIVE n’est pas incluse dans ces commandes. La raison de l’annulation MEXC et la validation de la nouvelle politique de sortie restent à traiter avant réarmement.

Le traitement des flux publics de V2.4.10 est conservé. L’amélioration observée dans la surveillance précédente ne suffit pas à établir leur stabilité prolongée : les reconnexions, les rattrapages et la file de scan restent à mesurer sur une durée plus longue, avec les nouvelles entrées suspendues.

## Vérifications

Le lanceur de tests verifier_v2411.py utilise le même Python pour toutes les suites. Les appels d’ordre et d’API dans les tests sont des réponses contrôlées ; aucun ordre réel n’est envoyé.

- 34 tests de flux publics : V2.4.10 conservée, ordre des deltas, rattrapage, générations et bornes.
- 16 tests de sortie : marge de fraîcheur, annulation puis exécution, annulations répétées, résultat inconnu, désaccord REST/WS, exécution partielle, commissions, protection des quantités et intégration complète avec journal SQLite.
- 7 tests de régularisation : sauvegarde, exactitude, idempotence, refus si état différent ou scanner actif, rollback après erreur SQL, intégration aux statistiques.
- 9 tests de relance sur processus factices : drain, conservation de l’environnement, STOP et armement inchangé, précontrôles, échec de lancement, régularisation entre arrêt et redémarrage.

Les 58 constantes LIVE préexistantes (dont les tailles et plafonds de fraîcheur) sont identiques à V2.4.10. Les seuils supplémentaires sont exposés dans le statut. Ces tests locaux ne démontrent ni rentabilité ni capacité de liquidation sur le moteur MEXC.
