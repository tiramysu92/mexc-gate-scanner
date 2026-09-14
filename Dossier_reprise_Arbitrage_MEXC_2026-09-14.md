# Arbitrage MEXC — dossier de reprise au 14 septembre 2026

## Portée et niveau de preuve

Ce document assure la continuité après saturation du fil « Analyse arbitrage v2.3.5 ». Il rassemble le contexte historique disponible, les rapports relus, le code V2.4.7 inspecté, les deux exports V2.4.6 ouverts et le dernier message utilisateur reproduit ci-dessous. Il ne constitue pas la transcription intégrale du fil : certains échanges intermédiaires et le dernier engagement exact de l’assistant restent introuvables.

Ne jamais prétendre qu’une analyse a continué ou est terminée sans résultat vérifiable. Les résultats historiques cités ci-dessous n’ont pas été recalculés dans cette reprise. Aucune connexion au serveur, modification de configuration, soumission d’ordre ou activation LIVE n’a été effectuée pendant cette reprise.

## Objectif et décisions à conserver

- Arbitrage spot uniquement, sans perpétuels ni futures, conformément à la contrainte halal exprimée par l’utilisateur.
- Objectif initial : créer du stablecoin, limiter l’exposition au token intermédiaire et automatiser progressivement scanner, exécution et rééquilibrage.
- Projet initial MEXC–Gate/MetaMask, puis priorité donnée aux routes sur MEXC stablecoin → token → autre stablecoin, à deux jambes.
- Le scanner trois jambes demeure un projet séparé. Ne pas le mélanger au bot deux jambes.
- Stablecoins suivis : USDT, USDC, USD1. L’utilité d’USD1 avait été discutée ; le code V2.4.7 le conserve. Ne pas le retirer sur la base d’une ancienne interrogation.
- Préserver les bags existants, travailler avec une allocation dédiée. Les tailles envisagées historiquement (250/500/1 000/2 000 USDT) ne sont pas les limites du micro-LIVE actuel.
- L’utilisateur veut comparer plusieurs seuils d’edge, tailles et distributions des stablecoins, y compris un capital inférieur à 2 000 USDT.
- Ne pas réintroduire une attente artificielle de confirmation de 100 ms : supprimée en V2.1. Les délais d’exécution simulés et les seuils d’âge sont des grandeurs distinctes.
- Historique de rééquilibrage : ne pas dépenser plus que les profits disponibles pour rééquilibrer ; des refus en cas de solde stablecoin nul avaient été discutés. Distinguer cette logique historique des paramètres Shadow ultérieurs et du rééquilibrage LIVE désactivé.
- Indicateurs demandés : opportunités par sens et par token, compteur par journée fixe, PnL net après frais/rééquilibrage, durée des opportunités, âges des carnets, latence réelle.
- Problèmes historiques : routes non clairement associées, nombre de routes figé à 74, données désynchronisées, resynchronisations Depth, files de scan/DB, serveur lent à recharger.
- Le compteur quotidien ne doit pas effacer une perte économique, une exposition ou une réservation encore ouverte.

## Chronologie reconstituée

| Période | État retrouvé | Limite |
|---|---|---|
| Fin août | Arbitrage RIO entre MEXC et MetaMask, transferts RIO suspendus ; gain exprimé en quantité de RIO ; lancement du projet MEXC–Gate | Contexte historique, pas un résultat du bot actuel |
| 1–3 septembre | Archives MEXC–Gate V2 puis Universe V3 à V3.4 | Existence des archives vérifiée, contenu non réaudité |
| 5–7 septembre | Scanner routes spot V1/V1.1 puis V2/V2.1/V2.2 ; séparation deux/trois jambes ; suppression de l’attente de 100 ms | Versions retrouvées, tous les échanges non récupérés |
| 8–10 septembre | Exports V2.3.1, V2.3.3, V2.3.4 ; travail de replay et correction comptable | Rapports du 10 septembre relus |
| 10 septembre | Référence V2.3.5 ; nouvel export étendu de 741 Mo non compressé | Ne pas attribuer les résultats de l’ancien export au nouvel export étendu |
| 10–11 septembre | Politique Depth Quality V2.3.6 et suite ; fichier app(4).py et export V2.3.8 retrouvés | Transitions et choix exacts intermédiaires non entièrement reconstitués |
| 11 septembre | Scanner trois jambes V3.0 séparé, port 8083 | Aucun résultat de rentabilité retrouvé |
| 12 septembre | Exports V2.4.2 avant V2.4.3 et couverture V2.4.3 | Fichiers identifiés, analyses de ces bases non refaites |
| 13 septembre | Deux exports de pertes V2.4.6, ouverts et vérifiés ; meta version 2.4.6a-hybrid-leg2-100ms | Les trois pertes sont réelles selon le journal, pas Shadow |
| 13 septembre, 20:50 UTC | MEXC_V247_EXECUTION_REVIEWED.py, version 2.4.7-sized-fresh-execution | Dernier livrable retrouvé ; déploiement effectif non vérifié |
| 14 septembre | Cinq relevés utilisateur avec âge local court et âge/skew MEXC très élevé | Pas encore d’export V2.4.7 contenant ces relevés |

## Anciennes simulations : résultats et limites

Sources directement relues : Rapport_simulations_MEXC.md et Rapport_V235_reference.md.

Replay V2.3.4 : 280 passages, 840 décisions, 26 880 captures de jambes Depth et 23 553 captures de paires stablecoins ; fenêtre du 9 septembre 2026 02:18:53 à 23:55:17 UTC, environ 21 h 36. Les 280 passages ne sont pas 280 stratégies indépendantes.

Ancien Shadow enregistré : FAST +23,19 ; TARGET −57,62 ; DEGRADED −86,04 USDT avant rééquilibrages. Comptabilité et fenêtres différentes : comparaison directe avec un portefeuille continu invalide.

Défaut comptable retrouvé : max(0, profit_bank - coût) pouvait effacer un compteur négatif. Référence corrigée : PnL signé distinct du budget de rééquilibrage.

Référence proposée V2.3.5 : 25 % des quantités, cap 200 USDT, spread ≤1 %, revente de secours estimée ≤2 %, edge net prudent ≥0,10 %. Avec capital 1 000 USDT : FAST +4,72 ; TARGET +5,08 ; DEGRADED +1,71 USDT. Contrôle de fin de période : +1,68 / +1,55 / +0,51. Résultats de simulation historiques, pas validation de gains réels ni résultat V2.4.7.

Les 300 ms discutées auparavant étaient l’horizon de capture V2.3.4, pas la latence FAST/TARGET/DEGRADED. Les captures ultérieures s’étendent à T0+5 000 ms.

Le rapport montrait l’intérêt de tester 500–1 000 USDT, mais le résultat dépendait fortement de la capture choisie et de la liquidité. Ne pas reprendre uniquement les scénarios positifs et oublier les stress défavorables.

## Configuration de référence du code V2.4.7

Paramètres par défaut du fichier, pas preuve des variables d’environnement effectivement utilisées sur le serveur.

| Élément | Valeur / comportement |
|---|---|
| Port deux jambes | 8081 |
| Base recherche | mexc_routes_v247.db |
| Journal TEST/Shadow privé | mexc_live_v247_test.db |
| Journal LIVE | mexc_live_v246.db conservé pour l’historique |
| Mode par défaut | shadow |
| Capital Shadow | 1 000 USDT |
| Plafond ordre Shadow | 350 USD |
| Plafond ordre LIVE | 20 USD |
| Capital LIVE | 200 USD |
| Seuil de perte journalière LIVE | 5 USD, déclencheur d’arrêt et non plafond de perte garanti |
| Concurrence LIVE | 1 |
| Frais supposés | 0,05 % par jambe, non interrogés sur le compte |
| Edge Shadow initial | 0,35 % |
| Plancher collecte replay | 0,30 % |
| Fraîcheur entrée LIVE | local ≤50 ms ; MEXC ≤100 ms ; skew local et MEXC ≤50 ms |
| Fraîcheur jambe 2 | local ≤100 ms ; MEXC ≤150 ms ; retard timestamp Depth L2 sur fill L1 ≤150 ms |
| Réserve de sécurité LIVE | 0,20 % au minimum selon formule du code ; ne pas l’assimiler à une commission réellement payée |
| Edge conservateur L2 | ≥0,15 % |
| Allocation cible Shadow | USDT 40 % / USDC 10 % / USD1 50 % |
| Rééquilibrage Shadow historique | contrôle 30 min, maximum 4 h, écart anticipé 15 points, minimum 3 points |
| Rééquilibrage BOT Shadow | désactivé par défaut dans V2.4.7 |
| Rééquilibrage réel | désactivé |
| Pause par crypto intermédiaire | 5 secondes |
| Entrée / sortie prévues V2.4.7 | LIMIT FOK ; anciennes validations MARKET incompatibles |
| Secours | MARKET prévalidé ; ne pas déclencher un second ordre si le résultat du précédent est incertain |
| WS privé | hybrid, confirmation terminale correspondante WS ou REST, réconciliation REST poursuivie |
| Armement | trois verrous, phrase spécifique V247 ; pas de remise à zéro automatique des causes historiques |

Profils historiques, exprimés en échéances depuis T0 (ce ne sont pas les deux durées à additionner) :

| Profil | Jambe 1 | Jambe 2 |
|---|---:|---:|
| FAST | 15 ms | 30 ms |
| TARGET | 25 ms | 50 ms |
| DEGRADED | 50 ms | 100 ms |

Six profils BOT : BOT_FAST, BOT_TARGET, BOT_DEGRADED, BOT_100_200, BOT_150_300, BOT_200_400. Distinguer les portefeuilles recherche historiques et les simulations BOT.

Qualité Depth : A emploie 50 % de la capacité robuste ; B+ 33 %, avec edge après réduction de moitié du BBO ≥0,75 %, edge sans niveau 1 ≥−2 %, couverture L1–L3 ≥×3. B−, C et D sont refusés par la politique. Pour le LIVE V2.4.7, la qualité est recalculée à la taille réellement abordable, frais/réserve compris, sous le plafond de 20 USD.

La V2.4.7 réexamine les nouvelles versions de carnet indépendamment du premier classement de recherche. Une mauvaise classe au T0 historique ne condamne pas définitivement un événement devenu ensuite admissible.

## Trois pertes V2.4.6 directement vérifiées

Base : mexc_v246_live_losses_20260913_133204.db.gz. Les trois lignes portent mode=live et six ordres MARKET sont FILLED.

| Route | Demande USD | Statut | PnL réalisé enregistré | Motif enregistré |
|---|---:|---|---:|---|
| USDC>SAGA>USDT | 20,00 | completed | −0,339909821 | Route achevée ; aucune erreur enregistrée |
| USDT>SKL>USDC | 10,01034888 | forced_unwind | −0,033223976 | Depth L2 ancien : 101 ms >100 ms |
| USDT>牛来>USD1 | 20,00 | forced_unwind | −0,084331359 | Edge après L1 : −0,7221 % <0,1500 % |

Total PnL réalisé enregistré : −0,457465156 USD. Total economic_pnl enregistré : −0,458134267 USD, résidus compris selon le modèle de cette base. Il s’agit de valeurs du journal, pas d’une réconciliation indépendante du compte MEXC.

Le circuit historique conserve consecutive_unwind_limit actif après les deux liquidations de secours. Ne pas effacer cette cause sur changement de version.

La cause détaillée de la perte SAGA n’est pas démontrée par ce seul examen. Ne pas en attribuer arbitrairement la cause à la latence, aux frais ou à un bug précis.

## Dernier message utilisateur et examen effectué

Seuils indiqués par l’utilisateur : entrée locale 50 ms ; MEXC 100 ms ; skew 50 ms. Horaires du 14 septembre 2026 en UTC+02:00.

| Heure | Route | Taille A USD | Edge normal (ratio) | Âge local / skew ms | Âge MEXC / skew ms |
|---|---|---:|---:|---:|---:|
| 01:35:34.028 | USD1>牛来>USDT | 51.920365682841414 | 0.003931319362337149 | 34 /14 | 123896 /123849 |
| 01:29:31.017 | USDC>RAY>USDT | 27.592411596749997 | 0.0052680359642478525 | 36 /30 | 181297 /83971 |
| 01:26:00.190 | USDT>TIA>USDC | 16.762997498749375 | 0.0044359184994524625 | 54 /4 | 539617 /539544 |
| 01:17:39.058 | USDC>GRVT>USDT | 350.0 | 0.012205246283265359 | 47 /11 | 527939 /527875 |
| 01:17:28.157 | USDC>GRVT>USDT | 350.0 | 0.01280136244715413 | 137 /43 | 526057 /525906 |

Les ratios sont reproduits tels quels. Dans le code, net/normal_edge intègre le modèle de frais ; ne pas les présenter comme des edges avant frais.

Les edges correspondent respectivement à environ 0,3931 %, 0,5268 %, 0,4436 %, 1,2205 %, 1,2801 %. Aucun n’est une preuve de gain exécuté.

Constats :
1. Les cinq dépassent âge ET skew MEXC de l’entrée LIVE. TIA et le second GRVT dépassent aussi l’âge local de 50 ms.
2. Les âges MEXC sont 123,896 /181,297 /539,617 /527,939 /526,057 secondes.
3. schedule_decision conserve les observations de recherche avec des limites locales plus larges (300 ms d’âge, 250 ms de skew par défaut). La classification Shadow historique applique d’autres seuils locaux (150/50 ms), sans appliquer dans cette sélection les seuils d’âge/skew MEXC du LIVE.
4. maybe_live_candidate applique _snapshot_freshness avant mise en file ; _fresh_live_signal et les contrôles ultérieurs revérifient la fraîcheur.
5. Un test isolé de la fonction _snapshot_freshness extraite par AST du code, sans import ni lancement du bot, a rejeté les cinq couples d’âges/skews. Le test utilise des timestamps synthétiques cohérents avec les agrégats ; il ne reconstitue pas les messages réseau originaux et ne prouve pas le comportement du serveur.
6. « Taille A 350 USD » est compatible avec le plafond Shadow. Ce n’est pas la preuve d’un ordre LIVE de 350 USD.
7. Le code associe send_ts et timestamp de réception lors d’un delta valide, ne rafraîchit pas les timestamps lors de la simple republication BBO, et marque un snapshot REST sans timestamp MEXC avec send_ts=0 jusqu’à un delta WS cohérent.
8. Un décalage d’horloge global seul ne suffit pas à expliquer les skews MEXC inter-jambes de plusieurs minutes : un même offset s’annule dans une différence entre deux timestamps.
9. Sous la formule du code, et si les deux timestamps ne sont pas futurs, âge maximal moins skew donne l’âge de l’autre jambe : 47 ms pour 牛来, 97 326 ms pour RAY, 73 ms pour TIA, 64 ms puis 151 ms pour GRVT. Cela oriente vers une asymétrie entre les jambes dans plusieurs relevés, sans identifier lequel des deux symboles est ancien.
10. Des messages reçus récemment peuvent porter un timestamp d’émission ancien. Les relevés seuls ne permettent pas de trancher entre retard de flux/traitement, association incorrecte de timestamps, relevés d’une version antérieure ou autre défaut.

## Point de reprise et informations encore manquantes

La première priorité est d’expliquer la fraîcheur mesurée dans les derniers relevés et de vérifier leur disposition réelle, avant toute conclusion de rentabilité ou modification des seuils.

À examiner dans le prochain export V2.4.7 :
- version du processus effectivement actif et paramètres réellement chargés ;
- provenance des cinq lignes : décisions recherche, Shadow, admissions BOT ou ordres ;
- diagnostics_v247, décisions et captures liées, compteurs candidate:route_exchange_depth_stale / route_depth_stale / route_exchange_depth_skew ;
- received_ts et send_ts séparés pour les deux symboles, versions, resynchronisations, transport et files ;
- live_admissions_v241 et journal réel, sans confondre signal historique et nouvelle admission à un autre timestamp.

Ne pas conclure à une défaillance du filtre LIVE uniquement parce qu’une observation existe dans la base recherche. Ne pas non plus conclure que le serveur filtre correctement uniquement parce que le fichier relu contient le filtre.

Les résultats finaux de l’analyse de l’export étendu de 741 Mo, de l’export V2.3.8 et les derniers échanges intermédiaires ne sont pas tous retrouvés. Ne pas substituer les anciens chiffres V2.3.5 à ces résultats manquants.

## Infrastructure et branche trois jambes

Contexte antérieur : serveur Ubuntu, dépôt tiramysu92/mexc-gate-scanner, répertoire /home/ubuntu/mexc-gate-scanner. OVH est mentionné historiquement, tandis qu’un prompt récent affiche ubuntu@ip-172-31-44-234 : adresse publique et hébergeur actuels non vérifiés. Ne pas inventer une URL à partir de cette adresse privée.

Trois jambes : /home/ubuntu/mexc_3leg_scanner ; /home/ubuntu/mexc_3leg_v300.db ; port 8083. Dernier relevé disponible : 8/8 WS, 152/152 Depth, âge 3 ms, 666 routes, zéro gaps/resync/queues/drops, zéro décisions/opportunités. C’est un état de collecte, pas une preuve de rendement.

## Inventaire des sources retrouvées

Cet inventaire permet de retrouver les fichiers lors d’une reprise ; leur existence ne signifie pas que leur contenu a été réanalysé.

| Fichier | Date UTC | Taille octets |
|---|---|---:|
| MEXC_V247_EXECUTION_REVIEWED.py | 2026-09-13 | 393542 |
| mexc_v246_three_losses_20260913_133142.db.gz | 2026-09-13 | 411166 |
| mexc_v246_live_losses_20260913_133204.db.gz | 2026-09-13 | 44914 |
| mexc_v238_analysis_export.db(1).gz | 2026-09-12 | 317003324 |
| mexc_v243_coverage_20260912_154107.db.gz | 2026-09-12 | 39553199 |
| mexc_v242_pre_v243_20260912_005957.db.gz | 2026-09-12 | 37118007 |
| mexc_v238_analysis_export.db.gz | 2026-09-11 | 317003324 |
| mexc-3leg-scanner-v3.0.zip | 2026-09-11 | 30690 |
| mexc_routes_v235.db | 2026-09-11 | 108224512 |
| mexc_scanner_v235_depth_quality_verified.zip | 2026-09-10 | 30955 |
| mexc_scanner_v235_depth_quality_verified.zip | 2026-09-10 | 30955 |
| mexc_v234_export_1800.db(1).gz | 2026-09-10 | 260107334 |
| mexc_v234_export_1800.db.gz | 2026-09-10 | 260107334 |
| Rapport_simulations_MEXC.md | 2026-09-10 | 9506 |
| Simulations_MEXC_reproductibles.zip | 2026-09-10 | 40374 |
| mexc_v234_export.db.gz | 2026-09-10 | 147096433 |
| mexc_v233_shadow_export.db.gz | 2026-09-09 | 185223666 |
| mexc_v231_export.db | 2026-09-08 | 93159424 |
| mexc_spot_2leg_scanner_v2_3_1_depth_strict.zip | 2026-09-07 | 22085 |
| mexc_spot_2leg_scanner_v2_3_depth_strict.zip | 2026-09-07 | 20831 |
| mexc_routes_v22_analysis.db | 2026-09-07 | 98648064 |
| mexc_spot_2leg_scanner_v2_2_topbook_localage.zip | 2026-09-07 | 17947 |
| mexc_spot_2leg_scanner_v2_2_topbook.zip | 2026-09-07 | 17921 |
| mexc_spot_2leg_scanner_v2_2_depth.zip | 2026-09-07 | 19297 |
| mexc_spot_2leg_scanner_v2_2_dual.zip | 2026-09-07 | 54547 |
| mexc_spot_2leg_scanner_v2_2.zip | 2026-09-07 | 16940 |
| mexc_routes_v21_export.db | 2026-09-07 | 41349120 |
| mexc_spot_2leg_scanner_v2_1.zip | 2026-09-06 | 14462 |
| mexc_routes_v2_final.db | 2026-09-06 | 140034048 |
| mexc_routes_v2_check.db | 2026-09-06 | 104923136 |
| mexc_spot_routes_scanner_v2.zip | 2026-09-06 | 14737 |
| mexc_routes_export.db | 2026-09-06 | 36864 |
| mexc_spot_routes_scanner_v1_1.zip | 2026-09-05 | 11882 |
| mexc_spot_routes_scanner_v1.zip | 2026-09-05 | 32166 |
| mexc_gate_scanner_v3_4.zip | 2026-09-03 | 12838 |
| mexc_gate_scanner_v3_3(1).zip | 2026-09-03 | 12390 |
| mexc_gate_scanner_v3_3.zip | 2026-09-02 | 12390 |
| mexc_gate_scanner_v3_2.zip | 2026-09-02 | 11716 |
| mexc_gate_scanner_v3_1.zip | 2026-09-02 | 10635 |
| mexc_gate_scanner_v3.zip | 2026-09-02 | 10078 |
| mexc_gate_scanner_v2.zip | 2026-09-01 | 2802 |
| mexc_gate_scanner.zip | 2026-08-27 | 4729 |
| Rapport_V235_reference.md | 2026-09-10 | 2114 |
| V235_reference_reproductible.zip | 2026-09-10 | 50717 |
| app(4).py | 2026-09-11 | 157631 |

Sources intégralement matérialisées dans cette reprise : code V247, deux exports de pertes V246, deux rapports Markdown. Les exports volumineux plus anciens ont été localisés, pas téléchargés ni rejoués à nouveau.



## Complément du diagnostic — poursuite du 14 septembre 2026

### Corrélation avec les pertes V2.4.6

Les tables decisions_v22 et ws_latency_v22 de l'export ciblé contiennent déjà l'asymétrie entre âge local et âge d'émission. Il ne s'agit pas seulement d'une différence dans la page web.

| Décision | Symbole ancien | Âge MEXC maximal au T0 | Âge local maximal au T0 | Réception moins émission, symbole ancien | Réception moins émission, autre symbole |
|---|---|---:|---:|---:|---:|
| USDC>SAGA>USDT | SAGAUSDT | 1796 ms | 7 ms | 1789 ms | 12 ms |
| USDT>SKL>USDC | SKLUSDT | 126570 ms | 22 ms | 126548 ms | 18 ms |
| USDT>牛来>USD1 | 牛来USDT | 50422 ms | 24 ms | 50405 ms | 18 ms |

Les différences réception−émission sont calculées directement avec les colonnes de chaque jambe ; un offset d'horloge éventuel reste inclus. Les autres jambes proches de quelques millisecondes et les grands skews inter-jambes rendent insuffisante l'explication par un décalage global seul.

Autre constat : sur les captures liées à ces décisions, les versions du côté ancien évoluent : 6 versions pour SAGAUSDT, 7 pour SKLUSDT, 17 pour 牛来USDT. Les timestamps d'émission progressent tout en restant décalés :
- SKLUSDT : émission 1789282951947→1789282955127 ; réception 1789283078495→1789283081918.
- 牛来USDT : émission 1789283072501→1789283077951 ; réception 1789283122906→1789283127902.

Aucun groupe (symbole, version, send_ts_ms) de ces captures ne présente plusieurs book_ts_ms distincts. Dans ce petit extrait, on ne voit donc pas la simple republication d'une même version qui lui attribuerait de nouvelles heures de réception. Cela n'exclut pas un autre défaut de mesure ou d'association.

Dans les mesures WS incluses dans l'export, SAGAUSDT atteint 52048 ms, SKLUSDT 130729 ms et 牛来USDT 51243 ms ; leurs autres jambes ont des valeurs proches de 10–26 ms dans les petits échantillons correspondants. Ces maxima portent sur les échantillons conservés, pas sur tout le marché ou toute la session.

### Ce qui est établi / non établi

Établi : des versions successives portent des timestamps anciens, et l'anomalie est déjà enregistrée au stade réception WS. La fraîcheur locale seule aurait laissé entrer les trois anciennes décisions ; le contrôle MEXC prévu en V2.4.7 les aurait exclues sur ces données.

Hypothèse principale à examiner : flux reçu/traité en retard sur certains symboles ou certaines connexions. Le decodeur horodate localement le message après décodage dans le callback : cette métrique ne mesure pas séparément l'arrivée réseau, l'attente dans les buffers et le traitement Python. On ne peut pas attribuer avec certitude le retard à MEXC, au réseau, à une file du client, ou à un défaut de timestamp sans davantage de mesures.

Le contrôle du schéma officiel Protobuf n'a pas pu être achevé via les pages externes accessibles. Aucune modification du décodage n'a été effectuée sur cette base.

Les trois pertes historiques ne permettent pas, à elles seules, d'affirmer que l'ancienneté du flux est l'unique cause économique de chacune. En particulier la perte SAGA nécessiterait une comparaison détaillée des prix d'exécution et du carnet pertinent.

### Collecte préparée

Fichier : collecte_mexc_timestamps.py, Python standard uniquement.

À placer dans /home/ubuntu/mexc-gate-scanner, puis lancer depuis ce répertoire :

    python3 collecte_mexc_timestamps.py

L'outil :
- lit en mode SQLite read-only les noms de bases V247/recherche, V247/TEST et V246/LIVE conservé ; accepte --db répété pour des chemins personnalisés ;
- prend la fenêtre 14 septembre 2026 01:10–01:45 UTC+02:00 pour les cinq relevés ;
- extrait les timestamps, versions, traces, diagnostics, admissions et tentatives des routes/symboles concernés ;
- relève la version en mémoire et les compteurs via GET local /api/status sur 8081, configurable par --port ;
- relève séparément version littérale et SHA256 d'app.py sur disque, sans l'importer ;
- ne lit pas les clés API, les variables d'environnement, les soldes ou les réponses privées brutes ;
- ne redémarre pas le scanner, ne modifie pas ses bases ou ses seuils, ne soumet aucun ordre ;
- crée une archive JSON ciblée mexc_diagnostic_timestamps_*.zip, sans écraser un fichier existant.

Chaque requête SQL est limitée en durée et en lignes. Les absences, erreurs et troncatures sont enregistrées explicitement. Les différentes tables ne constituent pas un snapshot atomique global. L'état HTTP courant ne doit pas être attribué rétrospectivement aux événements historiques.

Validation locale effectuée sur les deux exports V2.4.6 : les trois tentatives sont retrouvées dans chaque journal ; 282 lignes ciblées dans l'export recherche et 3 dans le journal LIVE ; aucune erreur de requête ni troncature. Le statut HTTP du serveur n'a pas été testé ici. Aucun nouveau résultat LIVE n'est encore disponible.

Prochain élément nécessaire : l'archive produite sur le serveur, pour vérifier le code actif, retrouver la disposition des cinq événements et comparer les timestamps par symbole. Si l'archive constate l'absence de données historiques, récupérer les bases/chemins réellement employés ou organiser une observation future limitée ; ne pas traiter une absence comme une preuve de rejet.


## Analyse de l'archive reçue à 03:14:45 heure de Paris — 14 septembre 2026

Source : mexc_diagnostic_timestamps_20260914_011445_302794.zip, diagnostic.json. Fenêtre historique 01:10–01:45 heure de Paris ; statut courant collecté à 03:14:45. Ne pas confondre les deux périodes.

### Version et intégrité

Le statut HTTP et la version littérale app.py donnent 2.4.7-sized-fresh-execution. SHA256 du fichier serveur : bf2f09e8980bc0830bb91c27fb42c5b5fa40c3ec528845058adeeafb0a0e21a8, identique au fichier MEXC_V247_EXECUTION_REVIEWED.py relu localement. Le hash prouve l'identité du fichier disque ; la version HTTP est cohérente avec lui sans constituer un hash du code en mémoire.

Au moment du statut : configured_mode=live, politique FILL_OR_KILL, circuit_open=false, offset d'horloge MEXC=+5 ms ; limites entrée locale 50 ms, MEXC 100 ms, skew local/MEXC 50 ms. Il ne faut pas présenter le bot comme configuré en Shadow ou le circuit historique comme encore ouvert à cet instant. Cette reprise n'a ni armé le bot ni modifié le circuit.

### Les cinq événements sont identifiés

Chacun correspond exactement à une décision enregistrée et à une ligne decision_quality_v235 de classe A avec eligible=1 et la taille A donnée par l'utilisateur. Ce sont les classements de recherche/Shadow.

Les requêtes non tronquées ne trouvent aucune admission dans live_admissions_v241 de la base recherche ni tentative live_attempts_v240 dans les trois bases collectées, sur les routes et la fenêtre ciblées. Les tables d'admissions manquantes dans les journaux privés ne sont pas une anomalie : leur copie est dans la base recherche. On peut conclure à l'absence d'admission/tentative enregistrée pour ces cinq événements dans cet export, pas à l'absence de tout trade sur tout le serveur ou toute la session.

Les compteurs de rejet sont agrégés, pas liés individuellement aux cinq identifiants. Ils corroborent le fonctionnement des contrôles, sans donner un motif journalisé par événement pour chacun de ces cinq signaux.

### Timestamps des jambes au T0

Différences réception locale moins émission MEXC en millisecondes, directement calculées depuis decisions_v22. L'offset d'horloge n'est pas ajouté dans ce tableau.

| Heure Paris | Route | Jambe 1 : différence ms | Jambe 2 : différence ms |
|---|---|---|---|
| 01:17:28 | USDC>GRVT>USDT | GRVTUSDC : 9 | GRVTUSDT : 525958 |
| 01:17:39 | USDC>GRVT>USDT | GRVTUSDC : 12 | GRVTUSDT : 527898 |
| 01:26:00 | USDT>TIA>USDC | TIAUSDT : 539563 | TIAUSDC : 15 |
| 01:29:31 | USDC>RAY>USDT | RAYUSDC : 97316 | RAYUSDT : 181257 |
| 01:35:34 | USD1>牛来>USDT | 牛来USD1 : 28 | 牛来USDT : 123863 |

GRVTUSDT et TIAUSDT sont les jambes vieilles d'environ neuf minutes. Pour 牛来, il s'agit de 牛来USDT. Pour RAY, les deux jambes sont anciennes, respectivement environ 97 et 181 secondes.

Les versions de carnet évoluent durant les cinq secondes de captures, tout en conservant ce retard important. Cela exclut comme explication suffisante un simple carnet sans nouvelle mise à jour. Un offset global de quelques millisecondes ne peut expliquer ces différences entre jambes.

### Retard croissant et remises à niveau

Mesures transport_ms de ws_latency_v22 ; proximité temporelle avec depth_sync_events, sans affirmer que la resynchronisation REST seule est la cause de la baisse :

| Symbole | Avant remise à niveau | Après remise à niveau | Puis |
|---|---|---|---|
| GRVTUSDT | 01:17:55 : 534298 ms | 01:18:28 : 34 ms | 01:18:59 : 10739 ms ; 01:19:29 : 23459 ms |
| 牛来USDT | 01:24:42 : 420033 ms | 01:25:12 : 7480 ms | 01:25:42 : 16494 ms ; 01:26:12 : 25369 ms |
| SAGAUSDC | 01:37:04 : 582140 ms | 01:37:35 : 190 ms | 01:38:05 : 5683 ms ; 01:38:35 : 11727 ms |
| TIAUSDT | 01:37:06 : 582553 ms | 01:37:36 : 492 ms | 01:38:10 : 5842 ms ; 01:38:40 : 13070 ms |

Les baisses GRVT commencent avant les événements READY, parfois dès avant NOT_READY. Cela est compatible avec une reprise du flux courant entraînant un saut de version et la resynchronisation, plutôt qu'une preuve que REST vide la file WebSocket.

Hypothèse fortement soutenue : accumulation de messages anciens sur certains flux, avec retours intermittents aux messages récents. Le point d'accumulation n'est pas isolé : amont MEXC, buffers réseau/client, lecture/traitement Python. Il manque des mesures par connexion et une comparaison avec un client public indépendant pour trancher. Ne pas annoncer une cause CPU/MEXC certaine ou un correctif démontré.

### Étendue et charge observée

Dans cet export ciblé uniquement : 49 décisions de recherche, toutes avec âge MEXC >100 ms ; 6 classements A éligibles, 1 A refusé par une autre condition, 2 B−, 40 D. Ce n'est pas la statistique globale de toutes les routes.

Les 35 diagnostics par minute montrent scan_queue entre 108 et 240 (médiane 161), db_queue entre 1 et 2 (médiane 1), sans hausse des compteurs de drops (zéro sur les deux bornes). Les déconnexions cumulées passent de 9 à 18 entre les diagnostics de 01:10:30 et 01:44:31. Cela ne prouve pas l'absence de contention CPU/locks ; cela ne montre pas non plus une file d'écriture DB massivement accumulée.

Au checkpoint 01:30:30, sur 709 carnets avec timestamps utilisables, 465 ont une différence réception−émission >1 seconde et 224 >60 secondes. La mesure porte sur le dernier message conservé pour chaque carnet, pas sur un paquet simultané pour tous. Le problème dépasse les seuls cinq exemples.

Dans le statut courant à 03:14:45 : 29/29 WS, 709 Depth prêts, scan_queue=76, db_queue=0, 30 déconnexions sur 60 minutes. Connecté et prêt signifient transport ouvert et carnet reconstruit, pas fraîcheur suffisante pour arbitrer. Le p95 d'âge local du carnet (75405 ms) est distinct du retard réception−émission et peut aussi refléter des marchés calmes.

### Entonnoir et conséquences

Au statut courant, compteurs cumulés depuis démarrage :
- 65563 observations rejetées pour âge local ;
- 31123 pour âge MEXC ;
- 38 observations ont franchi le premier contrôle de fraîcheur ;
- 25 rejets gateway:projected_leg2_edge sont enregistrés.

Ces nombres ne sont pas des trades ni tous des événements uniques. Les contrôles peuvent se répéter, donc ne pas calculer un taux de conversion 25/38 comme si les compteurs comptaient exactement les mêmes unités. Un signal frais peut ensuite échouer sur l'edge conservateur ou une nouvelle vérification.

L'edge et la classe A des cinq relevés comparent des carnets décalés dans le temps ; ils ne démontrent pas un arbitrage réalisable à cet instant. Le portefeuille de recherche historique peut admettre cette qualité sous ses filtres locaux plus larges. Son PnL est donc non validant pour le LIVE dans ces conditions. Les résultats agrégés de PnL Shadow de cette fenêtre ne sont pas inclus dans l'archive : pas de recalcul de leur valeur ici.

### Suite technique justifiée

Conserver les seuils de fraîcheur et les protections FOK. Ne pas tenter de rendre les cinq signaux admissibles en augmentant les âges acceptés.

Priorité : isoler la cause de l'accumulation par connexion (débit entrant, durée du callback, attente de locks, horodatage dès l'entrée du callback, comparaison d'un petit abonnement indépendant). L'ajout d'une récupération des connexions qui prennent durablement du retard doit être borné et déclencher une reconstruction cohérente du carnet ; supprimer arbitrairement des deltas casserait la continuité des versions. Une reconnexion seule peut ne fournir qu'une amélioration temporaire si la cause de la croissance du retard demeure.

En parallèle, distinguer clairement dans l'affichage et les métriques : qualité économique A/B+, validité temporelle MEXC, admission privée et exécution réelle. Préserver les observations rejetées pour le diagnostic, sans les présenter comme des opportunités exécutables.

Aucun patch de production, changement de seuil, redémarrage ni ordre n'a été effectué au cours de cette analyse. L'archive permet de conclure sur les cinq signaux ; elle ne permet pas encore de déclarer la cause du retard corrigée.


## V2.4.8 préparée — correctif des flux et interface compacte

Demande utilisateur : « donc on fait quoi ? trouve la solution », puis « optimise aussi l’interface du serveur, par exemple les shadow et les simul latence bot doivent prendre bcp moins de place ».

État : code et paquet préparés localement sur la base V2.4.7 dont le hash est confirmé ci-dessus. Aucun dépôt GitHub ni serveur distant n’a été modifié par cette préparation. Aucun redémarrage, réarmement ou ordre réel n’a été effectué. Le correctif n’est pas encore démontré en exploitation.

Livraison : `MEXC_V248_correctif.zip`, contenant `app.py` et les auxiliaires. Une copie autonome est nommée `MEXC_V248_PUBLIC_FLOW.py` ; elle doit devenir `app.py` dans le dépôt. Version en mémoire attendue : `2.4.8-bounded-public-flow`.

### Changements des flux publics

- Décodage Protobuf natif (`upb` ou `cpp`) obligatoire au démarrage ; seule nouvelle dépendance, `protobuf==7.36.1`. Champs recoupés avec les schémas officiels `mexcdevelop/websocket-proto` et API `GetMessageClass` de Protobuf. Le décodage privé n’est pas remplacé.
- Horodatage à l’entrée du callback, lecture d’en-tête minimale et file bornée à 256 messages par connexion. Décodage complet et application dans un consommateur séparé.
- Tous les deltas sont appliqués dans l’ordre. Publication BBO regroupée par symbole sur de petits lots de 32 messages maximum, cible 2 ms. Cache du meilleur bid/ask pour éviter de rescanner tout le carnet à chaque delta, sans suppression de profondeur.
- Récupération si file pleine, attente en file supérieure à 100 ms ou retard réception–émission supérieur à 1 000 ms persistant sur trois messages d’un même symbole pendant au moins 200 ms. Un message frais d’un autre symbole ne réinitialise pas cette détection.
- Invalidation immédiate de la disponibilité avant toute attente de verrou, fermeture puis reconstruction REST + WS cohérents. Génération par connexion pour bloquer les anciens deltas, nettoyages ou snapshots REST après reconnexion. Backoff existant conservé.
- REST sans timestamp d’émission MEXC reste non admissible jusqu’au delta WS cohérent. Les seuils d’entrée LIVE demeurent local 50 / MEXC 100 / skew 50 ms ; les seuils de récupération ne sont pas des seuils d’admission.
- Les nouvelles observations de recherche gardent leur classe économique A/B+ et leur taille, mais une donnée MEXC périmée rend `eligible=false` avec motif. Les anciens résultats Shadow ne sont ni effacés ni requalifiés rétroactivement.
- Mesures par connexion dans `public_flow` du statut HTTP et des diagnostics persistés : files, débits, retards, coûts de callback/décodage/application, attente de verrou, récupérations.

Conservés : univers 2-leg, abonnements 10 ms, exécution privée FOK, contrôles L2, paramètres 20/200/5 USD, frais/réserve, chemins des bases et phrase d’armement existante `ENABLE MEXC V247 LIVE 20 USD`. Un lanceur déjà armé LIVE demeure configuré ainsi lors du redémarrage ; la préparation n’a pas elle-même armé le bot.

### Interface demandée par l’utilisateur

Deux tableaux compacts : 3 Shadows de recherche et 6 simulations BOT, côte à côte sur écran large. Colonnes de synthèse : profil, délais L1/L2, PnL, trades, gagnés/perdus et NAV. Soldes, coûts et compteurs supplémentaires dépliables. Les détails ouverts restent ouverts pendant les rafraîchissements. La synthèse LIVE et le résumé des flux restent visibles ; les longs panneaux techniques sont repliés par défaut. Sur mobile, les tableaux s’empilent et la NAV reste accessible dans les détails.

Les délais de recherche restent référencés à T0, ceux des Shadows BOT à leur admission fraîche ; ne pas les additionner entre jambes.

### Vérifications et limites

17 tests hors ligne passent, avec le backend Protobuf natif réel. Ils couvrent notamment les cinq relevés de l’utilisateur, les versions consécutives/manquantes, les files pleines/anciennes, les retards persistants, la conservation des timestamps, les courses de reconnexion/REST, le blocage avant verrou et un flux sain exécuté avec threads. Le cache bid/ask est confronté à 2 000 modifications aléatoires et à une référence sans cache.

Import avec les vraies dépendances et compilation Python vérifiés. Syntaxe et exécution du rafraîchissement JavaScript vérifiées avec un DOM de test, dont persistance des détails ouverts et profils absents. Pas de capture visuelle d’un navigateur réel ni de validation sur le navigateur du serveur.

Microbenchmark local synthétique, 25 niveaux par côté : décodage Python ~87,5 µs contre natif ~17,5 µs, environ 5× ; 100 niveaux : ~329,1 contre ~65,6 µs. Ce n’est pas un gain mesuré sur le débit du scanner entier ni sur ses ordres. Les tests ne démontrent pas que l’amont MEXC ou la capacité du serveur ne constitue plus une limite.

### Installation et étape suivante

Mettre le contenu du ZIP à la racine de `tiramysu92/mexc-gate-scanner`, en remplaçant `app.py` et le collecteur. Sur le serveur, dans le même environnement Python que le scanner :

```bash
cd /home/ubuntu/mexc-gate-scanner
git pull --ff-only
python3 -m pip install -r requirements_v248.txt
python3 test_public_flow.py
```

Puis utiliser le lanceur habituel pour redémarrer l’unique scanner. La commande exacte de service/nohup et le chemin du venv ne sont pas établis par les données récupérées ; ne pas inventer une commande d’arrêt ou un nouveau lanceur qui perdrait ses paramètres.

Le collecteur mis à jour accepte `--recent-minutes` entre 1 et 60 et récupère `public_flow`. Après dix minutes d’observation de V2.4.8 :

```bash
python3 collecte_mexc_timestamps.py --recent-minutes 10
```

Comparer alors le retard des messages actifs, les files, la disponibilité des carnets et les récupérations. Des reconnexions en boucle ne constituent pas une correction validée. Si le retard persiste, les nouvelles mesures permettront de localiser la limite restante ; une comparaison avec un client public indépendant demeure utile pour séparer transport/amont et traitement local.
