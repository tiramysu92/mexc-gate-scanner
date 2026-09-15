# MEXC V3 — 3.0.0-candidate.1

Scanner spot à deux jambes, moteur commun de décision et d’exécution, simulations retardées et journal indépendant de V2.

**Le démarrage par défaut est l’observation publique.** Cette livraison prépare la V3 ; aucun déploiement ni ordre réel n’a été effectué pour la valider. Les tests couvrent des échanges factices et les contrats des adaptateurs. La tenue des flux et les réponses réelles MEXC restent à mesurer sur le serveur.

Lire d’abord `PROPOSITION_V3.md`, puis `docs/DECISIONS_ET_REX.md`. Les deux analyses antérieures sont conservées dans `docs/historique/` comme références datées.

## Nouvelle branche et installation séparée

Branche proposée : `mexc-spot-2leg-v3`. Téléverser **tout le contenu du paquet**, notamment les dossiers `mexc_v3`, `tests` et `docs`. Le vérificateur contrôle également ces fichiers ; copier seulement `app.py` ne suffit pas.

Créer un checkout distinct sur le serveur, après avoir mis le paquet sur cette nouvelle branche :

```bash
cd /home/ubuntu &&
git clone --single-branch --branch mexc-spot-2leg-v3 https://github.com/tiramysu92/mexc-gate-scanner.git mexc-gate-scanner-v3 &&
cd /home/ubuntu/mexc-gate-scanner-v3 &&
/home/ubuntu/mexc-venv/bin/python -m venv /home/ubuntu/mexc-v3-venv &&
/home/ubuntu/mexc-v3-venv/bin/python -m pip install -r requirements.txt &&
/home/ubuntu/mexc-v3-venv/bin/python verifier_v3.py
```

Ces commandes créent un environnement V3 distinct et ne relancent pas V2. Le bloc s’arrête si une étape échoue. Aucun fichier `.env`, clé API, journal réel ou fichier d’armement n’est inclus dans le paquet.

## Démonstration hors ligne

```bash
/home/ubuntu/mexc-v3-venv/bin/python app.py --mode demo --data-dir data_demo
```

Elle utilise un marché fictif `DEMO`, une horloge déterministe et le même coordinateur que le mode réel. Son résultat est explicitement synthétique. Réutiliser `data_demo` conserve le journal ; pour une autre démonstration indépendante, choisir un autre dossier.

## Observation sur MEXC

```bash
/home/ubuntu/mexc-v3-venv/bin/python app.py --mode observe --data-dir data_v3 --host 127.0.0.1 --port 8083
```

Ce mode utilise uniquement les flux publics et ne charge aucune clé API. Il lance trois portefeuilles simulés indépendants et le tableau de bord sur `http://127.0.0.1:8083`. Pour une consultation distante, utiliser un tunnel SSH vers le port 8083. L’option `--host` est explicite si l’environnement dispose déjà d’un accès protégé à son interface serveur.

Pour le laisser tourner après la fermeture du terminal :

```bash
nohup /home/ubuntu/mexc-v3-venv/bin/python -u app.py --mode observe --data-dir data_v3 > scanner_v3_observe.log 2>&1 &
```

Les flux V2 et V3 consomment chacun des connexions et du CPU : mesurer V3 seule pour juger sa capacité sur le serveur. Cette livraison ne termine pas automatiquement V2.

## Surveillance indépendante

```bash
/home/ubuntu/mexc-v3-venv/bin/python surveiller_v3.py --minutes 10 --interval 5
```

Le relevé affiche les connexions, les carnets, les rattrapages, l’attente et les reconnexions supplémentaires. Un rapport compressé est conservé même après `Ctrl+C`. Le surveillant ne crée ni ne supprime `STOP`, ne modifie aucun quota et ne commande aucune sortie. Le scanner reste autonome.

Le scanner écrit aussi `data_v3/last_report.json.gz` lors de son arrêt normal. Les journaux de chaque profil sont persistants. Les résultats et compteurs d’un profil ne sont pas remis à zéro au redémarrage.

## Mode réel inclus, distinct de cette validation

L’adaptateur réel est implémenté : REST signé, ordres FOK, notifications privées, frais privés et rapprochement REST. Il n’est pas activé par les commandes précédentes.

`docs/EXPLOITATION_LIVE.md` décrit l’initialisation explicite, l’import en lecture seule de l’historique V2, le fichier d’autorisation, les contrôles persistants et la procédure de diagnostic d’un ordre incertain. Le fichier d’arrêt bloque les nouveaux achats ; une sortie déjà engagée peut se terminer.

## Outils du paquet

| Fichier | Fonction |
|---|---|
| `app.py` | Observation, démonstration ou démarrage réel explicitement configuré |
| `verifier_v3.py` | Intégrité et tests avec le réseau interdit |
| `surveiller_v3.py` | Relevé indépendant, sans effet sur l’armement |
| `reconcilier_v3.py` | Requête des ordres incertains par identifiant durable, sans POST d’ordre |
| `benchmark_v3.py` | Mesure CPU locale sur un carnet fictif ; pas une latence LIVE |

Python requis : 3.12 ou supérieur, sous Linux. Le verrou de processus du journal utilise `fcntl`.
