# Réparation des tests V2.4.11

Ce complément s'applique à la V2.4.11 déjà récupérée dans le dépôt. Les sept erreurs de comptabilité provenaient du fichier de données de test absent ; le sous-dossier des tests de relance manquait également.

Tous les fichiers de ce ZIP se déposent directement à la racine du dépôt, sur la branche mexc-spot-routes-v23. Il n'y a aucun sous-dossier à téléverser. Remplacer test_regularisation.py, verifier_v2411.py et SHA256SUMS.txt ; ajouter les trois autres fichiers. Les anciens fichiers de test éventuels dans des sous-dossiers ne sont plus utilisés par le lanceur.

Le jeu de données est entièrement fictif. Les paramètres correspondants sont remplacés uniquement en mémoire dans les tests ; le script de régularisation réel et le code du bot sont conservés.

Le lanceur vérifie que tous les fichiers requis existent avant de démarrer. Il exécute ensuite les quatre suites depuis un dossier temporaire : les chemins de journal relatifs et le contrôle des processus restent isolés du scanner déjà actif. Aucun ordre réel n'est envoyé pendant les tests.

Après avoir ajouté les six fichiers sur GitHub, lancer sur le KVM :

```bash
cd /home/ubuntu/mexc-gate-scanner &&
git pull --ff-only &&
sha256sum -c SHA256SUMS.txt &&
/home/ubuntu/mexc-venv/bin/python verifier_v2411.py &&
/home/ubuntu/mexc-venv/bin/python relancer_mexc_v2411.py --regulariser-cto
```

Les tests doivent annoncer 66 réussites. Les messages de circuit émis pendant les scénarios de test sont simulés. Si une commande échoue, la suivante ne démarre pas.

La relance effective intervient uniquement après les tests. Elle conserve LIVE_STOP et le coupe-circuit, collecte le diagnostic complémentaire et applique la régularisation après arrêt du scanner. Renvoyer la sortie du lanceur et le fichier incident_cto_complet_....json.gz produit.
