# Validation de la candidate V3

Date de préparation : 15 septembre 2026.

- 70 tests réussis avec les connexions réseau interdites par le vérificateur.
- Intégration : flux Protobuf synthétique, reconstruction, décision et trois profils retardés, journaux et export final.
- JavaScript du tableau de bord : syntaxe vérifiée avec Node. Pas de vérification visuelle dans un navigateur disponible dans cet environnement.
- Pas de requête au compte MEXC, pas d’ordre réel, pas de réarmement ni modification du serveur.
- Microbenchmark CPU local : 43,648822 µs pour l’évaluation complète ; 17,164993 µs pour le contrôle d’un plan inchangé ; 2 000 itérations. Aucune latence réseau ou serveur ne peut en être déduite.
- Base V2.4.14 conservée : SHA-256 `47f953381159977b4ccf2a09eee3e03eadcbf45e4046a7ae0b0ba42ba9d5a102`.

Le paquet a également été extrait dans un dossier distinct : intégrité vérifiée et 70 tests réussis. Les tests ne dépendent pas de fichiers laissés dans l’espace de préparation.

Les tests ne prouvent pas un taux de remplissage, la capacité soutenue du serveur à l’univers complet ou une rentabilité réelle.
