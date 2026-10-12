# Consignes du cours (source de vérité)

Demande de Romial du 2026-10-11, complétée par ses décisions sur le plan (même jour). Le plan
détaillé validé est dans `PLAN.md`.

## Public et objectif

Cours complet de system design en français, pour un public qui sait programmer mais n'a jamais
conçu de système distribué. À la fin, l'apprenant conçoit un système réel, justifie chaque choix
technique et réussit un entretien de system design. Voix : ingénieur senior qui enseigne.

## Façon de travailler

1. Un seul chapitre par réponse, dans l'ordre du plan ; « suite » = chapitre suivant.
2. Chapitre trop long : le couper en sous-parties et dire où l'on s'arrête.
3. Renvoyer aux chapitres précédents par leur numéro plutôt que réexpliquer ; chaque renvoi « ch. N » est un lien vers le chapitre (outil `outils/liens_chapitres.py`, à relancer après chaque chapitre pour relier les renvois faits avant sa rédaction). Les scripts d’expérience sont cités par un lien vers le dépôt GitHub (décisions du 2026-10-12).

## Structure de chaque chapitre (parties 0 à 5)

- Problème de départ : facultatif (décision du 2026-10-11), à mettre quand il éclaire.
- Intuition : plus d’encadré « analogie » systématique depuis le ch. 10 (décision du 2026-10-11) ; une image de la vie courante seulement si elle est nécessaire, dans la prose, avec sa limite.
- Explication technique : définitions précises, fonctionnement pas à pas.
- Au moins un schéma TikZ, suivi de 2 ou 3 phrases qui disent comment le lire. Registre
  `diagramme.sty` (architectures à icônes) ou `papier.sty` (mécanismes, chronogrammes, matrices)
  selon le visuel ; les couleurs suivent le thème clair ou sombre du site.
- Ordres de grandeur : chiffres réels et un calcul d'estimation détaillé ligne par ligne.
- Compromis : tableau comparatif quand il y a plusieurs options.
- Exemple réel : seulement si le fait est sûr et sourcé, sinon rien.
- Erreurs fréquentes des débutants et en entretien.
- Exercices : 3 questions de compréhension, 1 exercice de conception ; corrigés détaillés à la
  fin du chapitre.
- Fiche de révision : 5 à 10 lignes.

Parties 6 (études de cas) : la démarche du ch. 1 de bout en bout remplace « intuition +
explication » ; estimation chiffrée, schéma d'architecture, modèle de données, API, au moins deux
approfondissements. Partie 7 (LLD) : diagramme de classes et code Python à la place des ordres
de grandeur. Code des exemples : Python.

## Règles de qualité

- Exactitude d'abord : aucun chiffre, aucune anecdote d'entreprise, aucune référence inventés.
- Chaque notion avec ses compromis ; aucune technologie « la meilleure » dans l'absolu.
- Progression du simple au complexe : pas de notion utilisée avant d'être expliquée.
- Terme anglais entre parenthèses à la première occurrence : « mise en cache (caching) ».
- Pas de remplissage.
- Règles d'écriture locales : `~/.claude/regles/ecriture-humaine/SKILL.md` (pas de tiret
  cadratin, pas de tics de texte généré, sources citées en notes).

## Publication

Docusaurus, design propre au cours, publié comme container_k8s : dépôt GitHub, GitHub Pages par
workflow Actions, figures SVG versionnées (pas de LaTeX en CI).

## Annexes à produire à la fin

Glossaire français-anglais ; tableau des chiffres de référence ; grille d'auto-évaluation pour
l'entretien ; lectures pour aller plus loin (références réelles uniquement).
