---
title: 'Partie 4 : styles d’architecture'
sidebar_label: Présentation de la partie
partie: 4
---

Les parties précédentes ont décrit des briques (répartiteurs, caches, bases, files) et les mécanismes qui les font tenir ensemble malgré les pannes. Cette partie traite de la façon de les assembler en un système entier, et de ce que chaque assemblage coûte. Elle commence par le découpage d’une application, du monolithe aux microservices ([chapitre 42](monolithe-microservices.mdx)), puis passe aux architectures orientées événements, au serverless, aux cellules qui limitent le rayon d’une panne, au déploiement sur plusieurs régions, et aux architectures de traitement des données : traitements par lots, flux, entrepôts et lacs de données. Aucun style n’est le meilleur dans l’absolu ; chaque chapitre chiffre ce qu’il apporte et ce qu’il fait payer, pour qu’on puisse choisir selon le problème posé.
