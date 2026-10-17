---
title: 'Partie 3 : systèmes distribués'
sidebar_label: Présentation de la partie
partie: 3
---

Dès qu’un système tourne sur plusieurs machines reliées par un réseau, une partie peut tomber en panne pendant que le reste continue, et aucune machine ne sait avec certitude ce qui arrive aux autres. Cette partie traite les problèmes qui en découlent et les mécanismes qui les résolvent : détecter qu’une machine est tombée ([chapitre 32](pannes-partielles.mdx)), ordonner des événements sans horloge commune, se mettre d’accord par un protocole de consensus (Paxos, Raft), coordonner des services, faire des transactions qui touchent plusieurs bases, résister aux pannes des dépendances par des nouvelles tentatives, des disjoncteurs et une contre-pression, compter et filtrer avec des structures probabilistes, et faire converger des répliques. Les briques de la partie 2 reposent toutes sur ces mécanismes, souvent cachés dans la base ou la file de messages qu’on utilise.
