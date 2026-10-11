---
title: Comment le cours est construit
sidebar_label: Présentation
slug: /
cartouche: Présentation
---

Ce cours s’adresse à des personnes qui savent programmer, ont peut-être déjà écrit une application web avec sa base de données, mais n’ont jamais eu à se demander ce qui se passe quand cette application reçoit cent fois plus de requêtes, quand un disque meurt pendant la nuit ou quand deux utilisateurs réservent la même place à la même milliseconde. À la fin, vous devez pouvoir concevoir un système réel, défendre chacun de vos choix avec des chiffres et des compromis explicites, et tenir quarante-cinq minutes face à un examinateur qui vous demande de concevoir une messagerie ou un service de paiement.

## L’ordre des parties

La partie 0 donne la méthode et l’outil de calcul dont tout le reste se sert. La partie 1 pose les fondamentaux (réseau, protocoles, latence, disponibilité, cohérence), la partie 2 les briques que l’on assemble (répartiteurs, caches, bases de données, files de messages), la partie 3 ce qui rend les systèmes distribués difficiles (pannes partielles, horloges, consensus). Les parties 4 et 5 traitent des styles d’architecture et de l’exploitation en production. La partie 6 applique tout cela à vingt-six systèmes conçus de bout en bout, et la partie 7, facultative, prépare aux entretiens de conception objet.

Les chapitres sont numérotés de 1 à 89 sans repartir à zéro à chaque partie : un renvoi comme « ch. 17 » désigne toujours le même chapitre. Une notion n’est jamais utilisée avant d’avoir été expliquée ; quand un chapitre a besoin d’une idée traitée plus loin, il le dit et renvoie vers elle.

## Ce que vous trouverez dans un chapitre

Chaque chapitre des parties 0 à 5 suit le même fil. Une analogie de la vie courante donne l’intuition, puis on montre où elle cesse d’être juste. L’explication technique vient ensuite, avec des définitions précises et le fonctionnement pas à pas, illustrée par des schémas qui suivent le thème clair ou sombre du site. Les ordres de grandeur sont toujours accompagnés d’un calcul d’estimation mené ligne par ligne. Quand plusieurs options existent, un tableau les compare. Un exemple d’entreprise n’apparaît que si le fait est documenté, avec sa source en note. Le chapitre se termine par les erreurs fréquentes, trois questions de compréhension, un exercice de conception, les corrigés détaillés et une fiche de révision.

Quelques encadrés reviennent :

:::analogie
L’image qui donne l’intuition, et l’endroit où elle trompe.
:::

:::calcul
Une estimation menée ligne par ligne, hypothèses comprises.
:::

:::piege
Une erreur que font souvent les débutants, ou les candidats en entretien.
:::

:::entretien
Ce qu’un examinateur attend sur ce point précis.
:::

## Conventions

Le vocabulaire anglais est celui de l’industrie et des entretiens : il est donné entre parenthèses à la première occurrence, par exemple « mise en cache (caching) », puis le terme le plus courant est employé. Les chiffres viennent de sources citées en notes de bas de page ; quand une valeur est une hypothèse de calcul, elle est présentée comme telle. Le code est en Python, les requêtes en SQL standard (PostgreSQL quand un dialecte est nécessaire).

Faites les exercices avant de lire les corrigés, et refaites les calculs vous-même : c’est en entretien, sans corrigé sous les yeux, que l’habitude de chiffrer fait la différence.
