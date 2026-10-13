# Cours de system design : plan détaillé (proposition du 2026-10-11)

Public : sait programmer, n'a jamais conçu de système distribué. Objectif : concevoir un système
réel, justifier chaque choix, réussir un entretien de system design. Un chapitre par réponse,
dans l'ordre ; « suite » = chapitre suivant. Les numéros de chapitre sont continus d'une partie à
l'autre pour permettre les renvois (« voir ch. 17 »).

Structure imposée de chaque chapitre (parties 0 à 5) : problème de départ (facultatif), explication technique
(sans analogie, décision du 2026-10-13), schéma(s) TikZ adaptés au thème clair/sombre suivis de
2-3 phrases de lecture, ordres de grandeur + calcul d'estimation ligne par ligne, compromis
(tableau), exemple réel (seulement si sûr), erreurs fréquentes (débutant et entretien),
exercices (3 questions de compréhension + 1 exercice de conception, corrigés en fin de
chapitre), fiche de révision (5 à 10 lignes).

## Partie 0 : méthode

### 1. La démarche de conception
Les huit étapes appliquées sur un service volontairement simple (un seul serveur, une base) :
exigences fonctionnelles et non fonctionnelles, estimation de charge, API, modèle de données,
architecture haut niveau, approfondissement, goulets d'étranglement et points uniques de
défaillance, évolutions. Différences entre l'entretien (45 min, dialogue, gestion du temps,
ce que l'examinateur évalue) et la situation réelle (document de conception, ADR, revue par les
pairs). Problème de départ : une équipe qui code avant d'avoir chiffré la charge.

### 2. Estimer la charge (back-of-the-envelope)
Puissances de 2 et de 10, conversions utiles (86 400 s par jour, ~2,6 millions de s par mois),
passage des utilisateurs actifs quotidiens (DAU) aux requêtes par seconde moyennes et de
pointe, ratio lecture/écriture, stockage sur cinq ans, bande passante, nombre de machines.
Écrire ses hypothèses, arrondir franchement, vérifier l'ordre de grandeur. Plusieurs calculs
complets (service de photos, messagerie).

## Partie 1 : fondamentaux

### 3. Client-serveur et DNS
Modèle client-serveur, adresses IP, ports, sockets. Résolution DNS pas à pas (résolveur
récursif, racine, TLD, serveur faisant autorité), types d'enregistrements (A, AAAA, CNAME, NS,
MX, TXT), TTL et caches. DNS comme outil de répartition (round-robin, GeoDNS) et ses limites.
Problème : une migration de serveur bloquée par un TTL de 24 h.

### 4. Transport : TCP et UDP
Poignée de main en trois temps, fiabilité, contrôle de flux et de congestion, blocage en tête
de file (head-of-line blocking), coût d'une connexion en allers-retours (RTT), keep-alive et
pools de connexions, TIME_WAIT et épuisement des ports éphémères. UDP et ses usages (DNS,
voix, jeux, QUIC). Problème : un service qui ouvre une connexion par requête.

### 5. HTTP/1.1, HTTP/2, HTTP/3 et QUIC
Méthodes, codes d'état, en-têtes, cache HTTP (Cache-Control, ETag). HTTP/2 : multiplexage,
compression des en-têtes, blocage en tête de file qui subsiste au niveau TCP. HTTP/3 sur QUIC :
établissement en 1-RTT ou 0-RTT, flux indépendants, migration de connexion. Nombre d'allers-
retours avant le premier octet selon la version, sur réseau mobile.

### 6. TLS et HTTPS
Ce que TLS garantit (confidentialité, intégrité, authentification du serveur), certificats et
chaîne de confiance, poignée de main TLS 1.3, 0-RTT et risque de rejeu, terminaison TLS,
mTLS entre services, renouvellement automatique (ACME). Problème : un certificat expiré qui
coupe un service entier.

### 7. Styles d'API : REST, GraphQL, gRPC
Ressources, verbes, codes, pagination (offset et curseur), versionnage. GraphQL : schéma,
sur-récupération et sous-récupération, problème N+1, cache plus difficile. gRPC : Protobuf,
HTTP/2, streaming, contrats. Sérialisation et évolution de schéma (JSON, Protobuf, Avro).
Tableau de choix selon le client (navigateur, mobile, service interne).

### 8. Pousser des données vers le client
Polling, long polling, Server-Sent Events, WebSocket, webhooks : fonctionnement, coût en
connexions ouvertes et en mémoire, reconnexion, passage à travers proxys et répartiteurs.
Sécurité des webhooks (signature HMAC, horodatage contre le rejeu). Problème : un tableau de
bord qui interroge le serveur chaque seconde depuis un million de navigateurs.

### 9. Latence, débit et chiffres à connaître
Définitions, percentiles (p50, p99, p99,9), latence de queue (tail latency) amplifiée par
l'appel en éventail (fan-out), loi de Little, saturation et files d'attente. Tableau des
latences de référence (cache CPU, mémoire, SSD, disque, réseau dans un centre de données, entre
continents), avec sources et valeurs actuelles. Problème : moyenne à 50 ms mais 1 % des
utilisateurs attend 3 s.

### 10. Passer à l'échelle : vertical, horizontal, sans état
Scalabilité verticale et horizontale, limites physiques et coût de chacune. Services sans état
(stateless) : où mettre l'état (magasin de sessions partagé, jeton côté client), sessions
collantes (sticky sessions) et leurs défauts. Loi d'Amdahl et contention. Problème : le site
qui s'effondre le jour des soldes.

### 11. Disponibilité et fiabilité
Disponibilité, fiabilité, durabilité ; MTBF et MTTR. Calcul des « neuf » (temps d'arrêt
autorisé par an, par mois), disponibilité de composants en série et en parallèle, redondance et
points uniques de défaillance (SPOF). SLI, SLO, SLA et budget d'erreur. Problème : un contrat
promettant 99,99 % sur une architecture qui ne peut pas les tenir.

### 12. Cohérence, CAP et PACELC
Modèles de cohérence (linéarisabilité, cohérence causale, lire ses propres écritures, lectures
monotones, cohérence à terme), différence entre sérialisabilité et linéarisabilité. Énoncé
exact du théorème CAP et contresens courants (« choisir deux parmi trois »), PACELC pour le cas
sans partition. Problème : un utilisateur modifie son profil et voit l'ancienne version.

## Partie 2 : briques de base

### 13. Répartition de charge
Répartiteurs L4 et L7, algorithmes (round-robin, pondéré, moins de connexions, temps de
réponse, hachage, « power of two choices »), contrôles de santé actifs et passifs, drainage
des connexions, haute disponibilité du répartiteur lui-même (IP virtuelle, anycast).
Logiciels (HAProxy, NGINX, Envoy) et répartiteurs gérés du cloud.

### 14. Reverse proxy et API gateway
Reverse proxy : terminaison TLS, compression, mise en tampon, protection de l'origine. API
gateway : authentification, routage, quotas, agrégation, transformation. Backend for frontend
(BFF), comparaison avec un service mesh. Risque : la passerelle qui devient un point unique de
défaillance ou un monolithe de configuration.

### 15. CDN et edge computing
Points de présence, routage vers le plus proche (anycast, DNS), modes pull et push, clé de
cache, TTL, purge, taux de succès, bouclier d'origine (origin shield), contenu dynamique.
Edge computing : exécuter du code à la périphérie, et ce qui reste difficile (état,
cohérence). Calcul : bande passante économisée à l'origine.

### 16. Cache (1) : placement et stratégies
Où placer un cache (client, CDN, mémoire locale du service, cache distribué, cache de la
base). Cache-aside, read-through, write-through, write-back, write-around. Politiques
d'éviction (LRU, LFU, FIFO, TTL) et leur comportement selon la distribution des accès. Calcul
de la latence moyenne en fonction du taux de succès.

### 17. Cache (2) : invalidation, stampede, hot keys
Invalidation (TTL, événement, numéro de version) et courses entre cache et base. Cache
stampede : verrou de recalcul, expiration anticipée probabiliste, stale-while-revalidate.
Clés chaudes (hot keys) : copies multiples, cache local de second niveau. Redis et Memcached
comparés. Problème : l'expiration d'une seule clé populaire fait tomber la base.

### 18. Bases relationnelles (1) : transactions et isolation
ACID défini précisément, niveaux d'isolation SQL et anomalies (lecture sale, lecture non
répétable, fantôme, mise à jour perdue, write skew), MVCC, verrouillage pessimiste et
optimiste, SELECT ... FOR UPDATE. Problème : deux clients réservent le même siège.

### 19. Bases relationnelles (2) : index et modélisation
Index B-tree pas à pas, index composite et ordre des colonnes, index couvrant, lecture d'un
plan d'exécution. Normalisation (1NF à 3NF) et dénormalisation réfléchie. Pool de connexions et
limite du nombre de connexions. Coût d'un index en écriture.

### 20. Moteurs de stockage : B-tree, LSM, SSTables
Journal d'écriture anticipée (WAL), B-tree contre arbre LSM : memtable, SSTables, compaction
(par paliers, par niveaux), amplification en écriture, en lecture et en espace. Pourquoi les
bases orientées écriture choisissent le LSM. Le filtre de Bloom y est présenté par son usage,
sa théorie vient au ch. 40.

### 21. NoSQL (1) : clé-valeur, document, colonnes larges, graphe
Modèle de données, requêtes possibles et impossibles, modélisation guidée par les requêtes
(cas Cassandra et DynamoDB), documents imbriqués contre références, traversées de graphe.
Systèmes représentatifs (Redis, DynamoDB, MongoDB, Cassandra, Neo4j) sans palmarès.

### 22. NoSQL (2) : séries temporelles, recherche, vecteurs
Séries temporelles : compression, rétention, sous-échantillonnage. Moteurs de recherche :
tokenisation, index inversé, BM25, Elasticsearch et OpenSearch. Bases vectorielles :
plongements (embeddings), recherche approchée des plus proches voisins (HNSW, IVF), compromis
rappel contre latence.

### 23. Choisir une base de données
Grille de questions : forme des données, motifs d'accès, besoins de cohérence, volume,
latence, compétences de l'équipe, coût d'exploitation. Persistance polyglotte et son coût
réel. Bases NewSQL (Spanner, CockroachDB). Cinq scénarios commentés.

### 24. Réplication (1) : leader-follower
Réplication synchrone, asynchrone, semi-synchrone ; retard de réplication et anomalies qu'il
produit (lire ses propres écritures, lectures monotones). Bascule (failover), split brain,
promotion d'un réplica, réplicas de lecture. Problème : la bascule automatique qui perd les
dernières écritures.

### 25. Réplication (2) : multi-leader, sans leader, quorums
Multi-leader (multi-région, édition hors ligne), sans leader (style Dynamo), quorums R + W > N
et ce qu'ils ne garantissent pas, sloppy quorum et hinted handoff, réparation à la lecture.
Conflits : dernier écrivain gagne (LWW), vecteurs de version (présentés ici sur l'exemple,
généralisés au ch. 33), fusion applicative.

### 26. Partitionnement (sharding)
Par plage, par hachage, hachage cohérent et nœuds virtuels, rééquilibrage (nombre fixe de
partitions, partitionnement dynamique), index secondaires locaux et globaux, partitions
chaudes, routage des requêtes. Calcul : nombre de partitions pour 50 To et 200 000
écritures/s.

### 27. Files de messages et logs
Pourquoi découpler par message. File (RabbitMQ, SQS) contre log partitionné (Kafka) :
consommation destructive ou relue, offsets, rétention. Pub/sub, consumer groups, ordre par
partition et clé de partition, schéma des messages et registre de schémas. Problème : un pic
d'inscriptions qui sature le service d'e-mail.

### 28. Garanties de livraison
At-most-once, at-least-once, exactly-once : ce que chacune coûte, ce que « exactly-once »
veut dire réellement (consommateur idempotent, transactions Kafka). Accusés de réception,
redélivrance, messages empoisonnés, files de lettres mortes (DLQ), ordre et parallélisme.

### 29. Stockage objet, bloc, fichier
Trois modèles d'accès, leurs interfaces et usages. Stockage objet : clés, métadonnées,
téléversement en plusieurs parties, URL présignées, classes de stockage, durabilité,
réplication contre codage d'effacement (erasure coding). Calcul : coût de 1 Po selon la classe.

### 30. Limitation de débit (rate limiting)
Seau à jetons (token bucket), seau percé (leaky bucket), fenêtre fixe, journal glissant,
compteur à fenêtre glissante : précision, mémoire, rafales. Où limiter (client, passerelle,
service), par quelle clé, réponse 429 et en-têtes. La version distribuée est traitée au ch. 59.

### 31. Identifiants uniques
Auto-incrément et séquences, allocation par blocs, UUID v4 et v7, ULID, Snowflake (horodatage,
machine, séquence). Effet de l'identifiant sur l'index B-tree (insertions aléatoires contre
croissantes), tri temporel, fuite d'information par des identifiants devinables.

## Partie 3 : systèmes distribués

### 32. Pannes partielles et détection de panne
Modèles de panne (arrêt, omission, byzantine), réseau non fiable, impossibilité de distinguer
un nœud lent d'un nœud mort. Heartbeats, délais adaptatifs (détecteur phi accrual),
protocoles de gossip (style SWIM), faux positifs et leurs conséquences.

### 33. Le temps dans un système distribué
Horloges physiques, NTP, dérive, sauts d'horloge ; pourquoi ordonner par horodatage est
dangereux. Horloges de Lamport, horloges vectorielles, horloges hybrides (HLC), TrueTime de
Spanner et l'attente de validation. Problème : deux écritures dont la plus récente est écrasée.

### 34. Consensus (1) : le problème et Paxos
Ce qu'est le consensus et où on en a besoin (élection, configuration, journal répliqué),
résultat FLP présenté sans démonstration, quorums majoritaires. Paxos : propositions,
promesses, acceptations, présenté par l'intuition et un déroulé chiffré.

### 35. Consensus (2) : Raft en détail
Rôles, mandats (terms), élection et délais aléatoires, réplication du journal, règle de
validation, propriétés de sûreté, instantanés (snapshots), changement de membres. Déroulés
pas à pas avec pannes. Coût : latence d'une écriture, taille de cluster (3 ou 5 nœuds).

### 36. Coordination : ZooKeeper, etcd, service discovery
Ce que fournissent ZooKeeper et etcd (clés, watches, baux), élection de leader, verrous
distribués, jetons de clôture (fencing tokens) et pourquoi un verrou sans eux est dangereux.
Service discovery côté client et côté serveur, DNS, registres.

### 37. Transactions distribuées : 2PC, sagas, outbox
Validation à deux phases et son blocage en cas de panne du coordinateur. Sagas par
orchestration et par chorégraphie, actions de compensation. Problème de la double écriture
(base + message) et motif outbox, capture des changements (CDC).

### 38. Résilience (1) : idempotence, retries, timeouts
Idempotence et clés d'idempotence, retries, backoff exponentiel avec jitter (calculs
comparés), timeouts et propagation des échéances (deadlines), requêtes doublées (hedged
requests). Problème : une tempête de retries qui achève un service déjà lent.

### 39. Résilience (2) : circuit breaker, bulkhead, back-pressure
Disjoncteur (états fermé, ouvert, semi-ouvert), cloisons (bulkheads), back-pressure,
délestage (load shedding), dégradation progressive, priorisation du trafic. Pannes en cascade
et métastables.

### 40. Structures probabilistes
Filtre de Bloom (taux de faux positifs, dimensionnement), HyperLogLog (comptage de
cardinalité), Count-Min Sketch (fréquences, éléments les plus fréquents). Calcul de la mémoire
nécessaire comparée à une structure exacte.

### 41. Faire converger des répliques : Merkle et CRDT
Arbres de Merkle pour l'anti-entropie, comparaison de deux répliques en O(log n) échanges.
CRDT : compteurs (G-Counter, PN-Counter), registres, ensembles (OR-Set), conditions de
convergence, limites (taille des métadonnées, invariants impossibles).

## Partie 4 : styles d'architecture

### 42. Monolithe, monolithe modulaire, SOA, microservices
Ce que chaque style apporte et coûte : déploiement, couplage, données par service,
communications réseau, observabilité, organisation des équipes (loi de Conway). Quand ne pas
découper. Monolithe distribué, découpage par domaine.

### 43. Architecture orientée événements, CQRS, event sourcing
Événements contre commandes, notification contre transfert d'état. CQRS : modèle d'écriture et
modèles de lecture. Event sourcing : journal des événements comme source de vérité,
projections, instantanés, évolution des événements, coût réel.

### 44. Serverless
Fonctions à la demande, démarrage à froid, limites (durée, mémoire, connexions aux bases),
modèle de coût à l'invocation, calcul du point de bascule contre des machines permanentes.

### 45. Architecture cellulaire
Découper le système en cellules indépendantes, routage vers une cellule, rayon d'impact
(blast radius), déploiement cellule par cellule, partitionnement par client (shuffle sharding).

### 46. Multi-région
Actif-passif, actif-actif, réplication entre régions, latences inter-régions, données qui
doivent rester dans une juridiction, routage des utilisateurs, conflits d'écriture, coût du
trafic entre régions.

### 47. Traitement par lots : MapReduce, Spark
Modèle MapReduce, shuffle, tolérance aux pannes par réexécution, Spark (RDD, DataFrame,
évaluation paresseuse), partitionnement et déséquilibre (skew). Calcul : temps de traitement
de 10 To.

### 48. Traitement de flux : Flink, Lambda, Kappa
Temps d'événement et temps de traitement, fenêtres, watermarks, données tardives, état et
points de contrôle (checkpoints), exactly-once de bout en bout. Architectures Lambda et Kappa.

### 49. Entrepôts, lacs, lakehouse
Data warehouse, data lake, lakehouse ; stockage en colonnes (Parquet), formats de tables
(Iceberg, Delta Lake), ETL contre ELT, OLTP contre OLAP, modélisation en étoile.

## Partie 5 : exploitation en production

### 50. Observabilité
Logs structurés, métriques (compteurs, jauges, histogrammes), traces distribuées et
propagation de contexte (OpenTelemetry), cardinalité. Méthodes RED et USE, signaux d'or.
Alertes sur les symptômes, alertes sur la consommation du budget d'erreur.

### 51. Conteneurs, Kubernetes et autoscaling
Les notions de Kubernetes utiles au concepteur (Pod, Deployment, Service, sondes, requêtes et
limites, StatefulSet), autoscaling horizontal et vertical, délais de démarrage, mise à
l'échelle sur métriques de file d'attente. Renvoi vers le cours container_k8s pour la pratique.

### 52. Mettre en production sans coupure
Rolling update, blue-green, canary, feature flags, retour arrière. Migrations de schéma sans
interruption (expand puis contract), compatibilité des versions pendant le déploiement.

### 53. Reprise après sinistre
Sauvegardes (complètes, incrémentales, continues), RPO et RTO, stratégies (sauvegarde et
restauration, pilot light, warm standby, multi-site actif), tests de restauration, sauvegardes
hors ligne contre les rançongiciels.

### 54. Sécurité (1) : authentification
Mots de passe et leur stockage, sessions côté serveur contre jetons, JWT (structure,
signature, révocation), OAuth 2.0 (flux code + PKCE, client credentials), OpenID Connect,
authentification multifacteur.

### 55. Sécurité (2) : autorisation, chiffrement, secrets, abus
Modèles d'autorisation (RBAC, ABAC, relations façon Zanzibar), chiffrement en transit et au
repos, chiffrement d'enveloppe et KMS, gestion et rotation des secrets. Protection contre les
abus : bots, credential stuffing, DDoS, WAF.

### 56. Coûts
Postes d'une facture cloud (calcul, stockage, transfert sortant, requêtes, services gérés),
estimation complète d'une architecture, leviers de réduction (dimensionnement, instances
réservées et spot, classes de stockage, cache, compression), coût par requête.

## Partie 6 : études de cas

Chaque cas applique la démarche du ch. 1 de bout en bout : exigences, estimation chiffrée, API,
modèle de données, schéma d'architecture, au moins deux approfondissements, goulets
d'étranglement, évolutions ; puis erreurs fréquentes, exercices et fiche.

### 57. Raccourcisseur d'URL et Pastebin
Génération de clés (encodage base62 d'un compteur, hachage tronqué, clés pré-générées),
redirection 301 contre 302 et statistiques, cache des liens populaires, expiration. Pastebin :
contenu dans le stockage objet, métadonnées en base.

### 58. Générateur d'identifiants uniques distribué
Exigences (unicité, tri, 64 bits, débit), Snowflake détaillé, attribution des numéros de
machine par etcd, recul d'horloge, comparaison avec l'allocation par segments en base.

### 59. Rate limiter distribué
Compteurs centralisés dans Redis et opérations atomiques (scripts Lua), fenêtre glissante,
synchronisation approximative entre régions, comportement si Redis tombe (laisser passer ou
refuser), configuration des règles.

### 60. Base clé-valeur distribuée (type Dynamo)
Hachage cohérent, réplication sur N nœuds, quorums R/W, hinted handoff, conflits et versions,
anti-entropie par arbres de Merkle, appartenance par gossip, moteur LSM local. Synthèse des
parties 2 et 3.

### 61. Web crawler
Frontière d'URL, politesse (robots.txt, délai par hôte), résolution DNS en masse,
déduplication des URL (Bloom) et des contenus (empreintes, SimHash), pièges à robots,
re-visite. Estimation : un milliard de pages par mois.

### 62. Système de notifications
Canaux (push APNs et FCM, SMS, e-mail), préférences, files par canal, retries et DLQ,
déduplication, limitation par utilisateur, modèles, suivi de livraison.

### 63. Service d'e-mail
Envoi (SMTP, réputation des IP, SPF, DKIM, DMARC), réception, stockage des boîtes, recherche,
pièces jointes dans le stockage objet, filtrage du spam.

### 64. Fil d'actualité (type X, Instagram)
Fan-out à l'écriture contre à la lecture, cas des comptes très suivis (hybride), cache du fil,
classement, pagination par curseur, médias servis par CDN.

### 65. Messagerie instantanée (type WhatsApp)
Connexions WebSocket persistantes et leur répartition, présence, accusés (envoyé, reçu, lu),
ordre par conversation, messages hors ligne, groupes, synchronisation multi-appareils,
principes du chiffrement de bout en bout.

### 66. Autocomplétion (typeahead)
Trie avec top-k par préfixe, collecte et agrégation des requêtes, reconstruction hors ligne
contre mise à jour continue, cache navigateur et CDN, partitionnement du trie, filtrage.

### 67. Moteur de recherche
Chaîne d'indexation, index inversé partitionné par document ou par terme, classement (BM25,
signaux, PageRank par l'intuition), requête en éventail et latence de queue, fraîcheur de
l'index.

### 68. Streaming vidéo (type YouTube, Netflix)
Téléversement, transcodage en DAG de tâches, segments et débit adaptatif (HLS, DASH), CDN et
pré-positionnement, métadonnées, coût du stockage et de la bande passante.

### 69. Stockage et synchronisation de fichiers (type Dropbox, Google Drive)
Découpage en blocs, déduplication par empreinte, synchronisation différentielle, métadonnées et
versions, notification des changements, conflits d'édition hors ligne.

### 70. Stockage objet (type S3)
Service de métadonnées, nœuds de données, placement, codage d'effacement, téléversement
multipart, modèle de cohérence, versionnage, cycle de vie, ramasse-miettes.

### 71. Services de proximité (type Yelp, Uber)
Géohash, quadtree, découpages S2 et H3, recherche des points proches, mises à jour de position
à haute fréquence, appariement chauffeur-passager, partitionnement géographique.

### 72. Cartographie et itinéraires (type Google Maps)
Graphe routier, Dijkstra et A*, hiérarchies de contraction, tuiles, estimation du temps de
trajet avec trafic en temps réel, mise à jour du graphe.

### 73. Billetterie à forte concurrence (type Ticketmaster)
File d'attente virtuelle, réservation temporaire avec expiration, contrôle de concurrence,
prévention de la survente, bots, pic à l'ouverture des ventes.

### 74. Réservation d'hôtel
Inventaire par type de chambre et par nuit, contraintes transactionnelles, idempotence,
surréservation volontaire, cache de disponibilité et cohérence, paiement.

### 75. Paiement et portefeuille numérique
Flux avec un prestataire de paiement, clés d'idempotence, grand livre en partie double,
réconciliation, machine à états d'un paiement, sagas et outbox, représentation des montants.

### 76. Bourse et moteur d'appariement d'ordres
Carnet d'ordres, priorité prix-temps, moteur déterministe mono-thread, séquenceur et journal
d'événements, réplication par rejeu, latence en microsecondes, diffusion des cotations.

### 77. Agrégation de clics publicitaires
Ingestion à haut débit, déduplication, agrégation fenêtrée, données tardives, exactitude contre
latence, stockage OLAP, réconciliation pour la facturation.

### 78. Classement en temps réel (leaderboard)
Ensembles triés Redis, rang exact contre approché, partitionnement des scores pour des
centaines de millions de joueurs, classements par période, persistance.

### 79. Ordonnanceur de tâches distribué
Stockage des tâches, déclenchement à l'heure (seaux temporels, files différées), exécution au
moins une fois et idempotence, baux des workers, tâches récurrentes, DAG de dépendances,
priorités.

### 80. Monitoring et métriques
Collecte pull contre push, modèle des séries temporelles et cardinalité, stockage compressé,
requêtes d'agrégation, évaluation des alertes, rétention et sous-échantillonnage.

### 81. Servir un LLM en production
Ce qui change avec un LLM (GPU, mémoire, phases prefill et decode, cache KV), batching
continu, streaming des tokens, files d'attente et limitation par tokens, routage entre
modèles, latence (temps au premier token, tokens par seconde), coût par million de tokens.

### 82. Système RAG
Ingestion (découpage, plongements), index vectoriel et recherche hybride, reclassement,
construction du contexte, mise à jour des documents, contrôle d'accès dans la recherche,
évaluation, cache sémantique.

## Partie 7 (optionnelle) : low-level design

### 83. Principes de conception objet et SOLID
Encapsulation, composition contre héritage, cohésion et couplage, les cinq principes SOLID
chacun avec un contre-exemple et sa correction.

### 84. Patrons de conception utiles
Stratégie, état, observateur, fabrique, constructeur, décorateur, adaptateur, commande,
singleton et ses pièges. Pour chacun : le problème qu'il résout et quand il est de trop.

### 85. Méthode d'entretien LLD et parking
Démarche (exigences, entités, relations, diagramme de classes, code des parties clés, tests),
appliquée au parking : places de tailles différentes, tarifs, entrées multiples.

### 86. Distributeur automatique
Machine à états, gestion de la monnaie, cas d'erreur, patron État.

### 87. Ascenseur
Modélisation, algorithmes de desserte, plusieurs cabines, patron Stratégie pour
l'ordonnancement.

### 88. Système de réservation
Salles ou places, conflits de réservation, concurrence et verrous dans le code, notifications.

### 89. Cache LRU et rate limiter en code
Implémentation d'un cache LRU (table de hachage + liste doublement chaînée) et d'un seau à
jetons, sûreté en contexte multi-thread, tests.

## Annexes

- A. Glossaire français-anglais.
- B. Tableau des chiffres de référence (latences, débits, tailles, coûts), sources indiquées.
- C. Grille d'auto-évaluation pour l'entretien.
- D. Lectures pour aller plus loin (livres, articles fondateurs, blogs d'ingénierie), chaque
  référence vérifiée.
