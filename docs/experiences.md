---
title: Reproduire les expériences
sidebar_label: Reproduire les expériences
cartouche: Présentation
description: "Où trouver les scripts des mesures du cours, ce qu'il faut installer pour les relancer, et comment nettoyer après."
---

Plusieurs chapitres publient des mesures faites pour le cours : le coût d’une poignée de main TLS, la mémoire de dix mille connexions WebSocket, la latence de queue d’une file d’attente, le débit d’un parc de serveurs derrière un répartiteur, ce que renvoie une réplique Redis pendant une coupure du réseau. Les scripts qui les produisent sont publiés dans le dépôt du cours, dans le dossier [`outils/experiences/`](https://github.com/menraromial/system-design/tree/main/outils/experiences), un fichier ou un sous-dossier par chapitre. Chaque script décrit en tête ce qu’il mesure, ce qu’il demande et comment le lancer.

## La machine des mesures

Les chiffres publiés ont été obtenus sur un ordinateur portable : processeur Intel Core Ultra 7 165H (22 fils d’exécution), SSD NVMe Samsung, Linux 7.0, Python 3.14, OpenSSL 3.5.5, Caddy 2.10.2, wrk 4.1.0, Docker 29.3 et gcc 15.2. Toutes les mesures sont locales. Serveurs, clients et répartiteurs tournent sur la même machine, et les délais de réseau, quand un chapitre en a besoin, sont ajoutés par un petit relais écrit en Python. Aucune mesure ne passe par Internet.

Sur une autre machine, les valeurs absolues changeront ; un processeur plus lent servira moins de requêtes par seconde, un autre disque aura d’autres latences. Les comparaisons doivent tenir, elles, comme le tourniquet plafonné par le serveur le plus lent, la reprise de session TLS plus rapide qu’une poignée de main complète, la lecture immédiate sur une réplique qui renvoie parfois l’ancienne valeur. Pour des mesures stables, mieux vaut ne rien faire tourner d’autre de lourd pendant l’exécution.

## Installer

```bash
git clone https://github.com/menraromial/system-design.git
cd system-design/outils/experiences
python3 -m venv .venv
. .venv/bin/activate
pip install -r ch13/requirements.txt    # le fichier du chapitre voulu
```

Les dépendances Python de chaque chapitre sont dans son `requirements.txt`. Les outils système à installer à part sont indiqués dans le tableau ci-dessous.

## Les expériences, chapitre par chapitre

| Ch. | Scripts | Ce qui est mesuré | À installer en plus de Python | Lancement |
|---|---|---|---|---|
| 4 | [`ch04_time_wait.py`](https://github.com/menraromial/system-design/blob/main/outils/experiences/ch04_time_wait.py) | épuisement des ports éphémères par les connexions en `TIME_WAIT` | Linux ; une adresse IP de la machine autre que `127.0.0.1` | `python3 ch04_time_wait.py ADRESSE` |
| 5 | [`ch05/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch05) | HTTP/1.1 et HTTP/2 avec 50 ms d’aller-retour ajoutées ; taille des en-têtes compressés par HPACK | Caddy, curl avec HTTP/2 ; paquet `hpack` | `./mesurer.sh` ; `python3 hpack_taille.py` |
| 6 | [`ch06/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch06) | poignées de main TLS complètes et reprises, TLS mutuel | OpenSSL 3.4 ou plus récent | `./experience.sh` |
| 7 | [`ch07/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch07) | pagination par décalage et par curseur, requêtes N+1 de GraphQL, taille des formats de sérialisation | `requirements.txt` | `python3 pagination.py`, `graphql_n1.py`, `serialisation.py` |
| 8 | [`ch08/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch08) | mémoire de 10 000 connexions ; trafic du polling, du long polling, de SSE et de WebSocket | `requirements.txt` | `python3 memoire.py` ; `python3 serveur.py PORT` puis `python3 trafic.py PORT` |
| 9 | [`ch09/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch09) | files d’attente et percentiles, latence de queue d’un éventail de requêtes, latences du processeur, de la mémoire et du disque | gcc, `taskset` | `python3 file_attente.py`, `eventail.py` ; `latences.c` : voir l’en-tête du fichier |
| 10 | [`ch10/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch10) | débit de 1 à 4 serveurs, emplacement des sessions, loi d’Amdahl | Caddy, wrk, Docker ; `requirements.txt` | `./echelle.sh python3`, `python3 sessions.py`, `amdahl.py` |
| 11 | [`ch11/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch11) | disponibilité vue par le client quand un serveur tombe, dans cinq architectures | Caddy ; `requirements.txt` ; utilise `ch10/app.py` | `python3 bascule.py` |
| 12 | [`ch12/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch12) | lectures périmées sur une réplique Redis, coût de `WAIT`, lectures monotones, partition | Docker, image `redis:8.8-alpine` ; `requirements.txt` | `python3 replique.py` |
| 13 | [`ch13/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch13) | politiques de répartition sur un parc hétérogène, hachage d’URI, répartition L4 et L7, drainage | Caddy, wrk ; `requirements.txt` | `python3 repartition.py`, `python3 drainage.py` |
| 14 | [`ch14/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch14) | protection de l’origine contre les clients lents par la mise en tampon, coût et gain de la compression, coût de l’authentification déléguée (`forward_auth`) | Caddy, wrk ; `requirements.txt` | `python3 tampon.py`, `compresser.py`, `auth.py` |
| 15 | [`ch15/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch15) | un petit CDN (8 PoP, bouclier d’origine) : taux de succès, requêtes à l’origine, clé de cache, latence d’un succès et d’un échec, TTL contre purge | `requirements.txt` | `python3 cdn.py` |
| 16 | [`ch16/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch16) | coût d’une lecture à chaque niveau (mémoire, Redis, PostgreSQL), stratégies d’écriture et pertes du write-back à la panne, politiques d’éviction de Redis | Docker, images `postgres:18-alpine` et `redis:8.8-alpine` ; `requirements.txt` | `python3 niveaux.py`, `ecriture.py`, `eviction.py` |
| 17 | [`ch17/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch17) | avalanche de recalculs (naïve, verrou, XFetch, stale-while-revalidate), course entre cache et base (double suppression, baux), débit d’une clé chaude | Docker, images `postgres:18-alpine` et `redis:8.8-alpine` ; `requirements.txt` | `python3 stampede.py`, `course.py`, `chaudes.py` |
| 18 | [`ch18/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch18) | les cinq anomalies d’isolation à chaque niveau, cinq façons de réserver un siège, MVCC (versions, VACUUM, transaction longue) | Docker, image `postgres:18-alpine` ; `requirements.txt` | `python3 anomalies.py`, `sieges.py`, `mvcc.py` |
| 19 | [`ch19/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch19) | effet de différents index sur trois requêtes (2 millions de lignes), hauteur des B-tree, coût des index à l’insertion, mises à jour HOT, connexions avec et sans pool | Docker, image `postgres:18-alpine` ; `requirements.txt` | `python3 index.py`, `ecritures.py`, `pool.py` |
| 20 | [`ch20/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch20) | coût d’une synchronisation du disque et validation groupée ; deux B-tree (LMDB, SQLite) et un arbre LSM (RocksDB, par niveaux et par paliers) : amplification en écriture et en espace, lectures, filtre de Bloom | `requirements.txt` (lmdb, rocksdict) ; environ 1,5 Go libres dans `~/.cache` | `python3 journal.py`, `moteurs.py` (environ 15 minutes) |
| 21 | [`ch21/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch21) | requêtes par clé et par valeur dans Redis ; commandes imbriquées, avec références (MongoDB) et en tables (PostgreSQL), document qui grandit jusqu’à 16 Mio ; table Cassandra et requête sans clé de partition ; traversées et plus court chemin dans Neo4j et PostgreSQL | Docker, images `redis:8.8-alpine`, `postgres:18-alpine`, `mongo:7`, `cassandra:5`, `neo4j:5` ; `requirements.txt` | `python3 cle_valeur.py`, `documents.py`, `colonnes.py`, `graphe.py` |
| 22 | [`ch22/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch22) | série temporelle de 12 millions de mesures (table ordinaire et TimescaleDB : compression, agrégat continu, rétention) ; recherche de mots dans 500 000 textes (PostgreSQL et OpenSearch) ; plus proches voisins parmi 200 000 vecteurs (pgvector, exact, HNSW, IVFFlat) | Docker, images `timescale/timescaledb:latest-pg17`, `pgvector/pgvector:pg18`, `opensearchproject/opensearch:2` ; `requirements.txt` | `python3 series.py`, `recherche.py`, `vecteurs.py` |
| 23 | [`ch23/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch23) | capacité d’un PostgreSQL seul mesurée par pgbench (lecture par clé et TPC-B, 1 à 64 clients) ; PostgreSQL contre une grappe CockroachDB de trois nœuds (latence, débit d’écriture, perte du nœud qui détient le bail) | Docker, images `postgres:18-alpine`, `cockroachdb/cockroach:latest` ; `requirements.txt` | `python3 capacite.py`, `reparti.py` |
| 24 | [`ch24/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch24) | PostgreSQL 18, un primaire et deux répliques derrière un relais qui retarde ou coupe le journal : coût de chaque `synchronous_commit` et des quorums, retard (grosse transaction, conflit avec une requête de la réplique), lire ses écritures par le LSN, bascule qui perd des écritures, cerveau divisé et `pg_rewind` | Docker, image `postgres:18-alpine` ; `requirements.txt` | `python3 synchrone.py`, `retard.py`, `bascule.py` |
| 25 | [`ch25/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch25) | deux PostgreSQL 18 abonnés l’un à l’autre (mises à jour croisées, additions perdues, clé dupliquée qui arrête la réplication) ; grappe Cassandra 5 de trois nœuds (niveaux de cohérence, nœud absent, réparation à la lecture, `nodetool repair`, indications différées, dernier écrivain gagne, transactions légères) | Docker, images `postgres:18-alpine`, `cassandra:5` (trois nœuds, 1,4 Go chacun au plus) ; `requirements.txt` | `python3 multileader.py`, `quorum.py` |
| 26 | [`ch26/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch26) | un million de clés réparties sur 10 puis 11 nœuds (modulo, anneau à 1, 16 et 256 jetons, hachage jump, partitions fixes), clés croissantes, popularité de Zipf et salage ; grappe Citus (32 partitions, requêtes routées ou sur toutes les partitions, rééquilibrage à l’ajout d’un nœud, isolement d’un gros client) | Python seul pour `hachage.py` ; Docker, image `citusdata/citus` et `requirements.txt` pour `citus.py` | `python3 hachage.py`, `citus.py` |
| 27 | [`ch27/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch27) | pic d’inscriptions vers un service d’e-mail limité, en appel direct et par une file RabbitMQ ; file RabbitMQ contre log Kafka (consommation, diffusion, relecture, groupes, ordre par clé avec et sans producteur idempotent, rééquilibrage classique et KIP-848) | Docker, images `rabbitmq:4-management`, `apache/kafka` ; `requirements.txt` | `python3 pic.py`, `file_log.py` |
| 28 | [`ch28/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch28) | consommateur Kafka qui plante cinq fois (au plus une fois, au moins une fois, validation automatique, consommateur idempotent avec SQLite) ; transformation de Kafka vers Kafka avec ou sans transaction ; RabbitMQ : redélivrance, message empoisonné (`reject` et `nack`, limite de livraisons, file de lettres mortes), ordre et parallélisme | Docker, images `apache/kafka`, `rabbitmq:4-management` ; `requirements.txt` ; données dans `~/.cache/cours-sd-ch28`, supprimées à la fin | `python3 kafka_garanties.py`, `rabbit_garanties.py` |
| 29 | [`ch29/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch29) | SeaweedFS par l’API S3 : PUT, GET, métadonnées, listing par préfixe et délimiteur ; modifier 1 octet et renommer un dossier, fichier contre objet ; téléversement en plusieurs parties et reprise par ListParts ; URL présignées en lecture et en écriture ; codage d’effacement (`ec.encode`) puis lecture après la perte de 4 et 5 fragments | Docker, image `chrislusf/seaweedfs` ; `requirements.txt` ; données dans `~/.cache/cours-sd-ch29`, supprimées à la fin | `python3 objet.py` |
| 30 | [`ch30/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch30) | cinq algorithmes de limitation de débit en simulation (rafales aux frontières, client régulier, précision sur 2 000 sources, mémoire par clé, décisions par seconde) ; module `limit_req` de nginx (rafale, `nodelay`, `delay`, code renvoyé) ; service HTTP qui répond 429 : réessai immédiat, `Retry-After`, recul avec gigue, clé par IP ou par clé d’API | Python seul pour `algorithmes.py` ; Docker, image `nginx:1.30-alpine` ; `requirements.txt` ; configuration dans `~/.cache/cours-sd-ch30`, supprimée à la fin | `python3 algorithmes.py`, `nginx.py`, `http429.py` |
| 31 | [`ch31/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch31) | générateurs d’identifiants en Python (vitesse, ordre, Snowflake avec une horloge simulée qui recule ou dérive, problème des chars allemands) ; 5 millions de lignes dans PostgreSQL 18 avec quatre clés primaires (séquence, UUID v7, UUID v4, UUID v4 en texte) : durée, journal, taille et densité de l’index, lignes récentes ; séquences (trous, cache, ordre de validation) et allocation par blocs | Python seul pour `generateurs.py` ; Docker, image `postgres:18-alpine` ; `requirements.txt` | `python3 generateurs.py`, `postgres_ids.py`, `sequences.py` |
| 32 | [`ch32/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch32) | un virement envoyé dans sept situations de panne (processus arrêté, machine injoignable, requête ou réponse perdue, serveur lent ou suspendu) ; trace réelle de battements de cœur avec des pauses par `SIGSTOP`, rejouée par des délais fixes et des détecteurs φ accrual ; simulation de SWIM (charge, détection, fausses déclarations, membres lents) | Python seul ; `requirements.txt` ; aucun conteneur | `python3 indiscernable.py`, `battements.py 1`, `swim.py 1` |
| 33 | [`ch33/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch33) | horloges de Linux (coût de lecture, état de la synchronisation par `adjtimex`, monotone contre brute) ; quatre serveurs NTP publics interrogés (décalage, délai, refus `RATE`) ; « la dernière écriture gagne » dans Cassandra 5 avec deux clients aux horloges décalées ; simulation de causalité (horodatages physiques, Lamport, vectoriels, HLC) et de l’attente de validation de TrueTime | Python seul pour `horloges.py`, `ntp.py` (accès UDP 123 vers Internet) et `causalite.py` ; Docker, image `cassandra:5`, pour `lww.py` ; `requirements.txt` | `python3 horloges.py`, `ntp.py`, `lww.py`, `causalite.py 1` |
| 34 | [`ch34/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch34) | Paxos sur cinq vrais processus accepteurs en UDP (durée d’une décision avec et sans `fsync`, accepteurs arrêtés par `SIGKILL` puis relancés, proposeur suspendu par `SIGSTOP` au milieu de sa phase 2) ; simulation de Paxos et de trois variantes fautives sur 10 000 exécutions (sûreté, vivacité, pannes, messages) | Python seul ; état des accepteurs dans `~/.cache/cours-sd-ch34`, supprimé à la fin | `python3 paxos_reel.py`, `paxos_sim.py 1` |
| 35 | [`ch35/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch35) | grappes etcd 3.7 de 1, 3 et 5 membres : durée des écritures, panne et isolement du leader, suiveur isolé avec et sans pré-vote, retards et gigue ajoutés par `tc`, connectivité partielle par `iptables` ; simulation de Raft (durée des élections, sûreté avec et sans la restriction d’élection et la règle de validation) | Docker, image `registry.k8s.io/etcd:3.7.0-0` et une image auxiliaire construite par le script (`tc`, `iptables`), supprimée à la fin ; Python seul pour `raft_sim.py` | `python3 etcd_raft.py`, `raft_sim.py 1` |
| 36 | [`ch36/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch36) | ZooKeeper 3.9, etcd 3.7 et Consul 1.20 : disparition d’un nœud éphémère et d’une clé à bail, délai des notifications, verrou attendu par 50 clients (effet de troupeau), détenteur suspendu avec et sans jeton d’isolement, instance tuée vue par cinq clients (watch etcd, requêtes bloquantes et DNS de Consul, cache DNS, nouvel essai) | Docker, images `zookeeper:3.9`, `registry.k8s.io/etcd:3.7.0-0` et `hashicorp/consul:1.20` (réseau de l’hôte) ; `pip install -r requirements.txt` | `python3 primitives.py`, `verrou.py`, `decouverte.py` |
| 37 | [`ch37/`](https://github.com/menraromial/system-design/tree/main/outils/experiences/ch37) | deux PostgreSQL 18 : validation à deux phases (durée, compte chaud, interblocage distribué, panne du coordinateur, VACUUM bloqué) ; double écriture vers Kafka 4.3 avec service et relais tués, boîte d’envoi par scrutation et par décodage logique, WAL retenu par un emplacement ; saga orchestrée sur trois services, orchestrateur tué, avec et sans journal ni idempotence | Docker, images `postgres:18-alpine` et `apache/kafka` (réseau de l’hôte) ; `pip install -r requirements.txt` | `python3 deux_phases.py`, `double_ecriture.py`, `saga.py` |

Les résultats s’affichent sur la sortie standard. Les fichiers de travail (configurations de Caddy, certificats du [ch. 6](partie-1/tls.mdx)) vont dans le dossier temporaire du système, sauf les cent petits fichiers que `ch05/mesurer.sh` crée dans `ch05/site/` ; `ch09/latences` crée un fichier de 1 Gio pour mesurer le disque et le supprime à la fin ; les scripts du [ch. 20](partie-2/moteurs-stockage.mdx) écrivent dans `~/.cache/cours-sd-ch20`, sur le disque et non dans un dossier temporaire qui peut être en mémoire, et le suppriment à la fin.

## Après une expérience

Chaque script arrête à la fin les processus qu’il a lancés (serveurs Python, Caddy, relais) et supprime les conteneurs qu’il a créés. Les conteneurs portent un nom qui commence par `cours-sd-`, et seuls ceux-là sont supprimés ; aucun script ne fait de nettoyage global de Docker. Si une exécution est interrompue avant la fin, par Ctrl-C par exemple, il peut rester un conteneur ou un processus :

```bash
docker ps -a --filter name=cours-sd-          # conteneurs restants du cours
docker rm -f cours-sd-ch10-redis
docker rm -f cours-sd-ch12-primaire cours-sd-ch12-replique-a cours-sd-ch12-replique-b
docker rm -f cours-sd-ch16-pg cours-sd-ch16-redis
docker rm -f cours-sd-ch17-pg cours-sd-ch17-redis cours-sd-ch17-n1 cours-sd-ch17-n2 cours-sd-ch17-n3 cours-sd-ch17-n4
docker rm -f cours-sd-ch18-pg
docker rm -f cours-sd-ch19-pg
docker rm -f cours-sd-ch21-pg cours-sd-ch21-redis cours-sd-ch21-mongo cours-sd-ch21-cassandra cours-sd-ch21-neo4j
docker rm -f cours-sd-ch22-timescale cours-sd-ch22-pgvector cours-sd-ch22-opensearch
docker rm -f cours-sd-ch23-pg cours-sd-ch23-crdb1 cours-sd-ch23-crdb2 cours-sd-ch23-crdb3
docker network rm cours-sd-ch23-reseau
docker rm -f -v cours-sd-ch24-primaire cours-sd-ch24-r1 cours-sd-ch24-r2
docker rm -f -v cours-sd-ch25-pga cours-sd-ch25-pgb cours-sd-ch25-c1 cours-sd-ch25-c2 cours-sd-ch25-c3
docker network rm cours-sd-ch25-reseau
docker rm -f -v cours-sd-ch26-coord cours-sd-ch26-w1 cours-sd-ch26-w2 cours-sd-ch26-w3
docker network rm cours-sd-ch26-reseau
docker rm -f -v cours-sd-ch27-rabbitmq cours-sd-ch27-kafka
docker rm -f -v cours-sd-ch28-rabbitmq cours-sd-ch28-kafka
rm -rf ~/.cache/cours-sd-ch28
docker rm -f -v cours-sd-ch29-seaweed
docker run --rm -v ~/.cache/cours-sd-ch29:/d --entrypoint sh chrislusf/seaweedfs -c 'rm -rf /d/*'
rm -rf ~/.cache/cours-sd-ch29
docker rm -f -v cours-sd-ch30-nginx
rm -rf ~/.cache/cours-sd-ch30
docker rm -f -v cours-sd-ch31-pg
docker rm -f -v cours-sd-ch33-cassandra
rm -rf ~/.cache/cours-sd-ch34
for i in 1 2 3 4 5; do docker rm -f -v cours-sd-ch35-e$i-outils cours-sd-ch35-e$i; done
docker network rm cours-sd-ch35-reseau
docker rmi cours-sd-ch35-outils
docker rm -f -v cours-sd-ch36-zk cours-sd-ch36-etcd cours-sd-ch36-consul
docker rm -f -v cours-sd-ch37-pg1 cours-sd-ch37-pg2 cours-sd-ch37-kafka
rm -rf ~/.cache/cours-sd-ch20                # données du ch. 20 si moteurs.py a été interrompu
pgrep -af "caddy run"                         # Caddy lancés par les scripts
```

Un Caddy installé comme service de la machine apparaît aussi dans la liste de `pgrep`, avec sa configuration système (`/etc/caddy/Caddyfile`). Celui-là n’appartient pas aux expériences et ne doit pas être arrêté. Les scripts écoutent sur des ports locaux, entre 6391 et 11793 selon les chapitres ; s’ils signalent un port déjà occupé, c’est souvent un reste d’exécution précédente ou un autre service de la machine.
