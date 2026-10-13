// Plan du cours : une seule source pour la page d'accueil et le cartouche des
// chapitres. Un chapitre rédigé reçoit l'identifiant de sa page (« doc ») :
// son lien s'active sur l'accueil. Le plan détaillé est dans PLAN.md.
export type Chapitre = {num: number; titre: string; doc?: string};
export type Partie = {num: number; titre: string; resume: string; chapitres: Chapitre[]};

export const PARTIES: Partie[] = [
  {
    num: 0,
    titre: 'Méthode',
    resume: 'La démarche de conception et l’estimation de charge, utilisées dans tout le reste du cours.',
    chapitres: [
      {num: 1, titre: 'La démarche de conception', doc: 'partie-0/demarche'},
      {num: 2, titre: 'Estimer la charge', doc: 'partie-0/estimation'},
    ],
  },
  {
    num: 1,
    titre: 'Fondamentaux',
    resume: 'Le réseau, les protocoles, les styles d’API, la latence, la scalabilité, la disponibilité et la cohérence.',
    chapitres: [
      {num: 3, titre: 'Client-serveur et DNS', doc: 'partie-1/client-serveur-dns'},
      {num: 4, titre: 'Transport : TCP et UDP', doc: 'partie-1/transport'},
      {num: 5, titre: 'HTTP/1.1, HTTP/2, HTTP/3 et QUIC', doc: 'partie-1/http'},
      {num: 6, titre: 'TLS et HTTPS', doc: 'partie-1/tls'},
      {num: 7, titre: 'Styles d’API : REST, GraphQL, gRPC', doc: 'partie-1/styles-api'},
      {num: 8, titre: 'Pousser des données vers le client', doc: 'partie-1/pousser-donnees'},
      {num: 9, titre: 'Latence, débit et chiffres à connaître', doc: 'partie-1/latence-debit'},
      {num: 10, titre: 'Passer à l’échelle : vertical, horizontal, sans état', doc: 'partie-1/passer-echelle'},
      {num: 11, titre: 'Disponibilité et fiabilité', doc: 'partie-1/disponibilite'},
      {num: 12, titre: 'Cohérence, CAP et PACELC', doc: 'partie-1/coherence-cap'},
    ],
  },
  {
    num: 2,
    titre: 'Briques de base',
    resume: 'Répartiteurs, CDN, caches, bases de données, réplication, partitionnement, files de messages, stockage, limitation de débit, identifiants.',
    chapitres: [
      {num: 13, titre: 'Répartition de charge', doc: 'partie-2/repartition-charge'},
      {num: 14, titre: 'Reverse proxy et API gateway', doc: 'partie-2/proxy-passerelle'},
      {num: 15, titre: 'CDN et edge computing', doc: 'partie-2/cdn-edge'},
      {num: 16, titre: 'Cache (1) : placement et stratégies', doc: 'partie-2/cache-strategies'},
      {num: 17, titre: 'Cache (2) : invalidation, stampede, hot keys', doc: 'partie-2/cache-invalidation'},
      {num: 18, titre: 'Bases relationnelles (1) : transactions et isolation', doc: 'partie-2/transactions-isolation'},
      {num: 19, titre: 'Bases relationnelles (2) : index et modélisation', doc: 'partie-2/index-modelisation'},
      {num: 20, titre: 'Moteurs de stockage : B-tree, LSM, SSTables'},
      {num: 21, titre: 'NoSQL (1) : clé-valeur, document, colonnes larges, graphe'},
      {num: 22, titre: 'NoSQL (2) : séries temporelles, recherche, vecteurs'},
      {num: 23, titre: 'Choisir une base de données'},
      {num: 24, titre: 'Réplication (1) : leader-follower'},
      {num: 25, titre: 'Réplication (2) : multi-leader, sans leader, quorums'},
      {num: 26, titre: 'Partitionnement (sharding)'},
      {num: 27, titre: 'Files de messages et logs'},
      {num: 28, titre: 'Garanties de livraison'},
      {num: 29, titre: 'Stockage objet, bloc, fichier'},
      {num: 30, titre: 'Limitation de débit (rate limiting)'},
      {num: 31, titre: 'Identifiants uniques'},
    ],
  },
  {
    num: 3,
    titre: 'Systèmes distribués',
    resume: 'Pannes partielles, temps, consensus, coordination, transactions distribuées, résilience, structures probabilistes, convergence.',
    chapitres: [
      {num: 32, titre: 'Pannes partielles et détection de panne'},
      {num: 33, titre: 'Le temps dans un système distribué'},
      {num: 34, titre: 'Consensus (1) : le problème et Paxos'},
      {num: 35, titre: 'Consensus (2) : Raft en détail'},
      {num: 36, titre: 'Coordination : ZooKeeper, etcd, service discovery'},
      {num: 37, titre: 'Transactions distribuées : 2PC, sagas, outbox'},
      {num: 38, titre: 'Résilience (1) : idempotence, retries, timeouts'},
      {num: 39, titre: 'Résilience (2) : circuit breaker, bulkhead, back-pressure'},
      {num: 40, titre: 'Structures probabilistes'},
      {num: 41, titre: 'Faire converger des répliques : Merkle et CRDT'},
    ],
  },
  {
    num: 4,
    titre: 'Styles d’architecture',
    resume: 'Du monolithe aux microservices, événements, serverless, cellules, multi-région, architectures de données.',
    chapitres: [
      {num: 42, titre: 'Monolithe, monolithe modulaire, SOA, microservices'},
      {num: 43, titre: 'Architecture orientée événements, CQRS, event sourcing'},
      {num: 44, titre: 'Serverless'},
      {num: 45, titre: 'Architecture cellulaire'},
      {num: 46, titre: 'Multi-région'},
      {num: 47, titre: 'Traitement par lots : MapReduce, Spark'},
      {num: 48, titre: 'Traitement de flux : Flink, Lambda, Kappa'},
      {num: 49, titre: 'Entrepôts, lacs, lakehouse'},
    ],
  },
  {
    num: 5,
    titre: 'Exploitation en production',
    resume: 'Observabilité, déploiement, reprise après sinistre, sécurité, coûts.',
    chapitres: [
      {num: 50, titre: 'Observabilité'},
      {num: 51, titre: 'Conteneurs, Kubernetes et autoscaling'},
      {num: 52, titre: 'Mettre en production sans coupure'},
      {num: 53, titre: 'Reprise après sinistre'},
      {num: 54, titre: 'Sécurité (1) : authentification'},
      {num: 55, titre: 'Sécurité (2) : autorisation, chiffrement, secrets, abus'},
      {num: 56, titre: 'Coûts'},
    ],
  },
  {
    num: 6,
    titre: 'Études de cas',
    resume: 'Vingt-six systèmes conçus de bout en bout avec la démarche du chapitre 1.',
    chapitres: [
      {num: 57, titre: 'Raccourcisseur d’URL et Pastebin'},
      {num: 58, titre: 'Générateur d’identifiants uniques distribué'},
      {num: 59, titre: 'Rate limiter distribué'},
      {num: 60, titre: 'Base clé-valeur distribuée (type Dynamo)'},
      {num: 61, titre: 'Web crawler'},
      {num: 62, titre: 'Système de notifications'},
      {num: 63, titre: 'Service d’e-mail'},
      {num: 64, titre: 'Fil d’actualité'},
      {num: 65, titre: 'Messagerie instantanée'},
      {num: 66, titre: 'Autocomplétion'},
      {num: 67, titre: 'Moteur de recherche'},
      {num: 68, titre: 'Streaming vidéo'},
      {num: 69, titre: 'Stockage et synchronisation de fichiers'},
      {num: 70, titre: 'Stockage objet (type S3)'},
      {num: 71, titre: 'Services de proximité'},
      {num: 72, titre: 'Cartographie et itinéraires'},
      {num: 73, titre: 'Billetterie à forte concurrence'},
      {num: 74, titre: 'Réservation d’hôtel'},
      {num: 75, titre: 'Paiement et portefeuille numérique'},
      {num: 76, titre: 'Bourse et moteur d’appariement'},
      {num: 77, titre: 'Agrégation de clics publicitaires'},
      {num: 78, titre: 'Classement en temps réel'},
      {num: 79, titre: 'Ordonnanceur de tâches distribué'},
      {num: 80, titre: 'Monitoring et métriques'},
      {num: 81, titre: 'Servir un LLM en production'},
      {num: 82, titre: 'Système RAG'},
    ],
  },
  {
    num: 7,
    titre: 'Low-level design',
    resume: 'Conception objet, patrons utiles et cinq exercices classiques d’entretien, en Python.',
    chapitres: [
      {num: 83, titre: 'Principes de conception objet et SOLID'},
      {num: 84, titre: 'Patrons de conception utiles'},
      {num: 85, titre: 'Méthode d’entretien LLD et parking'},
      {num: 86, titre: 'Distributeur automatique'},
      {num: 87, titre: 'Ascenseur'},
      {num: 88, titre: 'Système de réservation'},
      {num: 89, titre: 'Cache LRU et rate limiter en code'},
    ],
  },
];

export function partie(num: number | undefined): Partie | undefined {
  return PARTIES.find((p) => p.num === num);
}

export function numeroChapitre(num: number): string {
  return String(num).padStart(2, '0');
}
