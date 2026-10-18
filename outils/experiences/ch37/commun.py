"""Chapitre 37 : deux serveurs PostgreSQL et un courtier Kafka, dans des conteneurs.

Tous en réseau hôte, sur 127.0.0.1 :
  - cours-sd-ch37-pg1 et cours-sd-ch37-pg2, PostgreSQL 18, ports 11701 et 11702,
    avec max_prepared_transactions (validation à deux phases) et wal_level=logical
    (capture des changements) ;
  - cours-sd-ch37-kafka, Kafka en mode KRaft, un nœud, port 11792.
Un relais écrit en Python peut retarder les octets vers chaque serveur, pour imiter
des serveurs éloignés. Les conteneurs sont supprimés par leur nom, avec leurs
volumes.
"""
import asyncio
import statistics
import subprocess
import threading
import time

import psycopg

IMAGE = "postgres:18-alpine"
PG = {"pg1": 11701, "pg2": 11702}
KAFKA = "127.0.0.1:11792"


def dsn(port, base="postgres", **options):
    texte = f"host=127.0.0.1 port={port} dbname={base} user=postgres"
    return " ".join([texte, *(f"{k}={v}" for k, v in options.items())])


def connecter(port, base="postgres", autocommit=True):
    return psycopg.connect(dsn(port, base), autocommit=autocommit)


def nombre(x):
    return f"{x:,.0f}".replace(",", " ")


def centiles(durees):
    durees = sorted(durees)
    return statistics.median(durees), durees[int(0.99 * (len(durees) - 1))]


# ---- relais à délai ------------------------------------------------------------
class Relais:
    """Relais TCP qui retarde chaque octet de `delai` secondes dans chaque sens.
    Coupé, il retient les octets sans fermer les connexions, comme un réseau qui
    ne répond plus. Il tourne dans un fil à lui, avec sa propre boucle asyncio."""

    def __init__(self, port_ecoute, port_cible, delai=0.0):
        self.port_ecoute, self.port_cible, self.delai = port_ecoute, port_cible, delai
        self.generation, self.connexions = 0, []
        self.boucle = asyncio.new_event_loop()
        pret = threading.Event()
        threading.Thread(target=self._tourner, args=(pret,), daemon=True).start()
        pret.wait()

    def _tourner(self, pret):
        asyncio.set_event_loop(self.boucle)
        self.ouvert = asyncio.Event()
        self.ouvert.set()
        self.boucle.run_until_complete(asyncio.start_server(self._accueillir, "127.0.0.1", self.port_ecoute))
        pret.set()
        self.boucle.run_forever()

    async def _accueillir(self, lecteur, ecrivain):
        try:
            l2, e2 = await asyncio.open_connection("127.0.0.1", self.port_cible)
        except OSError:
            ecrivain.close()
            return
        gen = self.generation
        self.connexions.append((gen, ecrivain, e2))
        await asyncio.gather(self._transmettre(lecteur, e2, gen), self._transmettre(l2, ecrivain, gen),
                             return_exceptions=True)
        for e in (ecrivain, e2):
            e.close()

    async def _transmettre(self, lecteur, ecrivain, gen):
        file = asyncio.Queue()

        async def livrer():
            while True:
                echeance, donnees = await file.get()
                if donnees is None:
                    return
                attente = echeance - time.monotonic()
                if attente > 0:
                    await asyncio.sleep(attente)
                await self.ouvert.wait()
                if gen != self.generation:      # liaison rompue : les octets en route sont perdus
                    return
                ecrivain.write(donnees)
                await ecrivain.drain()

        livreur = asyncio.create_task(livrer())
        try:
            while True:
                donnees = await lecteur.read(65536)
                if not donnees:
                    break
                file.put_nowait((time.monotonic() + self.delai, donnees))
        finally:
            file.put_nowait((0, None))
            await livreur
            ecrivain.close()

    def couper(self):
        self.boucle.call_soon_threadsafe(self.ouvert.clear)

    def retablir(self):
        self.boucle.call_soon_threadsafe(self.ouvert.set)

    def rompre(self):
        """Ferme les connexions en cours en jetant les octets retenus ou en route ;
        les nouvelles connexions passent normalement."""
        def faire():
            self.generation += 1
            for gen, a, b in self.connexions:
                if gen < self.generation:
                    a.close()
                    b.close()
            self.connexions = [c for c in self.connexions if c[0] == self.generation]
            self.ouvert.set()
        self.boucle.call_soon_threadsafe(faire)
        time.sleep(0.2)


# ---- conteneurs -----------------------------------------------------------------
def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def supprimer(*noms):
    subprocess.run(["docker", "rm", "-f", "-v", *(f"cours-sd-ch37-{n}" for n in noms)],
                   capture_output=True)


def lancer_pg(*noms):
    supprimer(*noms)
    for nom in noms:
        docker("run", "-d", "--name", f"cours-sd-ch37-{nom}", "--network", "host",
               "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGE,
               "-c", f"port={PG[nom]}", "-c", "listen_addresses=127.0.0.1",
               "-c", "max_prepared_transactions=50", "-c", "wal_level=logical",
               "-c", "max_replication_slots=4", "-c", "max_connections=200")
    for nom in noms:
        attendre_pg(PG[nom])


def attendre_pg(port, delai=60):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            with connecter(port) as c:
                c.execute("select 1")
            # l'image redémarre le serveur après l'initialisation : attendre la
            # seconde mise en route
            time.sleep(0.5)
            with connecter(port) as c:
                c.execute("select 1")
            return
        except psycopg.OperationalError:
            time.sleep(0.3)
    raise RuntimeError(f"PostgreSQL {port} ne répond pas")


def lancer_kafka(delai=120):
    supprimer("kafka")
    env = {
        "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://127.0.0.1:11792,CONTROLLER://127.0.0.1:11793",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{KAFKA}",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@127.0.0.1:11793",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_HEAP_OPTS": "-Xmx768m -Xms768m",
        "KAFKA_LOG_DIRS": "/var/lib/kafka/journal",
    }
    args = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
    docker("run", "-d", "--name", "cours-sd-ch37-kafka", "--network", "host", *args,
           "apache/kafka:latest")
    from confluent_kafka.admin import AdminClient
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            AdminClient({"bootstrap.servers": KAFKA, "log_level": 0}).list_topics(timeout=2)
            return docker("exec", "cours-sd-ch37-kafka", "sh", "-c",
                          "ls /opt/kafka/libs | grep -o 'kafka-clients-[0-9.]*' | head -1",
                          check=False).stdout.strip().removeprefix("kafka-clients-").rstrip(".")
        except Exception:
            time.sleep(1)
    raise RuntimeError("Kafka ne répond pas")


def creer_sujet(nom, partitions=1):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": KAFKA, "log_level": 0})
    for f in admin.create_topics([NewTopic(nom, partitions, 1)]).values():
        try:
            f.result(30)
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" not in str(e):
                raise
    return admin


def lire_sujet(nom, delai=5.0):
    """Tous les messages du sujet, du début : liste de (clé, valeur, horodatage ms)."""
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"lecture-{time.time_ns()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False,
                  "log_level": 0})
    c.subscribe([nom])
    messages, calme = [], time.monotonic()
    while time.monotonic() - calme < delai:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        messages.append((m.key().decode() if m.key() else None, m.value().decode(),
                         m.timestamp()[1]))
        calme = time.monotonic()
    c.close()
    return messages
