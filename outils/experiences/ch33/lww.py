"""Chapitre 33 : « la dernière écriture gagne » quand les horloges divergent.

Cassandra 5 (un nœud, conteneur cours-sd-ch33-cassandra, port 11342) range
chaque cellule avec un horodatage en microsecondes, et garde celle dont
l'horodatage est le plus grand. Le pilote Python fixe cet horodatage côté
client, à partir de time.time() (MonotonicTimestampGenerator). Deux clients
écrivent la même clé ; le second écrit après le premier, en temps réel, mais
son horloge retarde de d : on le simule par un générateur d'horodatages
décalé de d, ce que produirait un client dont l'horloge est en retard.

    python3 lww.py
"""
import subprocess
import time

from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.timestamps import MonotonicTimestampGenerator

NOM, PORT = "cours-sd-ch33-cassandra", 11342


class Decale(MonotonicTimestampGenerator):
    """Le générateur du pilote, avec une horloge en retard de d secondes."""

    def __init__(self, retard):
        super().__init__()
        self.retard = retard

    def _next_timestamp(self, now, last):
        self.dernier = super()._next_timestamp(
            now - int(self.retard * 1e6), last)
        return self.dernier


def lancer():
    arreter()
    subprocess.run(["docker", "run", "-d", "--name", NOM, "-p",
                    f"{PORT}:9042", "-e", "MAX_HEAP_SIZE=512M",
                    "-e", "HEAP_NEWSIZE=128M", "cassandra:5"],
                   check=True, stdout=subprocess.DEVNULL)
    fin = time.monotonic() + 180
    while time.monotonic() < fin:
        try:
            c = Cluster(["127.0.0.1"], port=PORT,
                        connection_class=AsyncioConnection)
            c.connect().shutdown()
            c.shutdown()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Cassandra ne répond pas")


def arreter():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)


def session(retard=0.0):
    c = Cluster(["127.0.0.1"], port=PORT, connection_class=AsyncioConnection,
                timestamp_generator=Decale(retard))
    s = c.connect("cours")
    return c, s


def main():
    lancer()
    try:
        c = Cluster(["127.0.0.1"], port=PORT,
                    connection_class=AsyncioConnection)
        admin = c.connect()
        admin.execute("create keyspace cours with replication = "
                      "{'class': 'SimpleStrategy', 'replication_factor': 1}")
        admin.execute("create table cours.kv (k text primary key, v text)")
        version = admin.execute("select release_version from system.local"
                                ).one()[0]
        # les clusters partagent la boucle du réacteur asyncio : on ne ferme
        # rien avant la fin
        clusters = [c]
        print(f"Cassandra {version} ; le client A écrit, puis le client B "
              f"écrit 100 ms plus tard")
        print(f"   {'retard de B':>12} {'tB - tA':>10} {'valeur lue':>11}  "
              f"écriture perdue")
        ca, a = session(0.0)
        clusters.append(ca)
        for i, retard in enumerate([0.0, 0.005, 0.05, 0.1, 0.15, 0.5,
                                    2.0]):
            cb, b = session(retard)
            clusters.append(cb)
            k = f"k{i}"
            a.execute("insert into kv (k, v) values (%s, 'A')", (k,))
            ta = ca.timestamp_generator.dernier
            time.sleep(0.1)
            b.execute("insert into kv (k, v) values (%s, 'B')", (k,))
            tb = cb.timestamp_generator.dernier
            lu = a.execute("select v from kv where k = %s", (k,)).one()[0]
            perdue = "B, la plus récente" if lu == "A" else "A"
            print(f"   {retard * 1000:9.0f} ms {(tb - ta) / 1000:+7.1f} ms "
                  f"{lu:>11}  {perdue}")
        print("\n   égalité d'horodatage : deux écritures au même instant")
        for premier, second in [("aaa", "zzz"), ("zzz", "aaa")]:
            k = f"egal-{premier}"
            t = int(time.time() * 1e6)
            a.execute(f"insert into kv (k, v) values (%s, %s) using timestamp {t}",
                      (k, premier))
            a.execute(f"insert into kv (k, v) values (%s, %s) using timestamp {t}",
                      (k, second))
            lu = a.execute("select v from kv where k = %s", (k,)).one()[0]
            print(f"   « {premier} » puis « {second} » : on lit « {lu} »")
        for x in clusters:
            x.shutdown()
    finally:
        arreter()


if __name__ == "__main__":
    main()
