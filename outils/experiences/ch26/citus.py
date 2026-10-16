"""Chapitre 26 : une table partitionnée par hachage dans une grappe Citus.

Un coordinateur Citus (cours-sd-ch26-coord, publié sur le port 10632) et deux
nœuds de données (cours-sd-ch26-w1, w2), puis un troisième (w3), sur un réseau
Docker. La table commandes est répartie par hachage de client_id en 32
partitions (shards) ; une copie ordinaire de la même table, sur le coordinateur
seul, sert de comparaison. 20 000 clients, 100 commandes chacun.
  1. répartition des partitions et des données entre les nœuds ;
  2. quatre requêtes, durée médiane et nombre de partitions interrogées
     (« Task Count » du plan) : une commande par sa clé complète, toutes les
     commandes d'un client, les commandes d'un produit (index secondaire, local
     à chaque partition), une somme par produit sur toute la table ;
  3. ajout de w3 et rééquilibrage (citus_rebalance_start) pendant qu'un client
     insère en continu : durée, partitions déplacées, erreurs et pire latence
     des insertions ;
  4. un client devient énorme (500 000 commandes) : taille de sa partition,
     puis isolement du client dans une partition à lui
     (isolate_tenant_to_new_shard, en bloquant les écritures sur la partition
     pendant l'opération ; en mode en ligne, par réplication logique, le
     découpage est resté bloqué plus de 12 minutes sur la machine des mesures).
Prérequis : docker, image citusdata/citus, pip install -r requirements.txt.
Usage : python3 citus.py
"""
import re
import statistics
import subprocess
import threading
import time

import psycopg

RESEAU, NOEUDS = "cours-sd-ch26-reseau", ["coord", "w1", "w2", "w3"]
DSN = "host=127.0.0.1 port=10632 dbname=postgres user=postgres"
CLIENTS, PAR_CLIENT = 20_000, 100


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def nettoyer():
    subprocess.run(["docker", "rm", "-f", "-v", *(f"cours-sd-ch26-{n}" for n in NOEUDS)], capture_output=True)
    subprocess.run(["docker", "network", "rm", RESEAU], capture_output=True)


def lancer(nom):
    publier = ["-p", "10632:5432"] if nom == "coord" else []
    docker("run", "-d", "--name", f"cours-sd-ch26-{nom}", "--hostname", f"cours-sd-ch26-{nom}",
           "--network", RESEAU, *publier, "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
           "citusdata/citus:latest", "-c", "wal_level=logical", "-c", "shared_buffers=256MB")


def attendre_pg(nom, delai=120):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        if docker("exec", f"cours-sd-ch26-{nom}", "pg_isready", "-U", "postgres", check=False).returncode == 0:
            r = docker("exec", f"cours-sd-ch26-{nom}", "psql", "-U", "postgres", "-Atc",
                       "SELECT count(*) FROM pg_extension WHERE extname = 'citus'", check=False)
            if r.stdout.strip() == "1":
                return
        time.sleep(1)
    raise RuntimeError(f"{nom} ne répond pas")


def connecter():
    fin = time.monotonic() + 60
    while True:
        try:
            return psycopg.connect(DSN, autocommit=True)
        except psycopg.OperationalError:
            if time.monotonic() > fin:
                raise
            time.sleep(0.5)


def chrono(cx, sql, args, n):
    durees = []
    for i in range(n):
        t = time.perf_counter()
        cx.execute(sql, args(i)).fetchall()
        durees.append(1000 * (time.perf_counter() - t))
    return statistics.median(durees)


def taches(cx, sql, args):
    plan = "\n".join(l for l, in cx.execute("EXPLAIN " + sql, args).fetchall())
    m = re.search(r"Task Count: (\d+)", plan)
    return int(m.group(1)) if m else 1


def repartition(cx):
    return cx.execute("""SELECT nodename, count(*), sum(shard_size) FROM citus_shards
                         WHERE table_name = 'commandes'::regclass GROUP BY nodename ORDER BY nodename""").fetchall()


def afficher_repartition(cx):
    for nom, n, octets in repartition(cx):
        print(f"   {nom.removeprefix('cours-sd-ch26-'):<6}{n:>4} partitions{octets / 2**20:>8.0f} Mo")


def main():
    nettoyer()
    try:
        docker("network", "create", RESEAU)
        for n in ("coord", "w1", "w2"):
            lancer(n)
        for n in ("coord", "w1", "w2"):
            attendre_pg(n)
        cx = connecter()
        cx.execute("SELECT citus_set_coordinator_host('cours-sd-ch26-coord', 5432)")
        for n in ("w1", "w2"):
            cx.execute("SELECT citus_add_node(%s, 5432)", (f"cours-sd-ch26-{n}",))
        version, = cx.execute("SELECT extversion FROM pg_extension WHERE extname = 'citus'").fetchone()
        pg, = cx.execute("SHOW server_version").fetchone()
        print(f"Citus {version} sur PostgreSQL {pg.split()[0]}, un coordinateur et deux nœuds")

        for table in ("commandes", "commandes_locale"):
            cx.execute(f"""CREATE TABLE {table} (client_id int, id bigint, produit int, montant int,
                           cree timestamptz DEFAULT now(), PRIMARY KEY (client_id, id))""")
        cx.execute("SET citus.shard_count = 32")
        cx.execute("SELECT create_distributed_table('commandes', 'client_id')")
        t = time.perf_counter()
        remplir = f"""SELECT c, c::bigint * 1000 + k, (c * 7 + k * 13) % 5000, (c + k) % 500
                      FROM generate_series(1, {CLIENTS}) c, generate_series(1, {PAR_CLIENT}) k"""
        cx.execute(f"INSERT INTO commandes (client_id, id, produit, montant) {remplir}")
        d_dist = time.perf_counter() - t
        t = time.perf_counter()
        cx.execute(f"INSERT INTO commandes_locale (client_id, id, produit, montant) {remplir}")
        d_loc = time.perf_counter() - t
        for table in ("commandes", "commandes_locale"):
            cx.execute(f"CREATE INDEX ON {table} (produit)")
            cx.execute(f"VACUUM ANALYZE {table}")
        print(f"2 000 000 commandes chargées en {d_dist:.0f} s (table répartie), {d_loc:.0f} s (locale)")

        print("\n1. répartition de la table commandes (32 partitions par hachage de client_id)")
        afficher_repartition(cx)

        print("\n2. requêtes, durée médiane (200 essais) et partitions interrogées")
        print(f"   {'requête':<29}{'répartie':>12}{'locale':>11}{'partitions':>11}")
        requetes = [
            ("une commande par sa clé", "SELECT * FROM {t} WHERE client_id = %s AND id = %s",
             lambda i: (i % CLIENTS + 1, (i % CLIENTS + 1) * 1000 + 5), 200),
            ("les commandes d'un client", "SELECT * FROM {t} WHERE client_id = %s",
             lambda i: (i % CLIENTS + 1,), 200),
            ("les commandes d'un produit", "SELECT * FROM {t} WHERE produit = %s",
             lambda i: (i % 5000,), 200),
            ("somme par produit", "SELECT produit, sum(montant) FROM {t} GROUP BY produit",
             lambda i: (), 10),
        ]
        for nom, sql, args, n in requetes:
            dist = chrono(cx, sql.format(t="commandes"), args, n)
            loc = chrono(cx, sql.format(t="commandes_locale"), args, n)
            nb = taches(cx, sql.format(t="commandes"), args(0))
            print(f"   {nom:<29}{dist:>9.1f} ms{loc:>8.1f} ms{nb:>11}")

        print("\n3. ajout de w3 et rééquilibrage, pendant des insertions continues")
        lancer("w3")
        attendre_pg("w3")
        cx.execute("SELECT citus_add_node('cours-sd-ch26-w3', 5432)")
        avant = dict(cx.execute("""SELECT shardid, nodename FROM citus_shards
                                   WHERE table_name = 'commandes'::regclass""").fetchall())
        arret, durees, erreurs = threading.Event(), [], [0]

        def inserer():
            with psycopg.connect(DSN, autocommit=True) as c:
                k = 0
                while not arret.is_set():
                    t = time.perf_counter()
                    try:
                        c.execute("INSERT INTO commandes (client_id, id, produit, montant) VALUES (%s, %s, 1, 1)",
                                  (k % CLIENTS + 1, 10**9 + k))
                        durees.append(1000 * (time.perf_counter() - t))
                    except psycopg.Error:
                        erreurs[0] += 1
                    k += 1

        fil = threading.Thread(target=inserer)
        fil.start()
        time.sleep(2)
        t = time.perf_counter()
        cx.execute("SELECT citus_rebalance_start()")
        cx.execute("SELECT citus_rebalance_wait()")
        d = time.perf_counter() - t
        time.sleep(2)
        arret.set()
        fil.join()
        apres = dict(cx.execute("""SELECT shardid, nodename FROM citus_shards
                                   WHERE table_name = 'commandes'::regclass""").fetchall())
        bouges = [s for s in avant if apres.get(s) != avant[s]]
        taille, = cx.execute("SELECT coalesce(sum(shard_size), 0) FROM citus_shards WHERE shardid = ANY(%s)",
                             (bouges,)).fetchone()
        durees.sort()
        print(f"   rééquilibrage en {d:.0f} s : {len(bouges)} partitions déplacées sur 32,"
              f" {taille / 2**20:.0f} Mo")
        afficher_repartition(cx)
        print(f"   insertions pendant ce temps : {len(durees)}, {erreurs[0]} erreur{'s' if erreurs[0] > 1 else ''},")
        print(f"   médiane {statistics.median(durees):.1f} ms, pire {durees[-1]:.0f} ms")

        print("\n4. le client 0 passe à 500 000 commandes")
        cx.execute("""INSERT INTO commandes (client_id, id, produit, montant)
                      SELECT 0, k, k % 5000, k % 500 FROM generate_series(1, 500000) k""")
        tailles = [s for s, in cx.execute("""SELECT shard_size FROM citus_shards
                                             WHERE table_name = 'commandes'::regclass""").fetchall()]
        sa, = cx.execute("""SELECT shard_size FROM citus_shards WHERE shardid =
                            get_shard_id_for_distribution_column('commandes', 0)""").fetchone()
        print(f"   partition du client 0 : {sa / 2**20:.0f} Mo ; médiane des autres :"
              f" {statistics.median(tailles) / 2**20:.0f} Mo")
        t = time.perf_counter()
        nouvelle, = cx.execute("SELECT isolate_tenant_to_new_shard('commandes', 0, 'CASCADE',"
                               " shard_transfer_mode => 'block_writes')").fetchone()
        d = time.perf_counter() - t
        n, = cx.execute("SELECT count(*) FROM citus_shards WHERE table_name = 'commandes'::regclass").fetchone()
        seule, = cx.execute("SELECT shard_size FROM citus_shards WHERE shardid = %s", (nouvelle,)).fetchone()
        print(f"   isolement du client 0 en {d:.1f} s : {n} partitions, celle du client 0 fait"
              f" {seule / 2**20:.0f} Mo")
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
