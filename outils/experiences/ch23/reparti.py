"""Chapitre 23 : le prix et le gain d'une base SQL répartie.

Une table de 100 000 comptes est créée dans PostgreSQL 18 (une seule machine)
et dans une grappe CockroachDB de trois nœuds, chacun dans son conteneur, sur la
même machine. CockroachDB réplique chaque donnée sur trois nœuds par Raft
(ch. 35) : une écriture n'est confirmée qu'après l'accord d'une majorité.
  1. latence d'une écriture (UPDATE d'un compte, validée seule) et d'une lecture
     par clé, l'une après l'autre : médiane et 99e centile sur 1 000 ;
  2. débit d'écritures avec 32 connexions pendant 10 secondes ;
  3. le nœud de CockroachDB qui détient le bail (lease) de la table est arrêté
     brutalement : les écritures continuent-elles sur les deux autres, après quelle
     interruption, et avec quelle latence ? Puis le nœud redémarre.
Prérequis : docker, images postgres:18-alpine et cockroachdb/cockroach:latest,
pip install -r requirements.txt. Usage : python3 reparti.py
"""
import asyncio
import random
import statistics
import subprocess
import time

import psycopg

PG = "host=127.0.0.1 port=10332 dbname=cours user=postgres password=cours"
CRDB = "host=127.0.0.1 port={port} dbname=defaultdb user=root sslmode=disable"
RESEAU, NOEUDS, COMPTES = "cours-sd-ch23-reseau", 3, 100_000


def nombre(x):
    return f"{x:,.0f}".replace(",", " ")


def docker(*args, check=False):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check)


def nettoyer():
    docker("rm", "-f", "cours-sd-ch23-pg", *[f"cours-sd-ch23-crdb{i}" for i in range(1, NOEUDS + 1)])
    docker("network", "rm", RESEAU)


def lancer_crdb(i):
    noms = ",".join(f"cours-sd-ch23-crdb{j}" for j in range(1, NOEUDS + 1))
    docker("run", "-d", "--name", f"cours-sd-ch23-crdb{i}", "--hostname", f"cours-sd-ch23-crdb{i}",
           "--network", RESEAU, "-p", f"{10356 + i}:26257", "cockroachdb/cockroach:latest", "start",
           "--insecure", f"--join={noms}", "--cache=256MiB", "--max-sql-memory=256MiB", check=True)


def connecter(dsn, delai=120):
    fin = time.monotonic() + delai
    while True:
        try:
            return psycopg.connect(dsn, autocommit=True)
        except psycopg.OperationalError:
            if time.monotonic() > fin:
                raise
            time.sleep(1)


def centiles(durees):
    durees = sorted(durees)
    return statistics.median(durees), durees[int(0.99 * len(durees))]


def sequentiel(cx):
    alea = random.Random(23)
    ecr, lec = [], []
    for _ in range(1000):
        i = alea.randrange(COMPTES)
        t = time.perf_counter()
        cx.execute("UPDATE comptes SET solde = solde + 1 WHERE id = %s", (i,))
        ecr.append(1000 * (time.perf_counter() - t))
        t = time.perf_counter()
        cx.execute("SELECT solde FROM comptes WHERE id = %s", (i,)).fetchone()
        lec.append(1000 * (time.perf_counter() - t))
    return centiles(ecr), centiles(lec)


async def debit(dsns, duree=10, connexions=32):
    debut = time.monotonic()
    fin, n, durees, erreurs, secondes = debut + duree, 0, [], 0, [0] * duree

    async def client(dsn, graine):
        nonlocal n, erreurs
        alea = random.Random(graine)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as cx:
            while time.monotonic() < fin:
                t = time.perf_counter()
                try:
                    await cx.execute("UPDATE comptes SET solde = solde + 1 WHERE id = %s",
                                     (alea.randrange(COMPTES),))
                    durees.append(1000 * (time.perf_counter() - t))
                    n += 1
                    secondes[min(int(time.monotonic() - debut), duree - 1)] += 1
                except psycopg.Error:
                    erreurs += 1

    await asyncio.gather(*(client(dsns[k % len(dsns)], k) for k in range(connexions)))
    debit.secondes = secondes
    return n / duree, centiles(durees), erreurs, max(durees)


def preparer(cx):
    cx.execute("CREATE TABLE comptes (id int PRIMARY KEY, solde int NOT NULL DEFAULT 0)")
    cx.execute(f"INSERT INTO comptes (id) SELECT generate_series(0, {COMPTES - 1})")


def ligne(nom, ecr, lec):
    print(f"   {nom:<26}{ecr[0]:>8.2f}{ecr[1]:>8.2f}{lec[0]:>10.2f}{lec[1]:>8.2f}")


def main():
    nettoyer()
    try:
        docker("run", "-d", "--name", "cours-sd-ch23-pg", "-p", "10332:5432", "-e", "POSTGRES_PASSWORD=cours",
               "-e", "POSTGRES_DB=cours", "postgres:18-alpine", check=True)
        docker("network", "create", RESEAU, check=True)
        for i in range(1, NOEUDS + 1):
            lancer_crdb(i)
        time.sleep(3)
        docker("exec", "cours-sd-ch23-crdb1", "./cockroach", "init", "--insecure", check=True)
        pg = connecter(PG)
        crdb = connecter(CRDB.format(port=10357))
        version = crdb.execute("SELECT version()").fetchone()[0].split()[:3]
        preparer(pg)
        preparer(crdb)
        print(f"{nombre(COMPTES)} comptes ; PostgreSQL 18 et {' '.join(version)}, {NOEUDS} nœuds")

        print("\n1. une opération à la fois, en ms (médiane, 99e centile, sur 1 000)")
        print(f"   {'':<26}{'écriture':>16}{'lecture':>18}")
        ligne("PostgreSQL", *sequentiel(pg))
        ligne("CockroachDB, 3 nœuds", *sequentiel(crdb))

        print("\n2. écritures avec 32 connexions pendant 10 s")
        dsn_crdb = [CRDB.format(port=10356 + i) for i in range(1, NOEUDS + 1)]
        for nom, dsns in (("PostgreSQL", [PG]), ("CockroachDB, 3 nœuds", dsn_crdb)):
            par_s, (med, p99), err, _ = asyncio.run(debit(dsns))
            print(f"   {nom:<26}{nombre(par_s):>7} /s   médiane {med:5.1f} ms   p99 {p99:6.1f} ms")

        bail, = crdb.execute("SELECT lease_holder FROM [SHOW RANGE FROM TABLE comptes FOR ROW (0)]").fetchone()
        etat = docker("exec", "cours-sd-ch23-crdb1", "./cockroach", "node", "status", "--insecure",
                      "--format=csv", check=True).stdout.splitlines()
        adresse = next(l.split(",")[1] for l in etat[1:] if l.split(",")[0] == str(bail))
        v = int(adresse.split(":")[0].removeprefix("cours-sd-ch23-crdb"))
        autres = [d for i, d in enumerate(dsn_crdb, 1) if i != v]
        crdb.close()
        print(f"\n3. arrêt brutal (docker kill) du nœud {v}, qui détient le bail de la table")
        docker("kill", f"cours-sd-ch23-crdb{v}")
        par_s, (med, p99), err, pire = asyncio.run(debit(autres, duree=20))
        print(f"   20 s d'écritures sur les deux autres nœuds : {nombre(par_s)} /s")
        print(f"   médiane {med:.1f} ms, p99 {p99:.1f} ms, maximum {pire / 1000:.1f} s,"
              f" {err} erreur{'s' if err > 1 else ''}")
        print("   écritures achevées pendant chaque seconde :")
        for k in range(0, 20, 10):
            print("     " + " ".join(f"{x:>5}" for x in debit.secondes[k:k + 10]))
        docker("start", f"cours-sd-ch23-crdb{v}")
        t = time.perf_counter()
        connecter(dsn_crdb[v - 1]).close()
        print(f"   nœud {v} redémarré, prêt après {time.perf_counter() - t:.0f} s")
        par_s, (med, p99), err, pire = asyncio.run(debit(dsn_crdb, duree=10))
        print(f"   10 s d'écritures sur les trois nœuds : {nombre(par_s)} /s")
        print(f"   médiane {med:.1f} ms, p99 {p99:.1f} ms, maximum {pire:.0f} ms,"
              f" {err} erreur{'s' if err > 1 else ''}")
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
