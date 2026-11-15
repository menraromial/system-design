"""Client du chapitre 46, exécuté dans l'espace réseau d'une région
(docker exec cours-sd-ch46-<région> python3 /code/client.py ...). Il écrit son
résultat en JSON sur la sortie standard.

    latence <hôte> <port>                      SELECT 1 sur une connexion gardée,
                                               puis nouvelles connexions
    ecrire <hôte> <port> <n> <fils> <commit> [<durée s>]
                                               INSERT dans journal ; commit = local
                                               ou on ; avec une durée, écrit
                                               jusqu'à la fin ou jusqu'à l'erreur
    lire <hôte> <port> <n>                     lectures sur une connexion gardée
    observer <hôte> <port> <n>                 attend n nouvelles lignes du journal
                                               et mesure leur retard
    maj <hôte> <port> <durée> <débit> <mode> <parité> <lignes>
                                               UPDATE profils, mode tous ou attache
"""
import json
import random
import statistics
import sys
import threading
import time

import psycopg


def dsn(hote, port):
    return f"host={hote} port={port} user=postgres dbname=postgres connect_timeout=5"


def stats(v):
    v = sorted(v)
    return {"med": statistics.median(v), "p99": v[min(len(v) - 1, int(.99 * len(v)))], "n": len(v)}


def latence(hote, port):
    c = psycopg.connect(dsn(hote, port), autocommit=True)
    d = []
    for _ in range(100):
        t = time.perf_counter()
        c.execute("select 1").fetchone()
        d.append((time.perf_counter() - t) * 1000)
    c.close()
    n = []
    for _ in range(10):
        t = time.perf_counter()
        c = psycopg.connect(dsn(hote, port), autocommit=True)
        c.execute("select 1").fetchone()
        n.append((time.perf_counter() - t) * 1000)
        c.close()
    return {"requete": stats(d), "connexion": stats(n)}


def ecrire(hote, port, n, fils, commit, duree=None):
    lat, acquis, verrou = [], [], threading.Lock()
    fin = time.monotonic() + duree if duree else None
    erreurs = []

    def travail(k):
        try:
            c = psycopg.connect(dsn(hote, port), autocommit=True)
            c.execute(f"set synchronous_commit = {commit}")
            i = 0
            while (fin is None and i < n) or (fin is not None and time.monotonic() < fin):
                t = time.perf_counter()
                ident = c.execute("insert into journal (ts, charge) values (%s, %s) returning id",
                                  (time.time(), "x" * 200)).fetchone()[0]
                d = (time.perf_counter() - t) * 1000
                with verrou:
                    lat.append(d)
                    acquis.append(ident)
                i += 1
        except psycopg.Error as e:
            erreurs.append(type(e).__name__)

    t0 = time.monotonic()
    th = [threading.Thread(target=travail, args=(k,)) for k in range(fils)]
    for t in th:
        t.start()
    for t in th:
        t.join()
    duree_reelle = time.monotonic() - t0
    r = {"lat": stats(lat) if lat else None, "acquis": len(acquis), "debit": len(acquis) / duree_reelle,
         "erreurs": len(erreurs)}
    if duree:
        r["ids"] = acquis
    return r


def lire(hote, port, n):
    c = psycopg.connect(dsn(hote, port), autocommit=True)
    d = []
    for _ in range(n):
        t = time.perf_counter()
        c.execute("select count(*) from journal where id > (select max(id) - 10 from journal)").fetchone()
        d.append((time.perf_counter() - t) * 1000)
    return {"lat": stats(d)}


def observer(hote, port, n):
    c = psycopg.connect(dsn(hote, port), autocommit=True)
    dernier = c.execute("select coalesce(max(id), 0) from journal").fetchone()[0]
    print(json.dumps({"pret": True}), flush=True)
    retards = []
    while len(retards) < n:
        lignes = c.execute("select id, ts from journal where id > %s order by id", (dernier,)).fetchall()
        maintenant = time.time()
        for ident, ts in lignes:
            retards.append((maintenant - ts) * 1000)
            dernier = ident
        time.sleep(0.001)
    return {"retard": stats(retards)}


def maj(hote, port, duree, debit, mode, parite, lignes):
    c = psycopg.connect(dsn(hote, port), autocommit=True)
    alea = random.Random()
    fin = time.monotonic() + duree
    k = 0
    prochain = time.monotonic()
    while time.monotonic() < fin:
        if mode == "tous":
            ident = alea.randrange(lignes)
        else:
            ident = 2 * alea.randrange(lignes // 2) + parite
        c.execute("update profils set valeur = %s, region = %s where id = %s",
                  (alea.randrange(10 ** 9), str(parite), ident))
        k += 1
        prochain += 1 / debit
        time.sleep(max(0.0, prochain - time.monotonic()))
    return {"maj": k}


def main():
    cmd, hote, port = sys.argv[1], sys.argv[2], int(sys.argv[3])
    a = sys.argv[4:]
    if cmd == "latence":
        r = latence(hote, port)
    elif cmd == "ecrire":
        r = ecrire(hote, port, int(a[0]), int(a[1]), a[2], float(a[3]) if len(a) > 3 else None)
    elif cmd == "lire":
        r = lire(hote, port, int(a[0]))
    elif cmd == "observer":
        r = observer(hote, port, int(a[0]))
    else:
        r = maj(hote, port, float(a[0]), float(a[1]), a[2], int(a[3]), int(a[4]))
    print(json.dumps(r), flush=True)


if __name__ == "__main__":
    main()
