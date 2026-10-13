"""Chapitre 19 : ouvrir une connexion à chaque requête, ou les partager (pool).

  1. 500 fois SELECT 1, en ouvrant puis fermant une connexion à chaque fois, puis
     500 fois sur une connexion déjà ouverte : coût de l'ouverture (TCP,
     authentification SCRAM, création du processus serveur) ;
  2. 300 clients simultanés font chacun 10 requêtes qui occupent un cœur environ
     5 ms (compter 50 000 lignes de generate_series), contre un serveur limité à
     max_connections = 100, sur une machine de 22 fils d'exécution :
       - chaque client ouvre une connexion à chaque requête ;
       - chaque client garde sa propre connexion ;
       - tous passent par un pool de 20, puis de 90 connexions ouvertes une fois
         pour toutes (une file d'attente équitable : on prend une connexion, on la
         rend après la requête, et le premier arrivé est le premier servi).
     Mesures : requêtes réussies, refusées (« too many clients »), durée totale,
     latence médiane et 99e centile vue par le client (attente du pool comprise),
     et durée médiane de la requête elle-même.
Prérequis : docker, image postgres:18-alpine. Usage : python3 pool.py
"""
import asyncio
import statistics
import time

import psycopg

import commun

CLIENTS, REQUETES = 300, 10
REQUETE = "SELECT count(*) FROM generate_series(1, 50000)"


async def ouvrir():
    return await psycopg.AsyncConnection.connect(commun.DSN, autocommit=True)


async def executer(cx, stats):
    t = time.perf_counter()
    await cx.execute(REQUETE)
    stats["execution"].append(time.perf_counter() - t)


async def par_requete(stats, _):
    for _ in range(REQUETES):
        t = time.perf_counter()
        try:
            cx = await ouvrir()
        except psycopg.OperationalError:
            stats["refus"] += 1
            continue
        async with cx:
            await executer(cx, stats)
        stats["latences"].append(time.perf_counter() - t)


async def par_client(stats, _):
    try:
        cx = await ouvrir()
    except psycopg.OperationalError:
        stats["refus"] += REQUETES                 # le client n'obtient jamais de connexion
        return
    async with cx:
        for _ in range(REQUETES):
            t = time.perf_counter()
            await executer(cx, stats)
            stats["latences"].append(time.perf_counter() - t)


async def par_pool(stats, pool):
    for _ in range(REQUETES):
        t = time.perf_counter()
        cx = await pool.get()                      # attend qu'une connexion se libère
        try:
            await executer(cx, stats)
        finally:
            pool.put_nowait(cx)
        stats["latences"].append(time.perf_counter() - t)
        await asyncio.sleep(0)                     # laisse passer celui qui attendait la connexion


async def essai(client, taille=0):
    pool = asyncio.Queue()
    for cx in await asyncio.gather(*(ouvrir() for _ in range(taille))):
        pool.put_nowait(cx)
    stats = {"refus": 0, "latences": [], "execution": []}
    t = time.perf_counter()
    await asyncio.gather(*(client(stats, pool) for _ in range(CLIENTS)))
    duree = time.perf_counter() - t
    while not pool.empty():
        await pool.get_nowait().close()
    lat = sorted(stats["latences"])
    p99 = lat[int(0.99 * len(lat))] if lat else float("nan")
    return (len(lat), stats["refus"], duree, statistics.median(lat), p99,
            statistics.median(stats["execution"]))


async def main():
    commun.lancer()
    try:
        print("1. 500 requêtes SELECT 1, l'une après l'autre")
        t = time.perf_counter()
        for _ in range(500):
            async with await ouvrir() as cx:
                await cx.execute("SELECT 1")
        avec = (time.perf_counter() - t) / 500
        async with await ouvrir() as cx:
            t = time.perf_counter()
            for _ in range(500):
                await cx.execute("SELECT 1")
            sans = (time.perf_counter() - t) / 500
        print(f"   connexion ouverte à chaque requête  {1000 * avec:6.2f} ms par requête")
        print(f"   connexion déjà ouverte              {1000 * sans:6.2f} ms par requête")

        print(f"\n2. {CLIENTS} clients × {REQUETES} requêtes d'environ 5 ms, max_connections = 100")
        print(f"   {'':<27}{'réussies':>9}{'refusées':>9}{'durée':>8}{'médiane':>10}{'p99':>10}{'requête':>10}")
        for nom, f, taille in (("une connexion par requête", par_requete, 0),
                               ("une connexion par client", par_client, 0),
                               ("pool de 20 connexions", par_pool, 20),
                               ("pool de 90 connexions", par_pool, 90)):
            ok, refus, duree, med, p99, req = await essai(f, taille)
            print(f"   {nom:<27}{ok:>9}{refus:>9}{duree:>6.2f} s{1000 * med:>7.1f} ms{1000 * p99:>7.1f} ms"
                  f"{1000 * req:>7.1f} ms")
            await asyncio.sleep(1)                 # laisse les processus serveur se terminer
    finally:
        commun.arreter()


if __name__ == "__main__":
    asyncio.run(main())
