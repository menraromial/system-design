"""Chapitre 18 : deux clients réservent le même siège.

20 sièges libres ; 400 clients veulent chacun un siège tiré au hasard parmi les
20, et passent par 40 connexions à PostgreSQL, en même temps. Entre la lecture du
siège et l'écriture de la réservation, l'application met 1 ms à préparer la
réservation (calcul du prix, vérification du client). Cinq stratégies :
  - naïve : SELECT, puis UPDATE si le siège était libre (READ COMMITTED) ;
  - FOR UPDATE : SELECT ... FOR UPDATE dans une transaction (verrou pessimiste) ;
  - UPDATE conditionnel : UPDATE ... WHERE client IS NULL, et on regarde le
    nombre de lignes modifiées ;
  - optimiste : on lit la version du siège, et l'UPDATE n'aboutit que si la
    version n'a pas changé (WHERE version = v) ;
  - SERIALIZABLE : la stratégie naïve dans une transaction SERIALIZABLE, rejouée
    jusqu'à 5 fois en cas d'erreur 40001.
Mesures : réservations confirmées aux clients, sièges confirmés à plusieurs
clients (vendus plusieurs fois), conflits détectés, durée.
Prérequis : docker, image postgres:18-alpine. Usage : python3 sieges.py
"""
import asyncio
import collections
import random
import time

import psycopg
from psycopg import errors

import commun

SIEGES, CLIENTS, CONNEXIONS, PREPARATION = 20, 400, 40, 0.001


async def naive(cx, siege, client, stats):
    r = await (await cx.execute("SELECT client FROM sieges WHERE id = %s", (siege,))).fetchone()
    if r[0] is not None:
        return False
    await asyncio.sleep(PREPARATION)
    await cx.execute("UPDATE sieges SET client = %s WHERE id = %s", (client, siege))
    return True


async def for_update(cx, siege, client, stats):
    async with cx.transaction():
        r = await (await cx.execute("SELECT client FROM sieges WHERE id = %s FOR UPDATE",
                                    (siege,))).fetchone()
        if r[0] is not None:
            return False
        await asyncio.sleep(PREPARATION)
        await cx.execute("UPDATE sieges SET client = %s WHERE id = %s", (client, siege))
        return True


async def conditionnel(cx, siege, client, stats):
    await asyncio.sleep(PREPARATION)
    cur = await cx.execute("UPDATE sieges SET client = %s WHERE id = %s AND client IS NULL",
                           (client, siege))
    return cur.rowcount == 1


async def optimiste(cx, siege, client, stats):
    r = await (await cx.execute("SELECT client, version FROM sieges WHERE id = %s", (siege,))).fetchone()
    if r[0] is not None:
        return False
    await asyncio.sleep(PREPARATION)
    cur = await cx.execute("""UPDATE sieges SET client = %s, version = version + 1
                              WHERE id = %s AND version = %s""", (client, siege, r[1]))
    if cur.rowcount == 0:
        stats["conflits"] += 1                     # quelqu'un est passé entre la lecture et l'écriture
    return cur.rowcount == 1


async def serialisable(cx, siege, client, stats):
    for _ in range(5):
        try:
            await cx.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            r = await (await cx.execute("SELECT client FROM sieges WHERE id = %s", (siege,))).fetchone()
            if r[0] is not None:
                await cx.execute("COMMIT")
                return False
            await asyncio.sleep(PREPARATION)
            await cx.execute("UPDATE sieges SET client = %s WHERE id = %s", (client, siege))
            await cx.execute("COMMIT")
            return True
        except errors.SerializationFailure:
            await cx.execute("ROLLBACK")
            stats["conflits"] += 1
    return False


async def essai(strategie):
    async with await psycopg.AsyncConnection.connect(commun.DSN, autocommit=True) as cx:
        await cx.execute("DROP TABLE IF EXISTS sieges")
        await cx.execute("CREATE TABLE sieges (id int PRIMARY KEY, client int, version int DEFAULT 0)")
        await cx.execute("INSERT INTO sieges (id) SELECT generate_series(1, %s)", (SIEGES,))
    alea = random.Random(18)
    demandes = asyncio.Queue()
    for client in range(1, CLIENTS + 1):
        demandes.put_nowait((alea.randint(1, SIEGES), client))
    confirmes = collections.defaultdict(list)       # siège -> clients à qui on a dit « réservé »
    stats = collections.Counter()

    async def guichet():
        async with await psycopg.AsyncConnection.connect(commun.DSN, autocommit=True) as cx:
            while not demandes.empty():
                siege, client = demandes.get_nowait()
                if await strategie(cx, siege, client, stats):
                    confirmes[siege].append(client)

    t = time.perf_counter()
    await asyncio.gather(*(guichet() for _ in range(CONNEXIONS)))
    duree = time.perf_counter() - t
    reservations = sum(len(c) for c in confirmes.values())
    doubles = sum(1 for c in confirmes.values() if len(c) > 1)
    return reservations, doubles, stats["conflits"], duree


async def main():
    commun.lancer()
    try:
        print(f"{SIEGES} sièges, {CLIENTS} clients, {CONNEXIONS} connexions, 1 ms entre lecture et écriture")
        print(f"{'stratégie':<22}{'confirmées':>11}{'vendus plusieurs fois':>24}{'conflits':>10}{'durée':>9}")
        for nom, f in (("naïve", naive), ("SELECT ... FOR UPDATE", for_update),
                       ("UPDATE conditionnel", conditionnel), ("optimiste (version)", optimiste),
                       ("SERIALIZABLE + reprise", serialisable)):
            reservations, doubles, conflits, duree = await essai(f)
            print(f"{nom:<22}{reservations:>11}{doubles:>24}{conflits:>10}{1000 * duree:>6.0f} ms")
    finally:
        commun.arreter()


if __name__ == "__main__":
    asyncio.run(main())
