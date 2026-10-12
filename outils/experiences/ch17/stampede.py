"""Chapitre 17 : une clé populaire expire, et tous ses lecteurs recalculent en même temps.

La page d'accueil est le résultat d'une requête qui coûte 100 ms à la base
(simulée par pg_sleep(0.1) dans PostgreSQL). Elle est gardée dans Redis avec un
TTL de 2 s. 100 clients la lisent en boucle pendant 20 s, avec 20 ms de pause
entre deux lectures ; l'application dispose de 40 connexions à la base.
Quatre stratégies :
  - naïve : en cas d'échec, chaque lecteur recalcule et réécrit la clé ;
  - verrou : un seul lecteur prend un verrou (SET NX) et recalcule, les autres
    attendent et relisent le cache ;
  - XFetch : chaque lecteur recalcule par avance avec une probabilité qui croît
    à l'approche de l'expiration (Vattani et al., 2015, beta = 1) ;
  - stale-while-revalidate : la valeur périmée reste servie pendant qu'un seul
    lecteur la recalcule en arrière-plan.
La clé est en cache au départ. Mesures : nombre de recalculs, nombre maximal
de recalculs simultanés, latence des lectures (médiane, 99e centile, maximum).
Prérequis : docker. Usage : python3 stampede.py   (pip install -r requirements.txt)
"""
import asyncio
import json
import math
import random
import statistics
import time

import psycopg
import redis.asyncio as aioredis

import commun

DUREE, CLIENTS, PAUSE, TTL, CONNEXIONS = 20.0, 100, 0.020, 2.0, 40
CLE = "page:accueil"


class Base:
    def __init__(self):
        self.recalculs = self.en_cours = self.max_en_cours = 0

    async def ouvrir(self):
        self.libres = asyncio.Queue()
        for _ in range(CONNEXIONS):
            self.libres.put_nowait(await psycopg.AsyncConnection.connect(commun.DSN, autocommit=True))

    async def recalculer(self):
        cx = await self.libres.get()
        self.recalculs += 1
        self.en_cours += 1
        self.max_en_cours = max(self.max_en_cours, self.en_cours)
        try:
            await cx.execute("SELECT pg_sleep(0.1)")
            return f"page calculée à {time.monotonic():.3f}"
        finally:
            self.en_cours -= 1
            self.libres.put_nowait(cx)

    async def fermer(self):
        while not self.libres.empty():
            await (self.libres.get_nowait()).close()


async def lire_naif(r, base):
    v = await r.get(CLE)
    if v is None:
        v = await base.recalculer()
        await r.set(CLE, v, px=int(TTL * 1000))
    return v


async def lire_verrou(r, base):
    while True:
        v = await r.get(CLE)
        if v is not None:
            return v
        if await r.set(CLE + ":verrou", "1", nx=True, px=1000):
            v = await base.recalculer()
            await r.set(CLE, v, px=int(TTL * 1000))
            await r.delete(CLE + ":verrou")
            return v
        await asyncio.sleep(0.010)                 # un autre recalcule : relire bientôt


async def lire_xfetch(r, base):
    brut = await r.get(CLE)
    if brut is not None:
        e = json.loads(brut)
        # recalcul anticipé : Time() - delta * beta * log(rand()) >= expiry (beta = 1)
        if time.time() - e["delta"] * math.log(random.random()) < e["expiry"]:
            return e["valeur"]
    debut = time.monotonic()
    v = await base.recalculer()
    delta = time.monotonic() - debut
    await r.set(CLE, json.dumps({"valeur": v, "delta": delta, "expiry": time.time() + TTL}),
                px=int(TTL * 1000))
    return v


async def lire_swr(r, base):
    brut = await r.get(CLE)
    if brut is not None:
        e = json.loads(brut)
        if time.time() < e["frais_jusqu_a"]:
            return e["valeur"]
        if await r.set(CLE + ":verrou", "1", nx=True, px=1000):   # un seul rafraîchit
            asyncio.ensure_future(rafraichir(r, base))
        return e["valeur"]                                          # en attendant : la périmée
    return await rafraichir(r, base)


async def rafraichir(r, base):
    v = await base.recalculer()
    # la clé vit plus longtemps que sa fraîcheur, pour pouvoir être servie périmée
    await r.set(CLE, json.dumps({"valeur": v, "frais_jusqu_a": time.time() + TTL}), px=int(TTL * 10_000))
    await r.delete(CLE + ":verrou")
    return v


async def essai(lire):
    r = aioredis.Redis(port=commun.REDIS, max_connections=CLIENTS)
    await r.flushall()
    base = Base()
    await base.ouvrir()
    await lire(r, base)                            # clé déjà en cache au départ : on mesure
    base.recalculs = base.max_en_cours = 0         # le régime établi, pas le démarrage à froid
    latences, fin = [], time.monotonic() + DUREE

    async def client():
        await asyncio.sleep(random.random() * PAUSE)
        while time.monotonic() < fin:
            t = time.perf_counter()
            await lire(r, base)
            latences.append((time.perf_counter() - t) * 1000)
            await asyncio.sleep(PAUSE)
    await asyncio.gather(*(client() for _ in range(CLIENTS)))
    await base.fermer()
    await r.aclose()
    latences.sort()
    return base, latences


async def main():
    commun.lancer()
    try:
        print(f"{CLIENTS} clients, {DUREE:.0f} s, TTL {TTL:.0f} s, recalcul de 100 ms, {CONNEXIONS} connexions")
        print(f"{'stratégie':<24}{'lectures':>9}{'recalculs':>10}{'simultanés':>11}"
              f"{'médiane':>10}{'99e cent.':>11}{'max':>9}")
        for nom, lire in (("naïve", lire_naif), ("verrou de recalcul", lire_verrou),
                          ("XFetch (beta = 1)", lire_xfetch), ("stale-while-revalidate", lire_swr)):
            base, lat = await essai(lire)
            p99 = lat[int(0.99 * len(lat))]
            print(f"{nom:<24}{len(lat):>9}{base.recalculs:>10}{base.max_en_cours:>11}"
                  f"{statistics.median(lat):>7.2f} ms{p99:>8.1f} ms{lat[-1]:>6.0f} ms")
    finally:
        commun.arreter()


if __name__ == "__main__":
    asyncio.run(main())
