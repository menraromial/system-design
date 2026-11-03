"""Chapitre 38 : requêtes doublées (hedged requests) contre la traîne de latence.

Dix répliques (replica.py, ports 11840 à 11849) répondent en 1 à 3 ms, sauf
pendant des pauses de 50 à 150 ms, en moyenne toutes les 5 s. Le client envoie
400 lectures par seconde (arrivées de Poisson) pendant 30 s, chacune à une
réplique tirée au hasard.

1. lecture simple ; puis le délai d'attente qu'il faudrait pour un taux donné
   de faux délais dépassés ;
2. requête doublée : si la réponse n'est pas venue après le 95e centile de la
   lecture simple, on envoie la même lecture à une autre réplique et l'on garde
   la première réponse (l'autre est annulée) ; puis doublée d'emblée ;
3. éventail (fan-out) : 40 requêtes par seconde, qui lisent chacune les dix
   répliques et attendent les dix réponses, sans et avec doublement.

    python3 doublees.py
"""
import asyncio
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

import aiohttp

ICI = Path(__file__).parent
PORTS = list(range(11840, 11850))
DUREE = 30


def lancer():
    procs = [subprocess.Popen([sys.executable, str(ICI / "replica.py"), "--port", str(p)]) for p in PORTS]
    time.sleep(1.5)
    return procs


def centile(d, q):
    return d[min(len(d) - 1, int(q * len(d)))]


def afficher(nom, durees, envois, demandes):
    d = sorted(durees)
    print(f"   {nom:<26}{statistics.median(d):>7.1f}{centile(d, .95):>7.1f}{centile(d, .99):>7.1f}"
          f"{centile(d, .999):>8.1f}{d[-1]:>7.0f}{100 * (envois / demandes - 1):>8.1f} %")


async def campagne(session, debit, largeur, doubler_apres, graine):
    """largeur : répliques lues par requête ; doubler_apres : en ms, ou None."""
    alea = random.Random(graine)
    durees, envois = [], 0

    async def une(port):
        async with session.get(f"http://127.0.0.1:{port}/lire") as r:
            await r.read()

    async def lecture(port):
        nonlocal envois
        envois += 1
        premiere = asyncio.create_task(une(port))
        if doubler_apres is None:
            await premiere
            return
        fini, _ = await asyncio.wait({premiere}, timeout=doubler_apres / 1000)
        if fini:
            return
        envois += 1
        autre = alea.choice([p for p in PORTS if p != port])
        seconde = asyncio.create_task(une(autre))
        fini, reste = await asyncio.wait({premiere, seconde}, return_when=asyncio.FIRST_COMPLETED)
        for t in reste:
            t.cancel()

    async def demande():
        t = time.perf_counter()
        cibles = alea.sample(PORTS, largeur)
        await asyncio.gather(*(lecture(p) for p in cibles))
        durees.append((time.perf_counter() - t) * 1000)

    taches = []
    t0 = prochain = time.monotonic()
    while time.monotonic() - t0 < DUREE:
        prochain += alea.expovariate(debit)
        attente = prochain - time.monotonic()
        if attente > 0:
            await asyncio.sleep(attente)
        taches.append(asyncio.create_task(demande()))
    await asyncio.gather(*taches)
    return durees, envois, len(taches)


async def main():
    procs = lancer()
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
            await campagne(s, 200, 1, None, 0)            # mise en route
            print(f"10 répliques, pauses de 50 à 150 ms toutes les 5 s en moyenne ; {DUREE} s par essai")
            print(f"\n   {'(ms)':<26}{'méd.':>7}{'95e':>7}{'99e':>7}{'99,9e':>8}{'max':>7}{'envois':>10}")
            print("1. lecture simple, 400/s")
            d, e, n = await campagne(s, 400, 1, None, 1)
            afficher("simple", d, e, n)
            p95 = centile(sorted(d), .95)
            print("\n   délai d'attente et part de faux délais dépassés")
            for delai in (5, 10, 20, 50, 100, 150):
                part = sum(x > delai for x in d) / len(d)
                print(f"     {delai:>4} ms : {100 * part:6.2f} %")

            print(f"\n2. lecture doublée, 400/s (95e centile de la lecture simple : {p95:.1f} ms)")
            d, e, n = await campagne(s, 400, 1, p95, 2)
            afficher(f"doublée après {p95:.1f} ms", d, e, n)
            d, e, n = await campagne(s, 400, 1, 0, 3)
            afficher("doublée d'emblée", d, e, n)

            print("\n3. éventail sur les 10 répliques, 40 requêtes/s")
            d, e, n = await campagne(s, 40, 10, None, 4)
            afficher("éventail simple", d, e, 10 * n)
            d, e, n = await campagne(s, 40, 10, p95, 5)
            afficher(f"éventail doublé ({p95:.1f} ms)", d, e, 10 * n)
    finally:
        for p in procs:
            p.kill()
            p.wait()


if __name__ == "__main__":
    asyncio.run(main())
