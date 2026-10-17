"""Chapitre 30 : comportement des clients face au code 429, et choix de la clé.

Lance serveur.py dans un sous-processus (port 11030), limité à 10 requêtes
par seconde avec des rafales de 20, puis :
1. trois clients qui veulent chacun 150 réponses, avec 4 requêtes en vol :
   réessai immédiat, attente de Retry-After, recul exponentiel avec gigue ;
2. 19 utilisateurs à 1 requête par seconde et un abusif à 30 par seconde,
   tous derrière la même adresse IP, avec une limite par IP puis par clé
   d'API.

    python3 http429.py
"""
import asyncio
import random
import subprocess
import sys
import time
from pathlib import Path

import aiohttp

PORT = 11030
URL = f"http://127.0.0.1:{PORT}"
ICI = Path(__file__).parent


async def attendre_serveur():
    async with aiohttp.ClientSession() as s:
        for _ in range(100):
            try:
                async with s.get(URL + "/stats") as r:
                    return await r.json()
            except aiohttp.ClientError:
                await asyncio.sleep(0.05)
    raise RuntimeError("le serveur ne répond pas")


async def stats():
    async with aiohttp.ClientSession() as s:
        async with s.get(URL + "/stats") as r:
            return await r.json()


class Serveur:
    def __init__(self, cle):
        self.cle = cle

    async def __aenter__(self):
        self.p = subprocess.Popen(
            [sys.executable, str(ICI / "serveur.py"), "--port", str(PORT),
             "--cle", self.cle, "--debit", "10", "--capacite", "20"])
        self.debut = await attendre_serveur()
        return self

    async def __aexit__(self, *exc):
        self.p.terminate()
        self.p.wait()


async def client(strategie, voulu=150, en_vol=4):
    envoyees, refusees, recues = 0, 0, 0
    rng = random.Random(1)

    async def tache(s):
        nonlocal envoyees, refusees, recues
        echecs = 0
        while recues < voulu:
            envoyees += 1
            async with s.get(URL + "/api") as r:
                await r.read()
                if r.status == 200:
                    recues += 1
                    echecs = 0
                    continue
                refusees += 1
                echecs += 1
                if strategie == "retry-after":
                    await asyncio.sleep(float(r.headers["Retry-After"]))
                elif strategie == "recul":
                    plafond = min(5.0, 0.05 * 2 ** echecs)
                    await asyncio.sleep(rng.uniform(0, plafond))

    debut = time.monotonic()
    async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=en_vol)) as s:
        await asyncio.gather(*(tache(s) for _ in range(en_vol)))
    return time.monotonic() - debut, envoyees, refusees


async def strategies():
    print("1. un client veut 150 réponses ; limite 10/s, rafale 20")
    print(f"   {'stratégie':22} {'durée':>7} {'envoyées':>9} {'429':>7} "
          f"{'CPU serveur':>12}")
    for strategie, nom in [("immediat", "réessai immédiat"),
                           ("retry-after", "attendre Retry-After"),
                           ("recul", "recul exp. + gigue")]:
        async with Serveur("ip") as srv:
            duree, env, ref = await client(strategie)
            fin = await stats()
            cpu = fin["cpu"] - srv.debut["cpu"]
        print(f"   {nom:22} {duree:6.1f}s {env:9} {ref:7} {cpu:10.2f} s")


async def utilisateur(s, cle, debit, duree, res):
    debut = time.monotonic()
    k = 0
    while (t := time.monotonic() - debut) < duree:
        attente = k / debit - t
        if attente > 0:
            await asyncio.sleep(attente)
        k += 1
        async with s.get(URL + "/api", headers={"X-Cle-API": cle}) as r:
            await r.read()
            res[cle][r.status == 200] += 1


async def cles():
    print("\n2. 19 utilisateurs à 1/s et un abusif à 30/s, même IP, 20 s")
    for mode, nom in [("ip", "clé = adresse IP"),
                      ("entete", "clé = clé d'API")]:
        async with Serveur(mode):
            res = {f"u{i}": [0, 0] for i in range(19)} | {"abusif": [0, 0]}
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=100)) as s:
                await asyncio.gather(
                    *(utilisateur(s, f"u{i}", 1, 20, res) for i in range(19)),
                    utilisateur(s, "abusif", 30, 20, res))
        ok = sum(v[1] for c, v in res.items() if c != "abusif")
        tot = sum(sum(v) for c, v in res.items() if c != "abusif")
        ab = res["abusif"]
        print(f"   {nom:18} utilisateurs : {100 * ok / tot:5.1f} % servis ; "
              f"abusif : {ab[1]} sur {sum(ab)}")


async def main():
    await strategies()
    await cles()


if __name__ == "__main__":
    asyncio.run(main())
