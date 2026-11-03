"""Chapitre 38 : délais d'attente et échéances dans une chaîne de trois services.

Le client appelle A (port 11850), qui appelle B (11851), qui appelle C (11852).
Les trois services tournent dans un sous-processus (option --services).

1. C est en panne et répond 503 aussitôt. On compte les tentatives que reçoit
   C pour une requête du client, quand chaque niveau fait jusqu'à 3 tentatives,
   puis quand seul le client en fait.

2. C répond en 50 ms, sauf une fois sur dix où il lui faut 1,5 s. Le client
   attend 1 s par tentative et fait jusqu'à 3 tentatives ; 20 requêtes par
   seconde pendant 30 s. Trois réglages :
     fixes       A et B attendent 2 s leur service suivant ;
     décroissants  A attend 0,9 s et B 0,8 s ;
     échéance    le client envoie le temps qui lui reste (X-Echeance-Ms) ;
                 A et B retranchent le temps écoulé, l'emploient comme délai
                 et le transmettent ; C abandonne son travail à l'échéance.
   On mesure le temps de travail de C consacré à des requêtes dont le client
   était déjà parti. Pour le savoir, le client ajoute l'instant où il cessera
   d'attendre (X-Fin-Client, horloge monotone de la machine), que A et B
   transmettent sans s'en servir : c'est un instrument de mesure, pas une
   partie du protocole.

    python3 echeances.py
"""
import argparse
import asyncio
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp import web

ICI = Path(__file__)
PA, PB, PC = 11850, 11851, 11852


# ---- les trois services ------------------------------------------------------
def services(args):
    alea = random.Random(38)
    etat = {"panne": False, "reglage": "fixes", "tentatives": 3}
    c_stats = {"recues": 0, "utile": 0.0, "perdu": 0.0, "abandon": 0.0}
    session = {}

    def delai_pour(requete, propre):
        """Délai de l'appel suivant, et en-tête à transmettre."""
        mesure = {"X-Fin-Client": requete.headers["X-Fin-Client"]}
        if etat["reglage"] != "echeance":
            return propre, mesure
        reste = float(requete.headers["X-Echeance-Ms"]) / 1000 - (time.monotonic() - requete["arrivee"])
        return reste, {**mesure, "X-Echeance-Ms": str(int(reste * 1000))}

    def intermediaire(suivant, propre):
        async def gerer(requete):
            requete["arrivee"] = time.monotonic()
            for n in range(etat["tentatives"]):
                delai, entetes = delai_pour(requete, propre)
                if delai <= 0:
                    return web.Response(status=504, text="échéance passée")
                try:
                    async with session["s"].get(f"http://127.0.0.1:{suivant}/", headers=entetes,
                                                timeout=aiohttp.ClientTimeout(total=delai)) as r:
                        corps = await r.read()
                        if r.status == 200:
                            return web.Response(body=corps)
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    pass
            return web.Response(status=503, text="échec")
        return gerer

    async def service_c(requete):
        arrivee = time.monotonic()
        c_stats["recues"] += 1
        if etat["panne"]:
            return web.Response(status=503, text="en panne")
        travail = 1.5 if alea.random() < 0.1 else 0.05
        if etat["reglage"] == "echeance":
            reste = float(requete.headers["X-Echeance-Ms"]) / 1000 - (time.monotonic() - arrivee)
            if travail > reste:
                await asyncio.sleep(max(0.0, reste))       # travail interrompu à l'échéance
                c_stats["abandon"] += max(0.0, reste)
                return web.Response(status=504, text="échéance passée")
        await asyncio.sleep(travail)
        if time.monotonic() > float(requete.headers["X-Fin-Client"]):
            c_stats["perdu"] += travail
        else:
            c_stats["utile"] += travail
        return web.Response(text="ok")

    async def regler(requete):
        q = requete.query
        etat["panne"] = q.get("panne") == "1"
        etat["reglage"] = q.get("reglage", "fixes")
        etat["tentatives"] = int(q.get("tentatives", "3"))
        for k in c_stats:
            c_stats[k] = 0 if k == "recues" else 0.0
        return web.json_response(etat)

    async def lire(requete):
        return web.json_response(c_stats)

    propres = {"fixes": (2.0, 2.0), "decroissants": (0.9, 0.8), "echeance": (None, None)}

    def gerer_a(requete):
        return intermediaire(PB, propres.get(etat["reglage"], (2.0, 2.0))[0])(requete)

    def gerer_b(requete):
        return intermediaire(PC, propres.get(etat["reglage"], (2.0, 2.0))[1])(requete)

    async def principal():
        session["s"] = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))
        coureurs = []
        for port, routes in ((PA, [web.get("/", gerer_a), web.get("/regler", regler)]),
                             (PB, [web.get("/", gerer_b)]),
                             (PC, [web.get("/", service_c), web.get("/stats", lire)])):
            app = web.Application()
            app.add_routes(routes)
            r = web.AppRunner(app, access_log=None)
            await r.setup()
            await web.TCPSite(r, "127.0.0.1", port).start()
            coureurs.append(r)
        await asyncio.Event().wait()

    asyncio.run(principal())


# ---- le client ---------------------------------------------------------------
async def regler(s, **q):
    async with s.get(f"http://127.0.0.1:{PA}/regler", params={k: str(v) for k, v in q.items()}) as r:
        await r.read()


async def stats_c(s):
    async with s.get(f"http://127.0.0.1:{PC}/stats") as r:
        return await r.json()


async def appel(s, tentatives, delai=1.0):
    for _ in range(tentatives):
        try:
            entetes = {"X-Echeance-Ms": str(int(delai * 1000)), "X-Fin-Client": str(time.monotonic() + delai)}
            async with s.get(f"http://127.0.0.1:{PA}/", headers=entetes,
                             timeout=aiohttp.ClientTimeout(total=delai)) as r:
                await r.read()
                if r.status == 200:
                    return True
        except (asyncio.TimeoutError, aiohttp.ClientError):
            pass
    return False


async def client():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
        for _ in range(100):
            try:
                await regler(s)
                break
            except aiohttp.ClientError:
                await asyncio.sleep(0.05)
        print("1. C en panne : tentatives reçues par C pour 100 requêtes du client")
        for nom, niveau in (("3 tentatives à chaque niveau", 3), ("3 tentatives au seul client", 1)):
            await regler(s, panne=1, tentatives=niveau)
            for _ in range(100):
                await appel(s, 3)
            c = await stats_c(s)
            print(f"   {nom:<30} {c['recues']:>5}, soit {c['recues'] / 100:.0f} par requête")

        print("\n2. C lent une fois sur dix (1,5 s) ; client : 1 s par tentative, 3 tentatives,")
        print("   20 requêtes/s pendant 30 s")
        print(f"   {'réglage':<14}{'réussite':>9}{'reçues':>8}{'C utile':>9}{'C perdu':>9}"
              f"{'abandonné':>10}{'durée 99e':>11}")
        alea = random.Random(5)
        for reglage, tent in (("fixes", 3), ("decroissants", 3), ("echeance", 1)):
            await regler(s, panne=0, reglage=reglage, tentatives=tent)
            durees, ok = [], 0

            async def une():
                nonlocal ok
                t = time.perf_counter()
                reussi = await appel(s, 3)
                ok += reussi
                durees.append(time.perf_counter() - t)

            taches = []
            t0 = prochain = time.monotonic()
            while time.monotonic() - t0 < 30:
                prochain += alea.expovariate(20)
                if (a := prochain - time.monotonic()) > 0:
                    await asyncio.sleep(a)
                taches.append(asyncio.create_task(une()))
            await asyncio.gather(*taches)
            await asyncio.sleep(4.5)                   # laisser finir le travail en cours
            c = await stats_c(s)
            durees.sort()
            nom = {"fixes": "fixes (2 s)", "decroissants": "0,9 / 0,8 s", "echeance": "échéance"}[reglage]
            print(f"   {nom:<14}{100 * ok / len(taches):>8.1f}%{c['recues']:>8}{c['utile']:>8.0f}s"
                  f"{c['perdu']:>8.0f}s{c['abandon']:>9.0f}s{durees[int(.99 * len(durees))]:>10.2f}s")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--services", action="store_true")
    args = p.parse_args()
    if args.services:
        services(args)
        return
    proc = subprocess.Popen([sys.executable, str(ICI), "--services"])
    try:
        asyncio.run(client())
    finally:
        proc.kill()
        proc.wait()


if __name__ == "__main__":
    main()
