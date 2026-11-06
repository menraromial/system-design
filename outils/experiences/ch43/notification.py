"""Chapitre 43 : notification d'événement contre transfert d'état par événement.

Un service de commandes (port 12201) garde 10 000 commandes de cinq lignes et
publie un événement à chaque modification dans un journal (un courtier minimal,
port 12202, qui garde les événements et les sert par lots). Pendant les mesures,
le service modifie encore 1 000 commandes par seconde.

Cinq consommateurs lisent chacun les 20 000 premiers événements :
  notification   l'événement ne contient que l'identifiant et la version ;
                 le consommateur rappelle le service pour lire la commande ;
  transfert      l'événement contient la commande entière ; aucun rappel.

On mesure la durée, les appels reçus par le service et son temps de processeur,
les octets lus, et la part des rappels qui trouvent une version plus récente
que celle de l'événement (états intermédiaires que le consommateur ne verra
jamais). Puis le service est suspendu 3 s (SIGSTOP) pendant la lecture, et l'on
compte ce que chaque consommateur a traité pendant ces 3 s (lecture limitée à
2 000 événements par seconde et par consommateur dans ce second essai).

    python3 notification.py
"""
import argparse
import asyncio
import json
import os
import random
import resource
import signal
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp import web

ICI = Path(__file__)
SOURCE, COURTIER = 12201, 12202
N_CMD, N_EV, CONSO = 10_000, 20_000, 5


# ---- le courtier : un journal en mémoire ------------------------------------------
def courtier():
    journal = []

    async def publier(requete):
        journal.extend(await requete.json())
        return web.json_response(len(journal))

    async def lire(requete):
        depuis, n = int(requete.query["depuis"]), int(requete.query.get("n", 500))
        forme = requete.query["forme"]
        return web.json_response([e[forme] for e in journal[depuis:depuis + n]])

    async def taille(requete):
        return web.json_response(len(journal))

    app = web.Application(client_max_size=64 * 1024 * 1024)
    app.add_routes([web.post("/publier", publier), web.get("/lire", lire), web.get("/taille", taille)])
    web.run_app(app, host="127.0.0.1", port=COURTIER, print=None, access_log=None)


# ---- le service de commandes -----------------------------------------------------
def service():
    alea = random.Random(43)
    commandes = {i: {"id": i, "version": 0, "client": f"client-{i % 977}", "statut": "créée",
                     "lignes": [{"article": alea.randrange(5000), "quantite": alea.randint(1, 4),
                                 "prix": alea.randint(100, 9000)} for _ in range(5)]}
                 for i in range(N_CMD)}
    etat = {"appels": 0, "s": None, "actif": False}

    def modifier():
        i = alea.randrange(N_CMD)
        c = commandes[i]
        c["version"] += 1
        c["statut"] = alea.choice(["payée", "préparée", "expédiée", "livrée"])
        c["lignes"][0]["quantite"] = alea.randint(1, 4)
        return {"notification": {"id": i, "version": c["version"]},
                "transfert": json.loads(json.dumps(c))}

    async def lire(requete):
        etat["appels"] += 1
        return web.json_response(commandes[int(requete.match_info["id"])])

    async def stats(requete):
        u = resource.getrusage(resource.RUSAGE_SELF)
        return web.json_response({"appels": etat["appels"], "cpu": u.ru_utime + u.ru_stime})

    async def produire(requete):
        """Produit n événements d'un coup (préparation du journal)."""
        n = int(requete.query["n"])
        evs = [modifier() for _ in range(n)]
        async with etat["s"].post(f"http://127.0.0.1:{COURTIER}/publier", json=evs) as r:
            await r.read()
        return web.json_response(n)

    async def demarrage(app):
        etat["s"] = aiohttp.ClientSession()

        async def continu():                       # 1 000 modifications par seconde
            while True:
                await asyncio.sleep(0.01)
                if not etat["actif"]:
                    continue
                evs = [modifier() for _ in range(10)]
                try:
                    async with etat["s"].post(f"http://127.0.0.1:{COURTIER}/publier", json=evs) as r:
                        await r.read()
                except aiohttp.ClientError:
                    pass
        app["continu"] = asyncio.create_task(continu())

    async def lancer(requete):
        etat["actif"] = True
        return web.json_response(True)

    app = web.Application()
    app.on_startup.append(demarrage)
    app.add_routes([web.get("/commandes/{id}", lire), web.get("/stats", stats),
                    web.post("/produire", produire), web.post("/lancer", lancer)])
    web.run_app(app, host="127.0.0.1", port=SOURCE, print=None, access_log=None)


# ---- les consommateurs ------------------------------------------------------------
async def consommateur(s, mode, compteurs, horodatages, cadence=None):
    pos, octets, perimes, traites = 0, 0, 0, 0
    t0 = time.monotonic()
    copie = {}
    while pos < N_EV:
        async with s.get(f"http://127.0.0.1:{COURTIER}/lire",
                         params={"depuis": pos, "n": min(500, N_EV - pos), "forme": mode}) as r:
            corps = await r.read()
        octets += len(corps)
        lot = json.loads(corps)
        if mode == "notification":
            for n in lot:
                async with s.get(f"http://127.0.0.1:{SOURCE}/commandes/{n['id']}") as r:
                    corps = await r.read()
                    octets += len(corps)
                    c = json.loads(corps)
                perimes += c["version"] > n["version"]
                copie[c["id"]] = c
                traites += 1
                horodatages.append(time.monotonic())
        else:
            for c in lot:
                copie[c["id"]] = c
                traites += 1
                horodatages.append(time.monotonic())
        pos += len(lot)
        if cadence:                                  # au plus `cadence` événements par seconde
            await asyncio.sleep(max(0.0, traites / cadence - (time.monotonic() - t0)))
    compteurs.append((octets, perimes, traites))


async def attendre(port):
    for _ in range(200):
        try:
            _, e = await asyncio.open_connection("127.0.0.1", port)
            e.close()
            return
        except OSError:
            await asyncio.sleep(0.05)


async def essai(mode, suspendre=False):
    pc = subprocess.Popen([sys.executable, str(ICI), "--courtier"])
    ps = subprocess.Popen([sys.executable, str(ICI), "--service"])
    await attendre(COURTIER)
    await attendre(SOURCE)
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
            async with s.post(f"http://127.0.0.1:{SOURCE}/produire", params={"n": N_EV}) as r:
                await r.read()
            async with s.post(f"http://127.0.0.1:{SOURCE}/lancer") as r:
                await r.read()
            async with s.get(f"http://127.0.0.1:{SOURCE}/stats") as r:
                avant = await r.json()
            compteurs, horos = [], []
            t0 = time.monotonic()
            fenetre = None
            if suspendre:
                async def pause():
                    nonlocal fenetre
                    await asyncio.sleep(1.0)
                    os.kill(ps.pid, signal.SIGSTOP)
                    debut = time.monotonic()
                    await asyncio.sleep(3.0)
                    os.kill(ps.pid, signal.SIGCONT)
                    fenetre = (debut, time.monotonic())
                p = asyncio.create_task(pause())
            cadence = 2000 if suspendre else None
            await asyncio.gather(*(consommateur(s, mode, compteurs, horos, cadence) for _ in range(CONSO)))
            duree = time.monotonic() - t0
            if suspendre:
                await p
            async with s.get(f"http://127.0.0.1:{SOURCE}/stats") as r:
                apres = await r.json()
        octets = sum(c[0] for c in compteurs) / CONSO
        perimes = sum(c[1] for c in compteurs) / sum(c[2] for c in compteurs)
        res = {"duree": duree, "appels": apres["appels"] - avant["appels"], "cpu": apres["cpu"] - avant["cpu"],
               "octets": octets, "perimes": perimes}
        if fenetre:
            res["pendant"] = sum(fenetre[0] <= h < fenetre[1] for h in horos) / CONSO
        return res
    finally:
        for p_ in (ps, pc):
            os.kill(p_.pid, signal.SIGCONT)
            p_.kill()
            p_.wait()


async def client():
    print(f"{N_CMD} commandes de 5 lignes ; {CONSO} consommateurs lisent chacun")
    print(f"{N_EV} événements ;")
    print("le service modifie 1 000 commandes par seconde pendant la lecture\n")
    print(f"{'mode':<14}{'durée':>8}{'appels':>9}{'CPU source':>12}{'Mo lus/conso':>14}{'périmés':>9}")
    for mode in ("notification", "transfert"):
        r = await essai(mode)
        print(f"{mode:<14}{r['duree']:>7.1f}s{r['appels']:>9}{r['cpu']:>11.1f}s{r['octets'] / 1e6:>14.2f}"
              f"{100 * r['perimes']:>8.1f}%")
    print("\nlecture limitée à 2 000 événements par seconde et par consommateur ;")
    print("service suspendu 3 s, 1 s après le début de la lecture ;")
    print("événements traités par consommateur pendant la suspension :")
    for mode in ("notification", "transfert"):
        r = await essai(mode, suspendre=True)
        print(f"   {mode:<14}{r['pendant']:>6.0f}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--courtier", action="store_true")
    p.add_argument("--service", action="store_true")
    a = p.parse_args()
    if a.courtier:
        courtier()
    elif a.service:
        service()
    else:
        asyncio.run(client())


if __name__ == "__main__":
    main()
