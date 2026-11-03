"""Chapitre 38 : un service à capacité fixe, que l'on peut ralentir.

Le service a `--places` places de traitement ; une requête attend une place dans
une file sans limite (ordre d'arrivée), puis l'occupe `--service` ms. Comme la
plupart des serveurs, il ne sait pas que le client est parti : une requête
abandonnée par son client est traitée jusqu'au bout. Avec --echeance passee, il
lit l'en-tête X-Echeance-Ms (millisecondes qui restent au client, comme
grpc-timeout) et répond 504 sans rien faire si l'échéance est passée quand vient
son tour ; avec --echeance prevue, il répond 504 aussi quand le service, à sa
durée actuelle, finirait après l'échéance.

    GET  /travail          une requête
    POST /ralentir?f=3     multiplie la durée de service par f (f=1 : normal)
    GET  /stats            compteurs par seconde depuis le démarrage

    python3 serveur.py --port 11801 --places 10 --service 20 [--echeance prevue]
"""
import argparse
import asyncio
import collections
import resource
import time

from aiohttp import web


def application(args):
    places = asyncio.Semaphore(args.places)
    etat = {"facteur": 1.0, "debut": time.monotonic()}
    # par seconde : reçues, traitées, traitées pour rien, écartées (échéance),
    # temps processeur du serveur (s)
    stats = collections.defaultdict(lambda: [0, 0, 0, 0, 0.0])

    def seconde():
        return int(time.monotonic() - etat["debut"])

    async def travail(requete):
        arrivee = time.monotonic()
        stats[seconde()][0] += 1
        reste = requete.headers.get("X-Echeance-Ms")
        async with places:
            service = args.service * etat["facteur"]
            if args.echeance and reste is not None:
                fin = (time.monotonic() - arrivee) * 1000
                if args.echeance == "prevue":
                    fin += service
                if fin >= float(reste):
                    stats[seconde()][3] += 1
                    return web.Response(status=504, text="échéance passée")
            await asyncio.sleep(service / 1000)
        transport = requete.transport
        if transport is None or transport.is_closing():
            stats[seconde()][2] += 1           # le client est déjà parti
        else:
            stats[seconde()][1] += 1
        return web.Response(text="ok")

    async def ralentir(requete):
        etat["facteur"] = float(requete.query.get("f", "1"))
        return web.json_response({"facteur": etat["facteur"]})

    async def lire_stats(requete):
        n = max(stats, default=-1) + 1
        return web.json_response([stats[s] for s in range(n)])

    async def zero(requete):
        etat["debut"] = time.monotonic()
        stats.clear()
        return web.json_response({})

    async def mesurer_cpu(app):
        async def boucle():
            avant = None
            while True:
                u = resource.getrusage(resource.RUSAGE_SELF)
                cpu = u.ru_utime + u.ru_stime
                if avant is not None:
                    stats[seconde()][4] += cpu - avant
                avant = cpu
                await asyncio.sleep(0.25)
        app["cpu"] = asyncio.create_task(boucle())

    app = web.Application()
    app.on_startup.append(mesurer_cpu)
    app.add_routes([web.get("/travail", travail), web.post("/ralentir", ralentir),
                    web.get("/stats", lire_stats), web.post("/zero", zero)])
    return app


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=11801)
    p.add_argument("--places", type=int, default=10)
    p.add_argument("--service", type=float, default=20.0)
    p.add_argument("--echeance", choices=["passee", "prevue"])
    args = p.parse_args()
    web.run_app(application(args), host="127.0.0.1", port=args.port,
                print=None, backlog=4096, access_log=None)
