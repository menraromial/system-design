"""Chapitre 38 : une réplique qui répond vite, sauf pendant ses pauses.

GET /lire répond après 1 à 3 ms. De temps en temps (en moyenne toutes les
`--intervalle` s), le processus s'arrête tout entier pendant 50 à 150 ms, comme
pendant une pause du ramasse-miettes ou un compactage : les requêtes qui
arrivent alors attendent la fin de la pause, quelle qu'elles soient.

    python3 replica.py --port 11840 --intervalle 5
"""
import argparse
import asyncio
import random
import time

from aiohttp import web


def application(args):
    alea = random.Random(args.port)

    async def lire(requete):
        await asyncio.sleep(alea.uniform(0.001, 0.003))
        return web.Response(text="ok")

    async def pauses(app):
        async def boucle():
            while True:
                await asyncio.sleep(alea.expovariate(1 / args.intervalle))
                time.sleep(alea.uniform(0.05, 0.15))     # bloque tout le processus
        app["pauses"] = asyncio.create_task(boucle())

    app = web.Application()
    app.on_startup.append(pauses)
    app.add_routes([web.get("/lire", lire)])
    return app


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=11840)
    p.add_argument("--intervalle", type=float, default=5.0)
    args = p.parse_args()
    web.run_app(application(args), host="127.0.0.1", port=args.port, print=None, access_log=None)
