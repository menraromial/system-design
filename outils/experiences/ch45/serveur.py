"""Un serveur applicatif du chapitre 45.

    python3 serveur.py <port> <version>

GET /r?t=<locataire>&op=normal   travail court (0,3 ms de calcul), 200
GET /r?t=<locataire>&op=poison   requête empoisonnée : déclenche un défaut qui
                                 arrête le processus (os._exit), comme une entrée
                                 que le code ne sait pas traiter
version 2 : défaut introduit par un déploiement, 20 % des requêtes en erreur 500
"""
import hashlib
import os
import random
import sys

from aiohttp import web

PORT, VERSION = int(sys.argv[1]), int(sys.argv[2])


async def r(requete):
    if requete.query.get("op") == "poison":
        os._exit(1)
    h = hashlib.sha256()
    for _ in range(40):
        h.update(b"x" * 1024)
    if VERSION == 2 and random.random() < 0.20:
        return web.Response(status=500, text="défaut de la version 2")
    return web.Response(text=f"v{VERSION} {requete.query.get('t')}")


async def sante(requete):
    return web.Response(text=str(VERSION))


app = web.Application()
app.add_routes([web.get("/r", r), web.get("/sante", sante)])
web.run_app(app, host="127.0.0.1", port=PORT, print=None, access_log=None)
