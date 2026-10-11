"""Chapitre 10 : un serveur d'application minimal, avec ou sans état.

  POST /connexion   ouvre une session et pose le cookie « session »
  GET  /profil      200 si la session est connue de ce serveur, 401 sinon
  GET  /calcul      environ 2 ms de calcul, pour mesurer le passage à l'échelle

Les sessions sont gardées soit dans la mémoire du processus (SESSIONS=memoire),
soit dans Redis (SESSIONS=redis, REDIS=hôte:port), partagé par tous les serveurs.
Chaque réponse dit quel serveur l'a produite.
Usage : SESSIONS=memoire python3 app.py PORT   (pip install -r requirements.txt)
"""
import json
import os
import secrets
import sys
import time

from aiohttp import web

PORT = int(sys.argv[1])
MODE = os.environ.get("SESSIONS", "memoire")
sessions = {}
redis = None
mesures = {"lectures": 0, "duree": 0.0, "durees": []}


async def lire_session(cle):
    debut = time.perf_counter()
    if MODE == "redis":
        valeur = await redis.get(f"session:{cle}")
        valeur = json.loads(valeur) if valeur else None
    else:
        valeur = sessions.get(cle)
    mesures["lectures"] += 1
    mesures["duree"] += time.perf_counter() - debut
    mesures["durees"].append(time.perf_counter() - debut)
    return valeur


async def connexion(request):
    cle = secrets.token_urlsafe(16)
    donnees = {"utilisateur": (await request.json())["utilisateur"]}
    if MODE == "redis":
        await redis.set(f"session:{cle}", json.dumps(donnees), ex=1800)
    else:
        sessions[cle] = donnees
    r = web.json_response({"serveur": PORT})
    r.set_cookie("session", cle, httponly=True)
    return r


async def profil(request):
    donnees = await lire_session(request.cookies.get("session", ""))
    if donnees is None:
        return web.json_response({"serveur": PORT, "erreur": "non connecté"}, status=401)
    return web.json_response({"serveur": PORT, **donnees})


async def calcul(request):
    fin, n = time.perf_counter() + 0.002, 0
    while time.perf_counter() < fin:   # 2 ms de processeur, sans attente
        n += 1
    return web.json_response({"serveur": PORT})


async def statistiques(request):
    n = mesures["lectures"] or 1
    d = sorted(mesures["durees"]) or [0.0]
    return web.json_response({"serveur": PORT, "lectures": mesures["lectures"],
                              "lecture_moyenne_us": mesures["duree"] / n * 1e6,
                              "lecture_mediane_us": d[len(d) // 2] * 1e6})


async def demarrer(app):
    global redis
    if MODE == "redis":
        import redis.asyncio as aioredis
        hote, port = os.environ.get("REDIS", "127.0.0.1:6391").split(":")
        redis = aioredis.Redis(host=hote, port=int(port))


app = web.Application()
app.add_routes([web.post("/connexion", connexion), web.get("/profil", profil),
                web.get("/calcul", calcul), web.get("/statistiques", statistiques)])
app.on_startup.append(demarrer)
web.run_app(app, host="127.0.0.1", port=PORT, print=None, access_log=None)
