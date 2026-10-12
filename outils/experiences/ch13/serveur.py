"""Chapitre 13 : un serveur d'application à capacité limitée, pour la répartition.

Le serveur traite au plus K requêtes à la fois (K « fils » de travail) et chaque
requête occupe un fil pendant S millisecondes : sa capacité est donc K / S
requêtes par seconde. Au-delà, les requêtes attendent leur tour.

  GET /travail       occupe un fil pendant S ms, renvoie l'identifiant du serveur
  GET /objet/<nom>   idem, pour les essais de hachage d'URI
  GET /sante         200, ou 503 pendant le drainage
  GET /stats         nombre de requêtes servies ; /raz remet le compteur à zéro

Signaux : SIGTERM déclenche un arrêt selon MODE_ARRET :
  drainage  /sante répond 503, les requêtes continuent d'être servies ; le
            processus s'arrête quand aucune requête n'est arrivée depuis 1 s
            et qu'aucune n'est en cours ;
  immediat  le processus ferme son port d'écoute et ses connexions inactives,
            finit les requêtes en cours et s'arrête, sans rien signaler au
            répartiteur.
Usage : python3 serveur.py PORT K S_MS   (pip install -r requirements.txt)
"""
import asyncio
import os
import signal
import sys
import time

from aiohttp import web

PORT, K, S = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]) / 1000
MODE_ARRET = os.environ.get("MODE_ARRET", "drainage")
fils = asyncio.Semaphore(K)
etat = {"servies": 0, "en_cours": 0, "derniere": time.monotonic(), "drainage": False}


async def travail(request):
    etat["en_cours"] += 1
    etat["derniere"] = time.monotonic()
    try:
        async with fils:
            await asyncio.sleep(S)
        etat["servies"] += 1
        return web.Response(text=str(PORT))
    finally:
        etat["en_cours"] -= 1


async def sante(request):
    return web.Response(status=503 if etat["drainage"] else 200)


async def stats(request):
    return web.json_response({"serveur": PORT, "servies": etat["servies"]})


async def raz(request):
    etat["servies"] = 0
    return web.Response(text="ok")


async def main():
    app = web.Application()
    app.add_routes([web.get("/travail", travail), web.get("/objet/{nom}", travail),
                    web.get("/sante", sante), web.get("/stats", stats), web.get("/raz", raz)])
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", PORT, backlog=1024)
    await site.start()
    fin = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, fin.set)
    await fin.wait()
    if MODE_ARRET == "drainage":
        etat["drainage"] = True
        while etat["en_cours"] or time.monotonic() - etat["derniere"] < 1.0:
            await asyncio.sleep(0.05)
    # dans les deux cas, arrêt standard d'aiohttp : port fermé, connexions
    # inactives fermées, requêtes en cours terminées
    await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
