"""Application de test du chapitre 51, une image pour trois rôles (variable MODE).

MODE=web (défaut) : serveur HTTP sur le port 8080.
  GET /travail?ms=N   brûle N ms de temps processeur, puis répond (nom du pod,
                      durée réelle) ;
  GET /sante          sonde de vie, répond toujours 200 ;
  GET /pret           sonde de disponibilité, 200 une fois l'initialisation finie.
  DEMARRAGE_S=s       durée de l'initialisation (chargement simulé d'un cache),
                      pendant laquelle le serveur n'écoute pas encore ;
  MEMOIRE_MO=m        mémoire allouée pendant l'initialisation, par pas de 10 Mo
                      toutes les 100 ms (pour dépasser une limite).
MODE=travailleur : prend des tâches dans la liste Redis « taches » (BRPOP) ;
  chaque tâche attend TACHE_MS ms (appel à un service lent, sans calcul), puis
  pousse « horodatage d'entrée,horodatage de fin » dans la liste « faites ».
"""
import asyncio
import os
import socket
import time

from aiohttp import web

POD = socket.gethostname()


def bruler(ms):
    fin = time.process_time() + ms / 1000
    x = 0
    while time.process_time() < fin:
        for _ in range(1000):
            x += 1
    return x


async def travail(request):
    ms = float(request.query.get("ms", "20"))
    t = time.perf_counter()
    bruler(ms)
    return web.json_response({"pod": POD, "duree_ms": round(1000 * (time.perf_counter() - t), 2)})


async def sante(_):
    return web.Response(text="ok")


def web_main():
    memoire = []
    cible = int(os.environ.get("MEMOIRE_MO", "0"))
    debut = time.monotonic()
    duree = float(os.environ.get("DEMARRAGE_S", "0"))
    while len(memoire) * 10 < cible:
        memoire.append(bytearray(os.urandom(10 * 2**20)))     # pages réellement touchées
        print(f"{len(memoire) * 10} Mo alloués", flush=True)
        time.sleep(0.1)
    time.sleep(max(0.0, duree - (time.monotonic() - debut)))
    app = web.Application()
    app.router.add_get("/travail", travail)
    app.router.add_get("/sante", sante)
    app.router.add_get("/pret", sante)
    app["memoire"] = memoire
    print(f"prêt après {time.monotonic() - debut:.1f} s", flush=True)
    web.run_app(app, host="0.0.0.0", port=8080, print=None)


async def travailleur():
    import redis.asyncio as redis
    r = redis.Redis(host=os.environ.get("REDIS", "redis"), port=6379)
    attente = float(os.environ.get("TACHE_MS", "200")) / 1000
    while True:
        res = await r.brpop(["taches"], timeout=5)
        if res is None:
            continue
        entree = res[1].decode()
        await asyncio.sleep(attente)
        await r.lpush("faites", f"{entree},{time.time():.3f}")


if __name__ == "__main__":
    if os.environ.get("MODE", "web") == "travailleur":
        asyncio.run(travailleur())
    else:
        web_main()
