"""Chapitre 39 : un service à capacité fixe et sa file d'attente.

Le service a `--places` places et occupe une place `--service` ms par requête
(10 x 20 ms = 500 requêtes par seconde). Une requête qui ne trouve pas de place
attend dans une file, gérée selon `--discipline` :

  fifo      file sans limite, ordre d'arrivée ;
  borne     ordre d'arrivée, au plus `--borne` requêtes en attente ; au-delà,
            réponse 503 immédiate ;
  codel     variante de CoDel décrite par Facebook (« Fail at Scale ») : si la
            file n'a pas été vide depuis N = 100 ms, une requête attend au plus
            M = 5 ms, sinon au plus N ; au-delà, 503. La file compte comme vide
            quand une place reste libre faute de requête à servir ;
  lifo      CoDel, et la file est servie dans l'ordre inverse (dernière arrivée,
            première servie) dès qu'elle n'a pas été vide depuis N (LIFO adaptatif) ;
  priorite  au plus `--borne` en attente ; les requêtes « critique » (en-tête
            X-Priorite) passent avant les autres, une requête secondaire est
            refusée dès que `--seuil` requêtes attendent, et une critique qui
            trouve la file pleine en chasse la dernière secondaire.

Une requête dont le client est parti est traitée jusqu'au bout, comme dans la
plupart des serveurs.

    GET  /travail          une requête (en-tête X-Priorite : critique ou secondaire)
    POST /ralentir?f=3     multiplie la durée de service par f
    GET  /stats            par seconde : reçues, servies, pour rien, refusées,
                           critiques servies
    POST /zero             remet les compteurs à zéro

    python3 service.py --port 11901 --discipline codel
"""
import argparse
import asyncio
import collections
import time

from aiohttp import web

M, N = 0.005, 0.100


class Refus(Exception):
    pass


class Ordonnanceur:
    def __init__(self, places, discipline, borne, seuil):
        self.libres, self.discipline, self.borne, self.seuil = places, discipline, borne, seuil
        self.file = collections.deque()          # [futur, arrivée, attente max, priorité]
        self.vide_depuis = time.monotonic()

    def _longue(self, t):
        return self.vide_depuis < t - N            # file non vide depuis plus de N

    def _refuser(self, entree):
        if not entree[0].done():
            entree[0].set_exception(Refus())

    async def entrer(self, prio):
        t = time.monotonic()
        if not self.file and self.libres > 0:   # file vide et place libre
            self.vide_depuis = t
            self.libres -= 1
            return
        d = self.discipline
        if d in ("borne", "priorite") and len(self.file) >= self.borne:
            secondaires = [e for e in self.file if e[3] != "critique"]
            if d == "priorite" and prio == "critique" and secondaires:
                victime = secondaires[-1]
                self.file.remove(victime)
                self._refuser(victime)
            else:
                raise Refus()
        if d == "priorite" and prio != "critique" and len(self.file) >= self.seuil:
            raise Refus()
        attente = (M if self._longue(t) else N) if d in ("codel", "lifo") else None
        futur = asyncio.get_running_loop().create_future()
        self.file.append([futur, t, attente, prio])
        await futur

    def _choisir(self, t):
        if self.discipline == "lifo" and self._longue(t):
            return self.file.pop()
        if self.discipline == "priorite":
            for e in self.file:
                if e[3] == "critique":
                    self.file.remove(e)
                    return e
        return self.file.popleft()

    def liberer(self):
        t = time.monotonic()
        while self.file:
            e = self._choisir(t)
            if e[0].done():
                continue
            if e[2] is not None and t - e[1] > e[2]:
                self._refuser(e)
                continue
            e[0].set_result(None)                 # la place passe à cette requête
            return
        self.libres += 1                          # une place reste libre : file vide
        self.vide_depuis = t

    def purger(self):
        """Refuse les requêtes qui ont dépassé leur attente maximale (CoDel)."""
        if self.discipline not in ("codel", "lifo"):
            return
        t = time.monotonic()
        garder = collections.deque()
        for e in self.file:
            if e[2] is not None and t - e[1] > e[2]:
                self._refuser(e)
            elif not e[0].done():
                garder.append(e)
        self.file = garder


def application(args):
    ordo = Ordonnanceur(args.places, args.discipline, args.borne, args.seuil)
    etat = {"facteur": 1.0, "debut": time.monotonic()}
    stats = collections.defaultdict(lambda: [0, 0, 0, 0, 0])

    def seconde():
        return int(time.monotonic() - etat["debut"])

    async def travail(requete):
        prio = requete.headers.get("X-Priorite", "secondaire")
        stats[seconde()][0] += 1
        try:
            await ordo.entrer(prio)
        except Refus:
            stats[seconde()][3] += 1
            return web.Response(status=503, text="surchargé")
        try:
            await asyncio.sleep(args.service / 1000 * etat["facteur"])
        finally:
            ordo.liberer()
        t = requete.transport
        if t is None or t.is_closing():
            stats[seconde()][2] += 1
        else:
            stats[seconde()][1] += 1
            if prio == "critique":
                stats[seconde()][4] += 1
        return web.Response(text="ok")

    async def ralentir(requete):
        etat["facteur"] = float(requete.query.get("f", "1"))
        return web.json_response(etat["facteur"])

    async def lire(requete):
        n = max(stats, default=-1) + 1
        return web.json_response([stats[s] for s in range(n)])

    async def zero(requete):
        etat["debut"] = time.monotonic()
        stats.clear()
        return web.json_response({})

    async def purge(app):
        async def boucle():
            while True:
                await asyncio.sleep(0.01)
                ordo.purger()
        app["purge"] = asyncio.create_task(boucle())

    app = web.Application()
    app.on_startup.append(purge)
    app.add_routes([web.get("/travail", travail), web.post("/ralentir", ralentir),
                    web.get("/stats", lire), web.post("/zero", zero)])
    return app


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=11901)
    p.add_argument("--places", type=int, default=10)
    p.add_argument("--service", type=float, default=20.0)
    p.add_argument("--discipline", choices=["fifo", "borne", "codel", "lifo", "priorite"], default="fifo")
    p.add_argument("--borne", type=int, default=50)
    p.add_argument("--seuil", type=int, default=20)
    args = p.parse_args()
    web.run_app(application(args), host="127.0.0.1", port=args.port, print=None,
                backlog=4096, access_log=None)
