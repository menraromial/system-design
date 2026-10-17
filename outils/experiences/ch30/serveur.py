"""Chapitre 30 : service HTTP limité par un seau à jetons.

GET /api répond 200, ou 429 avec Retry-After et les champs RateLimit-Policy
et RateLimit du brouillon de l'IETF (draft-ietf-httpapi-ratelimit-headers-11).
La clé de limitation est l'adresse IP du client (--cle ip) ou l'en-tête
X-Cle-API (--cle entete). GET /stats donne les compteurs et le temps
processeur consommé par le serveur.

    python3 serveur.py --port 11030 --cle ip --debit 10 --capacite 20
"""
import argparse
import math
import resource
import time

from aiohttp import web


class Seau:
    def __init__(self, debit, capacite):
        self.debit, self.capacite = debit, capacite
        self.etat = {}

    def prendre(self, cle):
        """(accepté, jetons restants, secondes avant le prochain jeton)."""
        t = time.monotonic()
        jetons, avant = self.etat.get(cle, (self.capacite, t))
        jetons = min(self.capacite, jetons + (t - avant) * self.debit)
        if jetons >= 1:
            self.etat[cle] = (jetons - 1, t)
            return True, jetons - 1, 0.0
        self.etat[cle] = (jetons, t)
        return False, jetons, (1 - jetons) / self.debit


def application(args):
    seau = Seau(args.debit, args.capacite)
    compteurs = {"200": 0, "429": 0}
    politique = f'"par-cle";q={args.debit:g};w=1'

    async def api(requete):
        cle = (requete.headers.get("X-Cle-API", "anonyme")
               if args.cle == "entete" else requete.remote)
        ok, reste, attente = seau.prendre(cle)
        entetes = {"RateLimit-Policy": politique,
                   "RateLimit": f'"par-cle";r={int(reste)};'
                                f't={math.ceil(attente)}'}
        if ok:
            compteurs["200"] += 1
            return web.json_response({"ok": True}, headers=entetes)
        compteurs["429"] += 1
        entetes["Retry-After"] = str(max(1, math.ceil(attente)))
        return web.json_response({"erreur": "trop de requêtes"}, status=429,
                                 headers=entetes)

    async def stats(requete):
        u = resource.getrusage(resource.RUSAGE_SELF)
        return web.json_response({**compteurs, "cpu": u.ru_utime + u.ru_stime})

    app = web.Application()
    app.add_routes([web.get("/api", api), web.get("/stats", stats)])
    return app


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=11030)
    p.add_argument("--cle", choices=["ip", "entete"], default="ip")
    p.add_argument("--debit", type=float, default=10)
    p.add_argument("--capacite", type=float, default=20)
    args = p.parse_args()
    web.run_app(application(args), host="127.0.0.1", port=args.port,
                print=None, access_log=None)
