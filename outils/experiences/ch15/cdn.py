"""Chapitre 15 : un petit CDN pour mesurer ce que fait un cache de périphérie.

Une origine, 8 points de présence (PoP) et, selon l'essai, un bouclier d'origine
(origin shield) entre les deux. Chaque PoP est un cache LRU de 2 000 objets qui
respecte le Cache-Control de l'origine, regroupe les requêtes simultanées pour un
même objet et ajoute 80 ms à chaque requête vers son amont, pour imiter la
distance entre un PoP et l'origine (40 ms dans chaque sens). Tout tourne dans un
seul processus, sur des ports locaux.

Le client envoie 40 000 requêtes pour 10 000 objets de 10 Kio, dont la
popularité suit une loi de Zipf (s = 1), chacune à un PoP tiré au hasard.
Quatre mesures :
  1. taux de succès des PoP et requêtes reçues par l'origine, avec et sans bouclier ;
  2. clé de cache : la moitié des URL portent un paramètre utm_source aléatoire ;
  3. latence d'un succès et d'un échec dans le cache du PoP ;
  4. une mise à jour de l'objet : combien de temps les PoP servent l'ancienne
     version, en attendant l'expiration (TTL de 3 s) ou en purgeant.
Usage : python3 cdn.py   (pip install -r requirements.txt)
"""
import asyncio
import collections
import random
import statistics
import time
import urllib.parse

import aiohttp
from aiohttp import web

ORIGINE, BOUCLIER, POP0 = 9501, 9502, 9510
N_POP, N_OBJETS, N_REQUETES, CONCURRENCE = 8, 10_000, 40_000, 64
TAILLE = 10 * 1024
DELAI_AMONT = 0.080          # aller-retour ajouté entre un PoP et son amont


class Origine:
    def __init__(self):
        self.requetes, self.ttl, self.versions = 0, 600, collections.Counter()

    async def objet(self, request):
        self.requetes += 1
        nom = request.match_info["nom"]
        corps = f"{nom} version {self.versions[nom]}\n".encode().ljust(TAILLE, b".")
        return web.Response(body=corps, headers={"Cache-Control": f"max-age={self.ttl}"})

    async def maj(self, request):
        self.versions[request.match_info["nom"]] += 1
        return web.Response(text="ok")


def cle_complete(chemin, requete):
    return chemin + ("?" + requete if requete else "")


def cle_sans_suivi(chemin, requete):
    """La clé ignore les paramètres de suivi utm_*, qui ne changent pas la réponse."""
    garde = [(k, v) for k, v in urllib.parse.parse_qsl(requete) if not k.startswith("utm_")]
    return chemin + ("?" + urllib.parse.urlencode(sorted(garde)) if garde else "")


class Cache:
    """Un cache HTTP minimal : LRU, Cache-Control: max-age, regroupement des requêtes."""

    def __init__(self, amont, capacite, cle, delai):
        self.amont, self.capacite, self.cle, self.delai = amont, capacite, cle, delai
        self.entrees = collections.OrderedDict()      # clé -> (expiration, corps)
        self.en_vol = {}                              # clé -> futur partagé
        self.succes = self.echecs = 0

    async def demarrer(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))

    async def chercher(self, cle, url):
        await asyncio.sleep(self.delai)               # distance jusqu'à l'amont
        async with self.session.get(url) as r:
            corps = await r.read()
            cc = r.headers.get("Cache-Control", "")
        ttl = int(cc.split("max-age=")[1]) if "max-age=" in cc else 0
        return time.monotonic() + ttl, corps

    async def get(self, request):
        cle = self.cle(request.path, request.query_string)
        entree = self.entrees.get(cle)
        if entree and entree[0] > time.monotonic():
            self.entrees.move_to_end(cle)
            self.succes += 1
            return web.Response(body=entree[1], headers=self.entetes(entree, "HIT"))
        self.echecs += 1
        if cle not in self.en_vol:                    # une seule requête vers l'amont par clé
            url = self.amont + request.path_qs
            self.en_vol[cle] = asyncio.ensure_future(self.chercher(cle, url))
        futur = self.en_vol[cle]
        try:
            entree = await asyncio.shield(futur)
        finally:
            if futur.done():
                self.en_vol.pop(cle, None)
        self.entrees[cle] = entree
        self.entrees.move_to_end(cle)
        while len(self.entrees) > self.capacite:
            self.entrees.popitem(last=False)
        return web.Response(body=entree[1], headers=self.entetes(entree, "MISS"))

    @staticmethod
    def entetes(entree, etat):
        """Transmet en aval la durée de vie qui reste, comme un vrai cache (Age, max-age)."""
        reste = max(0, int(entree[0] - time.monotonic()))
        return {"X-Cache": etat, "Cache-Control": f"max-age={reste}"}

    async def purger(self, request):
        prefixe = "/objet/" + request.match_info["nom"]
        for k in [k for k in self.entrees if k == prefixe or k.startswith(prefixe + "?")]:
            del self.entrees[k]
        return web.Response(text="purgé")


async def servir(port, routes):
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    return runner


async def monter(origine, bouclier, cle):
    """Lance l'origine, éventuellement le bouclier, et les PoP ; renvoie les caches."""
    lances = [await servir(ORIGINE, [web.get("/objet/{nom}", origine.objet),
                                     web.post("/maj/{nom}", origine.maj)])]
    amont = f"http://127.0.0.1:{ORIGINE}"
    caches = []
    if bouclier:
        b = Cache(amont, N_OBJETS, cle, 0.005)        # le bouclier est près de l'origine
        await b.demarrer()
        lances.append(await servir(BOUCLIER, [web.get("/objet/{nom}", b.get),
                                              web.delete("/purge/{nom}", b.purger)]))
        amont, caches = f"http://127.0.0.1:{BOUCLIER}", [b]
    for i in range(N_POP):
        c = Cache(amont, 2_000, cle, DELAI_AMONT)
        await c.demarrer()
        lances.append(await servir(POP0 + i, [web.get("/objet/{nom}", c.get),
                                              web.delete("/purge/{nom}", c.purger)]))
        caches.append(c)
    return lances, caches


async def demonter(lances, caches):
    for c in caches:
        await c.session.close()
    for r in lances:
        await r.cleanup()


def requetes(avec_suivi, graine=15):
    r = random.Random(graine)
    poids = [1 / (k + 1) for k in range(N_OBJETS)]                  # Zipf, s = 1
    objets = r.choices(range(N_OBJETS), weights=poids, k=N_REQUETES)
    liste = []
    for o in objets:
        url = f"/objet/o{o}"
        if avec_suivi and r.random() < 0.5:
            url += f"?utm_source=campagne{r.randrange(1000)}"
        liste.append((r.randrange(N_POP), url))
    return liste


async def charger(liste):
    latences = {"HIT": [], "MISS": []}
    file = asyncio.Queue()
    for x in liste:
        file.put_nowait(x)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
        async def ouvrier():
            while not file.empty():
                pop, url = file.get_nowait()
                t = time.perf_counter()
                async with s.get(f"http://127.0.0.1:{POP0 + pop}{url}") as rep:
                    await rep.read()
                    latences[rep.headers["X-Cache"]].append((time.perf_counter() - t) * 1000)
        await asyncio.gather(*(ouvrier() for _ in range(CONCURRENCE)))
    return latences


async def essai(bouclier, cle, avec_suivi):
    origine = Origine()
    lances, caches = await monter(origine, bouclier, cle)
    try:
        latences = await charger(requetes(avec_suivi))
        pops = caches[1:] if bouclier else caches
        succes = sum(c.succes for c in pops)
        return succes / N_REQUETES, origine.requetes, latences
    finally:
        await demonter(lances, caches)


def centile(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p / 100 * len(v)))]


async def fraicheur(purge):
    """Après une mise à jour, jusqu'à quand un PoP peut-il encore servir l'ancienne version ?
    Chaque PoP est interrogé en parallèle toutes les 50 ms ; on retient l'instant
    d'envoi de la dernière requête qui a reçu l'ancienne version."""
    origine = Origine()
    origine.ttl = 3
    lances, caches = await monter(origine, False, cle_complete)
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
            for i in range(N_POP):                                   # chaque PoP garde la v0
                async with s.get(f"http://127.0.0.1:{POP0 + i}/objet/o1") as r:
                    await r.read()
            await asyncio.sleep(random.Random(1).uniform(0.5, 1.5))
            async with s.post(f"http://127.0.0.1:{ORIGINE}/maj/o1") as r:
                await r.read()
            t0 = time.monotonic()
            if purge:
                async def purger(i):
                    async with s.delete(f"http://127.0.0.1:{POP0 + i}/purge/o1") as r:
                        await r.read()
                await asyncio.gather(*(purger(i) for i in range(N_POP)))
            fin_purge = time.monotonic() - t0

            async def surveiller(i):
                derniere_perimee = 0.0
                while time.monotonic() - t0 < 10:
                    envoi = time.monotonic() - t0
                    async with s.get(f"http://127.0.0.1:{POP0 + i}/objet/o1") as r:
                        if (await r.read()).startswith(b"o1 version 1"):
                            return derniere_perimee
                    derniere_perimee = envoi
                    await asyncio.sleep(0.05)
                return derniere_perimee
            perimes = await asyncio.gather(*(surveiller(i) for i in range(N_POP)))
        return max(perimes), fin_purge
    finally:
        await demonter(lances, caches)


async def main():
    print(f"{N_POP} PoP (LRU de 2 000 objets) ; {N_REQUETES} requêtes, {N_OBJETS} objets de 10 Kio, Zipf")
    print(f"{'essai':<38}{'succès PoP':>11}{'origine':>9}{'part':>8}")
    resultats = {}
    for nom, bouclier, cle, suivi in (
            ("PoP seuls", False, cle_complete, False),
            ("PoP + bouclier d'origine", True, cle_complete, False),
            ("utm_* dans la clé de cache", False, cle_complete, True),
            ("utm_* retirés de la clé", False, cle_sans_suivi, True)):
        taux, n_origine, latences = await essai(bouclier, cle, suivi)
        resultats[nom] = latences
        print(f"{nom:<38}{100 * taux:>10.1f} %{n_origine:>9}{100 * n_origine / N_REQUETES:>7.1f} %")
    lat = resultats["PoP seuls"]
    print("\nlatence vue par le client (PoP seuls, 80 ms ajoutées vers l'origine)")
    for k, nom in (("HIT", "succès"), ("MISS", "échec")):
        print(f"  {nom:<8} médiane {statistics.median(lat[k]):6.1f} ms   99e centile {centile(lat[k], 99):6.1f} ms")
    print("\nmise à jour d'un objet servi par les 8 PoP, TTL de 3 s")
    for purge in (False, True):
        pire, fin_purge = await fraicheur(purge)
        nom = f"purge des 8 PoP ({fin_purge * 1000:.0f} ms)" if purge else "attente de l'expiration"
        print(f"  {nom:<26} dernière réponse périmée demandée à {pire * 1000:5.0f} ms")


if __name__ == "__main__":
    asyncio.run(main())
