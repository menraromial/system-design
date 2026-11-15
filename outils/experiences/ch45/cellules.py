"""Chapitre 45 : rayon d'impact d'une flotte partagée, de cellules et du shuffle
sharding, déploiement global ou cellule par cellule, coût du routeur de cellules.

Huit serveurs aiohttp (serveur.py, ports 12501 à 12508) ; un superviseur les
redémarre 1 s après leur arrêt. 200 locataires envoient chacun 4 requêtes par
seconde ; un client essaie un autre serveur autorisé quand le premier échoue.

  1. requête empoisonnée : le locataire 0 envoie, pendant 15 s, une ou quatre
     requêtes par seconde qui font tomber le serveur qui les traite ; nouvelles
     tentatives comprises. Trois façons d'attribuer les serveurs aux locataires :
       flotte partagée   les 8 serveurs pour tout le monde (3 tentatives)
       cellules          4 cellules de 2 serveurs, cellule = locataire mod 4
       shuffle sharding  2 serveurs tirés au hasard par locataire (28 paires)
  2. déploiement d'une version défectueuse (20 % d'erreurs) puis d'une version
     saine, en cellules : sur tous les serveurs d'un coup, ou cellule par
     cellule avec 6 s d'observation ; alarme si plus de 1 % d'erreurs sur 2 s,
     retour automatique à la version 1 ;
  3. routeur de cellules (port 12520) : latence ajoutée par le saut.

    python3 cellules.py          (environ 6 minutes)
"""
import argparse
import asyncio
import itertools
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp import web

ICI = Path(__file__).parent
PORTS = [12501 + i for i in range(8)]
ROUTEUR = 12520
N_LOC, DEBIT = 200, 4.0
REDEMARRAGE_S = 1.0
TIMEOUT = aiohttp.ClientTimeout(total=0.5)


def centile(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))]


# ---- la flotte et son superviseur --------------------------------------------------
class Flotte:
    def __init__(self):
        self.procs, self.version = {}, {}
        self.occupe = set()            # serveurs en cours de remplacement ou de redémarrage
        self.chutes = 0
        self.releves = set()

    def lancer(self, i, v):
        self.version[i] = v
        self.procs[i] = subprocess.Popen([sys.executable, str(ICI / "serveur.py"), str(PORTS[i]), str(v)],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    async def pret(self, s, i):
        while True:
            try:
                async with s.get(f"http://127.0.0.1:{PORTS[i]}/sante", timeout=TIMEOUT) as r:
                    if r.status == 200:
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            await asyncio.sleep(0.02)

    async def demarrer(self, s):
        for i in range(8):
            self.lancer(i, 1)
        await asyncio.gather(*(self.pret(s, i) for i in range(8)))
        self.tache = asyncio.create_task(self.surveiller(s))

    async def surveiller(self, s):
        while True:
            await asyncio.sleep(0.05)
            for i, p in self.procs.items():
                if p.poll() is not None and i not in self.occupe:
                    self.chutes += 1
                    self.occupe.add(i)
                    r = asyncio.create_task(self.relever(s, i))
                    self.releves.add(r)
                    r.add_done_callback(self.releves.discard)

    async def relever(self, s, i):
        await asyncio.sleep(REDEMARRAGE_S)
        self.lancer(i, self.version[i])
        await self.pret(s, i)
        self.occupe.discard(i)

    async def remplacer(self, s, i, v):
        """Déploie la version v sur le serveur i : arrêt, lancement, attente."""
        self.occupe.add(i)
        p = self.procs[i]
        p.terminate()
        p.wait()
        self.lancer(i, v)
        await self.pret(s, i)
        self.occupe.discard(i)

    def arreter(self):
        self.tache.cancel()
        for r in list(self.releves):
            r.cancel()
        for p in self.procs.values():
            p.kill()
            p.wait()


# ---- les clients ---------------------------------------------------------------------
async def requete(s, t, op, autorises, tentatives):
    for i in random.sample(autorises, len(autorises))[:tentatives]:
        try:
            async with s.get(f"http://127.0.0.1:{PORTS[i]}/r", params={"t": t, "op": op},
                             timeout=TIMEOUT) as r:
                await r.read()
                if r.status == 200:
                    return True
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
    return False


class Charge:
    """200 locataires à 4 req/s ; enregistre (instant, locataire, réussite)."""
    def __init__(self, s, autorises, tentatives):
        self.s, self.autorises, self.tentatives = s, autorises, tentatives
        self.journal, self.poison_jusqua, self.poison_depuis = [], 0.0, 1e18
        self.debit_poison = 1.0
        self.fin = False

    async def une(self, t, op):
        ok = await requete(self.s, t, op, self.autorises(t), self.tentatives)
        if op == "normal":
            self.journal.append((time.monotonic(), t, ok))

    async def locataire(self, t):
        await asyncio.sleep(random.random() / DEBIT)
        while not self.fin:
            asyncio.create_task(self.une(t, "normal"))
            await asyncio.sleep(1 / DEBIT)

    async def empoisonneur(self):
        while not self.fin:
            if self.poison_depuis <= time.monotonic() < self.poison_jusqua:
                asyncio.create_task(self.une(0, "poison"))
            await asyncio.sleep(1 / self.debit_poison)

    def lancer(self):
        self.taches = [asyncio.create_task(self.locataire(t)) for t in range(N_LOC)]
        self.taches.append(asyncio.create_task(self.empoisonneur()))

    async def arreter(self):
        self.fin = True
        await asyncio.gather(*self.taches)
        await asyncio.sleep(0.6)


def attribution(mode):
    alea = random.Random(45)
    paires = list(itertools.combinations(range(8), 2))
    tirage = {t: list(alea.choice(paires)) for t in range(N_LOC)}
    if mode == "flotte partagée":
        return lambda t: list(range(8)), 3
    if mode == "cellules":
        return lambda t: [2 * (t % 4), 2 * (t % 4) + 1], 2
    return lambda t: tirage[t], 2


async def poison():
    print("1. requête empoisonnée du locataire 0 pendant 15 s (199 autres locataires)")
    print(f"   {'attribution':<18}{'poison/s':>9}{'réussies':>9}{'touchés':>9}{'hors svc':>9}"
          f"{'chutes':>8}")
    for debit_poison, mode in itertools.product((1.0, 4.0), ("flotte partagée", "cellules",
                                                              "shuffle sharding")):
        autorises, tentatives = attribution(mode)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
            flotte = Flotte()
            await flotte.demarrer(s)
            charge = Charge(s, autorises, tentatives)
            charge.debit_poison = debit_poison
            charge.lancer()
            await asyncio.sleep(3)
            t0 = time.monotonic()
            charge.poison_depuis, charge.poison_jusqua = t0, t0 + 15
            await asyncio.sleep(16)
            await charge.arreter()
            flotte.arreter()
        par_loc = {}
        for t_, loc, ok in charge.journal:
            if t0 <= t_ < t0 + 16 and loc != 0:
                a = par_loc.setdefault(loc, [0, 0])
                a[0] += ok
                a[1] += 1
        tot = sum(a[1] for a in par_loc.values())
        reussies = sum(a[0] for a in par_loc.values()) / tot
        touches = sum(1 for a in par_loc.values() if a[0] < 0.99 * a[1])
        hors = sum(1 for a in par_loc.values() if a[0] < 0.5 * a[1])
        print(f"   {mode:<18}{debit_poison:>9.0f}{100 * reussies:>8.1f}%{touches:>9}{hors:>9}"
              f"{flotte.chutes:>8}")
        if mode == "shuffle sharding" and debit_poison == 4.0:
            p0 = set(autorises(0))
            meme = sum(1 for t in range(1, N_LOC) if set(autorises(t)) == p0)
            un = sum(1 for t in range(1, N_LOC) if len(set(autorises(t)) & p0) == 1)
            print(f"   paire du locataire 0 : {sorted(p0)} ; même paire : {meme} locataires ;")
            print(f"   un seul serveur commun : {un} locataires")
    print("   touchés : plus de 1 % d'échecs ; hors svc : plus de 50 % d'échecs")


# ---- déploiements ----------------------------------------------------------------------
async def deploiement(version, strategie):
    autorises, tentatives = attribution("cellules")
    cellule = {i: i // 2 for i in range(8)}
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
        flotte = Flotte()
        await flotte.demarrer(s)
        charge = Charge(s, autorises, tentatives)
        charge.lancer()
        await asyncio.sleep(3)
        t0 = time.monotonic()

        def alarme(cellules):
            fin = time.monotonic()
            fen = [ok for t_, loc, ok in charge.journal if t_ > fin - 2 and loc % 4 in cellules]
            return len(fen) > 50 and fen.count(False) > 0.01 * len(fen)

        async def poser(serveurs, v):
            for groupe in (serveurs[0::2], serveurs[1::2]):      # un serveur par cellule à la fois
                await asyncio.gather(*(flotte.remplacer(s, i, v) for i in groupe))

        alarme_a, retour = None, False
        vagues = [[0, 1, 2, 3]] if strategie == "globale" else [[0], [1], [2], [3]]
        for cells in vagues:
            serveurs = [i for i in range(8) if cellule[i] in cells]
            await poser(serveurs, version)
            fin_obs = time.monotonic() + 6
            while time.monotonic() < fin_obs:
                await asyncio.sleep(0.25)
                if alarme(cells):
                    alarme_a = time.monotonic() - t0
                    await poser(serveurs, 1)
                    retour = True
                    break
            if retour:
                break
        duree = time.monotonic() - t0
        await asyncio.sleep(1)
        await charge.arreter()
        flotte.arreter()
    apres = [(loc, ok) for t_, loc, ok in charge.journal if t_ >= t0]
    erreurs = sum(1 for _, ok in apres if not ok)
    touches = len({loc for loc, ok in apres if not ok})
    return erreurs, len(apres), touches, alarme_a, duree, retour


async def deploiements():
    print("\n2. déploiement en 4 cellules de 2 serveurs (alarme : plus de 1 % d'erreurs")
    print("   sur 2 s ; observation de 6 s après chaque vague)")
    print(f"   {'version':<12}{'stratégie':<10}{'erreurs':>9}{'touchés':>9}{'alarme':>8}{'durée':>8}"
          f"  issue")
    for version, nom in ((2, "défectueuse"), (3, "saine")):
        for strategie in ("globale", "par cellule"):
            e, n, t, a, d, retour = await deploiement(version, strategie)
            print(f"   {nom:<12}{strategie:<10}{e:>9}{t:>9}{'' if a is None else f'{a:.1f} s':>8}"
                  f"{d:>7.1f}s  {'retour à v1' if retour else 'déployée'}")
    print("   erreurs : requêtes échouées après deux tentatives ; touchés : locataires")
    print("   ayant reçu au moins une erreur ; alarme et durée : depuis le début")


# ---- routeur de cellules -----------------------------------------------------------------
def routeur():
    async def relayer(requete):
        t = int(requete.query["t"])
        i = random.choice([2 * (t % 4), 2 * (t % 4) + 1])          # table : cellule = t mod 4
        async with app["s"].get(f"http://127.0.0.1:{PORTS[i]}/r", params=requete.query) as r:
            return web.Response(status=r.status, body=await r.read())

    async def debut(app):
        app["s"] = aiohttp.ClientSession()

    app = web.Application()
    app.on_startup.append(debut)
    app.add_routes([web.get("/r", relayer)])
    web.run_app(app, host="127.0.0.1", port=ROUTEUR, print=None, access_log=None)


async def saut():
    print("\n3. routeur de cellules : 3 000 requêtes successives, en ms")
    p = subprocess.Popen([sys.executable, __file__, "--routeur"])
    async with aiohttp.ClientSession() as s:
        flotte = Flotte()
        await flotte.demarrer(s)
        while True:
            try:
                async with s.get(f"http://127.0.0.1:{ROUTEUR}/r", params={"t": 1}) as r:
                    await r.read()
                    break
            except aiohttp.ClientError:
                await asyncio.sleep(0.05)
        print(f"   {'chemin':<22}{'médiane':>9}{'99e':>8}")
        for nom, port in (("direct", None), ("par le routeur", ROUTEUR)):
            d = []
            for k in range(3000):
                t = k % N_LOC
                url = (f"http://127.0.0.1:{PORTS[2 * (t % 4)]}/r" if port is None
                       else f"http://127.0.0.1:{port}/r")
                t1 = time.perf_counter()
                async with s.get(url, params={"t": t, "op": "normal"}) as r:
                    await r.read()
                d.append((time.perf_counter() - t1) * 1000)
            print(f"   {nom:<22}{statistics.median(d):>9.2f}{centile(d, .99):>8.2f}")
        flotte.arreter()
    p.kill()
    p.wait()


async def principal():
    await poison()
    await deploiements()
    await saut()


if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--routeur", action="store_true")
    if a.parse_args().routeur:
        routeur()
    else:
        asyncio.run(principal())
