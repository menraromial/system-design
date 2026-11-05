"""Chapitre 39 : une dépendance qui ne répond plus, et la panne qui remonte.

Trois services dans un sous-processus (option --services) :
  B (port 11911) sert les profils en 10 ms ; « pendu », il ne répond plus ;
  C (port 11912) sert la recherche en 10 ms ;
  A (port 11910) expose /profil, qui appelle B, et /recherche, qui appelle C,
  avec un délai d'attente de 2 s. A a 20 « fils » de traitement (un sémaphore
  pris pour toute la durée d'une requête, comme un serveur à fils).

Les clients envoient 100 /profil et 100 /recherche par seconde (boucle
ouverte, délai 3 s, aucune nouvelle tentative) pendant 45 s ; B est pendu de
t = 10 à 30 s. Cinq réglages de A :
  aucun         les 20 fils sont communs, une requête attend un fil libre ;
  cloisons      10 fils pour /profil, 10 pour /recherche, et une requête qui
                ne trouve pas de fil libre dans le sien est refusée (503),
                comme un pool de Hystrix sans file d'attente ;
  disjoncteur   fils communs ; disjoncteur sur B réglé comme celui de Hystrix
                (au moins 20 appels et 50 % d'échecs sur 10 s pour s'ouvrir,
                5 s ouvert, puis un appel d'essai) ;
  court         même disjoncteur, mais une fenêtre de 2 s au lieu de 10 ;
  repli         disjoncteur à fenêtre de 2 s, et /profil répond un profil par
                défaut (200, X-Degrade: 1) quand B échoue ou que le disjoncteur
                est ouvert.

    python3 cascade.py
"""
import argparse
import asyncio
import collections
import random
import statistics
import textwrap
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp import web

ICI = Path(__file__)
PA, PB, PC = 11910, 11911, 11912
DELAI_A, DELAI_CLIENT = 2.0, 3.0


class Disjoncteur:
    def __init__(self, fenetre=10.0, minimum=20, seuil=0.5, ouvert=5.0):
        self.fenetre, self.minimum, self.seuil, self.duree = fenetre, minimum, seuil, ouvert
        self.etat, self.depuis = "fermé", time.monotonic()
        self.resultats = collections.deque()       # (instant, réussi)
        self.essai_en_cours = False
        self.journal = []

    def _passer(self, etat):
        self.etat, self.depuis = etat, time.monotonic()
        self.journal.append((self.depuis, etat))

    def autorise(self):
        if self.etat == "ouvert":
            if time.monotonic() - self.depuis < self.duree:
                return False
            self._passer("semi-ouvert")
            self.essai_en_cours = False
        if self.etat == "semi-ouvert":
            if self.essai_en_cours:
                return False
            self.essai_en_cours = True
        return True

    def noter(self, reussi):
        t = time.monotonic()
        if self.etat == "semi-ouvert":
            self._passer("fermé" if reussi else "ouvert")
            self.resultats.clear()
            return
        if self.etat != "fermé":
            return
        self.resultats.append((t, reussi))
        while self.resultats and self.resultats[0][0] < t - self.fenetre:
            self.resultats.popleft()
        n = len(self.resultats)
        echecs = sum(not r for _, r in self.resultats)
        if n >= self.minimum and echecs / n >= self.seuil:
            self._passer("ouvert")


# ---- les trois services ------------------------------------------------------
def services():
    etat = {"pendu": False, "reglage": "aucun", "debut": time.monotonic()}
    appels_b = collections.Counter()
    partage = {}

    def regler_a(reglage):
        partage["fils"] = asyncio.Semaphore(20)
        partage["profil"] = asyncio.Semaphore(10)
        partage["recherche"] = asyncio.Semaphore(10)
        partage["disj"] = Disjoncteur(fenetre=2.0 if reglage in ("court", "repli") else 10.0)
        etat["reglage"] = reglage

    async def service_b(requete):
        appels_b[int(time.monotonic() - etat["debut"])] += 1
        if etat["pendu"]:
            await asyncio.sleep(600)
        await asyncio.sleep(0.01)
        return web.json_response({"nom": "Ada", "photo": "/p/ada.jpg"})

    async def service_c(requete):
        await asyncio.sleep(0.01)
        return web.json_response({"resultats": 10})

    async def appeler(port):
        async with partage["s"].get(f"http://127.0.0.1:{port}/",
                                    timeout=aiohttp.ClientTimeout(total=DELAI_A)) as r:
            await r.read()
            return r.status == 200

    async def traiter(chemin):
        r = etat["reglage"]
        if chemin == "recherche":
            ok = await appeler(PC)
            return web.Response(text="ok") if ok else web.Response(status=502)
        disj = partage["disj"] if r in ("disjoncteur", "court", "repli") else None
        repli = web.json_response({"nom": "", "photo": "/p/defaut.jpg"}, headers={"X-Degrade": "1"})
        if disj and not disj.autorise():
            return repli if r == "repli" else web.Response(status=503, text="disjoncteur ouvert")
        try:
            ok = await appeler(PB)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            ok = False
        if disj:
            disj.noter(ok)
        if ok:
            return web.Response(text="ok")
        return repli if r == "repli" else web.Response(status=504)

    def point_a(chemin):
        async def gerer(requete):
            if etat["reglage"] == "cloisons":
                sem = partage[chemin]
                if sem.locked():
                    return web.Response(status=503, text="cloison pleine")
            else:
                sem = partage["fils"]
            async with sem:
                try:
                    return await traiter(chemin)
                except (asyncio.TimeoutError, aiohttp.ClientError):
                    return web.Response(status=504)
        return gerer

    async def regler(requete):
        etat["pendu"] = requete.query.get("pendu") == "1"
        if requete.query.get("zero") == "1":         # nouvel essai : état neuf
            regler_a(requete.query.get("reglage", "aucun"))
            etat["debut"] = time.monotonic()
            appels_b.clear()
        return web.json_response(etat)

    async def lire(requete):
        n = max(appels_b, default=-1) + 1
        journal = [(round(t - etat["debut"], 2), e) for t, e in partage["disj"].journal]
        return web.json_response({"appels_b": [appels_b[s] for s in range(n)], "journal": journal})

    async def principal():
        partage["s"] = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))
        regler_a("aucun")
        for port, routes in ((PA, [web.get("/profil", point_a("profil")),
                                   web.get("/recherche", point_a("recherche")),
                                   web.get("/regler", regler), web.get("/stats", lire)]),
                             (PB, [web.get("/", service_b)]),
                             (PC, [web.get("/", service_c)])):
            app = web.Application()
            app.add_routes(routes)
            r = web.AppRunner(app, access_log=None)
            await r.setup()
            await web.TCPSite(r, "127.0.0.1", port, backlog=4096).start()
        await asyncio.Event().wait()

    asyncio.run(principal())


# ---- le client ---------------------------------------------------------------
async def regler(s, **q):
    async with s.get(f"http://127.0.0.1:{PA}/regler", params={k: str(v) for k, v in q.items()}) as r:
        await r.read()


async def essai(s, reglage):
    await regler(s, reglage=reglage, pendu=0, zero=1)
    alea = random.Random(39)
    res = []                  # (seconde, chemin, issue, durée)
    t0 = time.monotonic()

    async def demande(chemin):
        s0 = int(time.monotonic() - t0)
        debut = time.monotonic()
        try:
            async with s.get(f"http://127.0.0.1:{PA}/{chemin}",
                             timeout=aiohttp.ClientTimeout(total=DELAI_CLIENT)) as r:
                await r.read()
                if r.status == 200:
                    issue = "dégradé" if r.headers.get("X-Degrade") else "ok"
                else:
                    issue = str(r.status)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            issue = "délai"
        res.append((s0, chemin, issue, time.monotonic() - debut))

    async def chef():
        await asyncio.sleep(10)
        await regler(s, reglage=reglage, pendu=1)
        await asyncio.sleep(20)
        await regler(s, reglage=reglage, pendu=0)

    pilote = asyncio.create_task(chef())
    taches = []
    prochain = t0
    while (t := time.monotonic()) - t0 < 45:
        prochain += alea.expovariate(200)
        if prochain > t:
            await asyncio.sleep(prochain - t)
        taches.append(asyncio.create_task(demande("profil" if alea.random() < 0.5 else "recherche")))
    await asyncio.gather(*taches)
    await pilote
    async with s.get(f"http://127.0.0.1:{PA}/stats") as r:
        stats = await r.json()
    return res, stats


def resumer(nom, res, stats):
    print(f"   {nom}")
    print(f"     {'phase':<9}{'recherche ok':>13}{'99e (ms)':>9}{'profil ok':>10}{'dont dégradé':>13}"
          f"{'appels à B/s':>13}")
    b = stats["appels_b"]
    for phase, a, z in (("avant", 0, 10), ("panne", 10, 30), ("après", 30, 35), ("après+5", 35, 45)):
        rech = [r for r in res if r[1] == "recherche" and a <= r[0] < z]
        prof = [r for r in res if r[1] == "profil" and a <= r[0] < z]
        rok = [r[3] * 1000 for r in rech if r[2] == "ok"]
        rok.sort()
        p99 = rok[int(.99 * (len(rok) - 1))] if rok else float("nan")
        pok = sum(r[2] in ("ok", "dégradé") for r in prof)
        deg = sum(r[2] == "dégradé" for r in prof)
        print(f"     {phase:<9}{100 * len(rok) / max(1, len(rech)):>12.1f}%{p99:>9.0f}"
              f"{100 * pok / max(1, len(prof)):>9.1f}%{100 * deg / max(1, len(prof)):>12.1f}%"
              f"{sum(b[a:z]) / (z - a):>13.0f}")
    if stats["journal"]:
        texte = "disjoncteur : " + ", ".join(f"{e} à {t:g} s" for t, e in stats["journal"][:8])
        print(textwrap.fill(texte, 72, initial_indent="     ", subsequent_indent="       "))


async def client():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
        for _ in range(100):
            try:
                await regler(s)
                break
            except aiohttp.ClientError:
                await asyncio.sleep(0.05)
        print("A : 20 fils, délai 2 s vers B et C ;")
        print("clients : 100 /profil et 100 /recherche par s, délai 3 s ;")
        print("B ne répond plus de t = 10 à 30 s\n")
        for reglage in ("aucun", "cloisons", "disjoncteur", "court", "repli"):
            res, stats = await essai(s, reglage)
            resumer(reglage, res, stats)
            print()
            await asyncio.sleep(3)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--services", action="store_true")
    if p.parse_args().services:
        services()
        return
    proc = subprocess.Popen([sys.executable, str(ICI), "--services"])
    try:
        asyncio.run(client())
    finally:
        proc.kill()
        proc.wait()


if __name__ == "__main__":
    main()
