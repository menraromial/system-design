"""Chapitre 38 : une tempête de retries achève un service qui n'était que lent.

Le service (serveur.py, port 11801) a 10 places et sert une requête en 20 ms,
soit 500 requêtes par seconde. Les clients lui envoient 300 requêtes par seconde
(arrivées de Poisson, en boucle ouverte), avec un délai d'attente de 1 s par
tentative. De t = 10 s à t = 20 s, le service est trois fois plus lent (167
requêtes par seconde), puis il redevient normal. On compare six politiques :

  aucun      une seule tentative ;
  immediat   jusqu'à 3 nouvelles tentatives, aussitôt après l'échec ;
  backoff    jusqu'à 3 nouvelles tentatives, attente exponentielle avec gigue
             complète (base 100 ms, plafond 2 s) ;
  budget     comme « immediat », mais une nouvelle tentative n'est permise que
             si les retries font moins de 10 % des requêtes des 10 dernières
             secondes (budget par client du livre SRE de Google) ;
  echeance   comme « immediat », mais le serveur lit l'échéance du client et
             n'exécute pas une requête dont le client est déjà parti ;
  prevue     comme « echeance », mais le serveur écarte aussi une requête qui
             finirait après l'échéance (attente + durée de service actuelle).

    python3 tempete.py [--duree 40] [--sortie ../../../figures/donnees/ch38-tempete.tex]
"""
import argparse
import asyncio
import collections
import random
import subprocess
import sys
import time
from pathlib import Path

import aiohttp

ICI = Path(__file__).parent
PORT = 11801
URL = f"http://127.0.0.1:{PORT}"
DEBIT, DELAI = 300, 1.0
RALENTI = (10, 20)
MAX_TENTATIVES = 4


class Budget:
    """Rapport retries / requêtes sur une fenêtre glissante de 10 s."""

    def __init__(self, ratio=0.1, fenetre=10):
        self.ratio, self.fenetre = ratio, fenetre
        self.req = collections.Counter()
        self.ret = collections.Counter()

    def _somme(self, c, s):
        return sum(c[x] for x in range(s - self.fenetre + 1, s + 1))

    def requete(self, s):
        self.req[s] += 1

    def autorise(self, s):
        if self._somme(self.ret, s) < self.ratio * self._somme(self.req, s):
            self.ret[s] += 1
            return True
        return False


async def lancer_serveur(politique):
    cmd = [sys.executable, str(ICI / "serveur.py"), "--port", str(PORT)]
    if politique in ("echeance", "prevue"):
        cmd += ["--echeance", {"echeance": "passee", "prevue": "prevue"}[politique]]
    p = subprocess.Popen(cmd)
    async with aiohttp.ClientSession() as s:
        for _ in range(200):
            try:
                async with s.get(URL + "/stats"):
                    return p
            except aiohttp.ClientError:
                await asyncio.sleep(0.05)
    raise RuntimeError("serveur absent")


async def essai(politique, duree):
    serveur = await lancer_serveur(politique)
    connecteur = aiohttp.TCPConnector(limit=0)
    session = aiohttp.ClientSession(connector=connecteur)
    budget = Budget()
    alea = random.Random(38)
    reussites = collections.Counter()       # par seconde d'arrivée
    arrivees = collections.Counter()
    finies = collections.Counter()          # réussites par seconde de fin
    envois = collections.Counter()          # tentatives par seconde
    retards = []
    await (await session.post(URL + "/zero")).release()
    t0 = time.monotonic()

    def seconde():
        return int(time.monotonic() - t0)

    async def demande():
        s0 = seconde()
        arrivees[s0] += 1
        budget.requete(s0)
        for n in range(MAX_TENTATIVES):
            if n > 0:
                if politique == "aucun":
                    return
                if politique == "budget" and not budget.autorise(seconde()):
                    return
                if politique == "backoff":
                    await asyncio.sleep(alea.uniform(0, min(2.0, 0.1 * 2 ** n)))
            envois[seconde()] += 1
            try:
                async with session.get(URL + "/travail", headers={"X-Echeance-Ms": str(int(DELAI * 1000))},
                                       timeout=aiohttp.ClientTimeout(total=DELAI)) as r:
                    await r.read()
                    if r.status == 200:
                        reussites[s0] += 1
                        finies[seconde()] += 1
                        return
            except (asyncio.TimeoutError, aiohttp.ClientError):
                pass

    async def surveiller_boucle():
        while True:
            a = time.monotonic()
            await asyncio.sleep(0.1)
            retards.append(time.monotonic() - a - 0.1)

    async def chef():
        await asyncio.sleep(RALENTI[0])
        await (await session.post(URL + "/ralentir?f=3")).release()
        await asyncio.sleep(RALENTI[1] - RALENTI[0])
        await (await session.post(URL + "/ralentir?f=1")).release()

    taches = set()
    veille = asyncio.create_task(surveiller_boucle())
    pilote = asyncio.create_task(chef())
    prochain = t0
    while (t := time.monotonic()) - t0 < duree:
        prochain += alea.expovariate(DEBIT)
        if prochain > t:
            await asyncio.sleep(prochain - t)
        tache = asyncio.create_task(demande())
        taches.add(tache)
        tache.add_done_callback(taches.discard)
    await asyncio.gather(*taches)
    pilote.cancel()
    veille.cancel()
    async with session.get(URL + "/stats") as r:
        serveur_stats = await r.json()
    await session.close()
    serveur.kill()
    serveur.wait()
    return dict(arrivees=arrivees, reussites=reussites, finies=finies, envois=envois,
                serveur=serveur_stats, retard=max(retards))


PHASES = [("avant", 0, 10), ("ralenti", 10, 20), ("après 1", 20, 30), ("après 2", 30, 40)]


def resumer(nom, r):
    print(f"   {nom}  (retard maximal de la boucle cliente : {r['retard'] * 1000:.0f} ms)")
    print(f"     {'phase':<9}{'reçues/s':>9}{'pour rien/s':>12}{'écartées/s':>11}"
          f"{'réussite':>10}{'CPU srv':>9}")
    srv = r["serveur"]
    for phase, a, b in PHASES:
        lignes = srv[a:b]
        n = max(1, len(lignes))
        recues = sum(x[0] for x in lignes) / n
        perdu = sum(x[2] for x in lignes) / n
        ecart = sum(x[3] for x in lignes) / n
        cpu = sum(x[4] for x in lignes) / n
        arr = sum(r["arrivees"][s] for s in range(a, b))
        ok = sum(r["reussites"][s] for s in range(a, b))
        print(f"     {phase:<9}{recues:>9.0f}{perdu:>12.0f}{ecart:>11.0f}"
              f"{100 * ok / max(1, arr):>9.1f}%{100 * cpu:>8.0f}%")


def ecrire_donnees(chemin, resultats, duree):
    lignes = ["% Fichier généré par outils/experiences/ch38/tempete.py : ne pas modifier.",
              "% réussites par seconde (selon la seconde de fin) et tentatives reçues par le serveur"]
    noms = {"aucun": "Aucun", "immediat": "Immediat", "backoff": "Backoff",
            "budget": "Budget", "echeance": "Echeance", "prevue": "Prevue"}
    for p, r in resultats.items():
        ok = ",".join(f"{s}/{r['finies'][s]}" for s in range(duree))
        recu = ",".join(f"{s}/{x[0]}" for s, x in enumerate(r["serveur"][:duree]))
        lignes.append(f"\\def\\ok{noms[p]}{{{ok}}}")
        lignes.append(f"\\def\\recu{noms[p]}{{{recu}}}")
    Path(chemin).write_text("\n".join(lignes) + "\n")


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--duree", type=int, default=40)
    p.add_argument("--politiques", default="aucun,immediat,backoff,budget,echeance,prevue")
    p.add_argument("--sortie")
    args = p.parse_args()
    print(f"service : 10 places x 20 ms (500/s), ralenti x3 de {RALENTI[0]} à {RALENTI[1]} s ;")
    print(f"clients : {DEBIT} requêtes/s, délai de {DELAI:g} s, au plus {MAX_TENTATIVES} tentatives\n")
    resultats = {}
    for pol in args.politiques.split(","):
        resultats[pol] = await essai(pol, args.duree)
        resumer(pol, resultats[pol])
        print()
    if args.sortie:
        ecrire_donnees(args.sortie, resultats, args.duree)


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
