"""Chapitre 39 : délester, borner la file, servir les dernières arrivées d'abord.

Le service (service.py, port 11901) sert 500 requêtes par seconde (10 places x
20 ms). Les clients envoient leurs requêtes en boucle ouverte (arrivées de
Poisson), attendent au plus 1 s, et marquent 30 % d'entre elles « critique ».

1. surcharge : 300 requêtes/s, puis 1 000/s de t = 10 à 30 s, puis 300/s ;
   aucune nouvelle tentative ; cinq disciplines de file.
2. métastable : le scénario du ch. 38 (300 requêtes/s, service trois fois plus
   lent de t = 10 à 20 s, trois nouvelles tentatives immédiates) ; seule la
   discipline de file du service change.

    python3 delestage.py [1|2] [--sortie ../../../figures/donnees/ch39-delestage.tex]
"""
import argparse
import asyncio
import collections
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

import aiohttp

ICI = Path(__file__).parent
PORT = 11901
URL = f"http://127.0.0.1:{PORT}"
DELAI = 1.0


async def lancer(discipline):
    p = subprocess.Popen([sys.executable, str(ICI / "service.py"), "--port", str(PORT),
                          "--discipline", discipline])
    async with aiohttp.ClientSession() as s:
        for _ in range(200):
            try:
                async with s.get(URL + "/stats"):
                    return p
            except aiohttp.ClientError:
                await asyncio.sleep(0.05)
    raise RuntimeError("service absent")


async def essai(discipline, debit, duree, tentatives, ralenti=None):
    """debit(t) : requêtes par seconde à l'instant t."""
    serveur = await lancer(discipline)
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))
    alea = random.Random(39)
    res = []                    # (seconde d'arrivée, issue, durée, priorité)
    finies = collections.Counter()
    await (await session.post(URL + "/zero")).release()
    t0 = time.monotonic()

    async def demande():
        s0 = int(time.monotonic() - t0)
        prio = "critique" if alea.random() < 0.3 else "secondaire"
        debut = time.monotonic()
        issue = "délai"
        for _ in range(tentatives):
            try:
                async with session.get(URL + "/travail", headers={"X-Priorite": prio},
                                       timeout=aiohttp.ClientTimeout(total=DELAI)) as r:
                    await r.read()
                    if r.status == 200:
                        issue = "ok"
                        finies[int(time.monotonic() - t0)] += 1
                        break
                    issue = "503"
            except (asyncio.TimeoutError, aiohttp.ClientError):
                issue = "délai"
        res.append((s0, issue, time.monotonic() - debut, prio))

    async def chef():
        if ralenti:
            await asyncio.sleep(ralenti[0])
            await (await session.post(URL + "/ralentir?f=3")).release()
            await asyncio.sleep(ralenti[1] - ralenti[0])
            await (await session.post(URL + "/ralentir?f=1")).release()

    pilote = asyncio.create_task(chef())
    taches = set()
    prochain = t0
    while (t := time.monotonic()) - t0 < duree:
        prochain += alea.expovariate(debit(t - t0))
        if prochain > t:
            await asyncio.sleep(prochain - t)
        tache = asyncio.create_task(demande())
        taches.add(tache)
        tache.add_done_callback(taches.discard)
    await asyncio.gather(*taches)
    await pilote
    async with session.get(URL + "/stats") as r:
        srv = await r.json()
    await session.close()
    serveur.kill()
    serveur.wait()
    return res, srv, finies


def resumer(nom, res, srv, phases):
    print(f"   {nom}")
    print(f"     {'phase':<9}{'arr./s':>7}{'ok/s':>6}{'503/s':>6}{'délai/s':>8}"
          f"{'méd.':>6}{'99e':>6}{'inutile/s':>10}{'critiques':>10}")
    for phase, a, b in phases:
        lot = [r for r in res if a <= r[0] < b]
        n = b - a
        ok = [r for r in lot if r[1] == "ok"]
        d = sorted(r[2] * 1000 for r in ok) or [0]
        crit = [r for r in lot if r[3] == "critique"]
        crit_ok = sum(r[1] == "ok" for r in crit)
        perdu = sum(x[2] for x in srv[a:b]) / n
        print(f"     {phase:<9}{len(lot) / n:>7.0f}{len(ok) / n:>6.0f}"
              f"{sum(r[1] == '503' for r in lot) / n:>6.0f}{sum(r[1] == 'délai' for r in lot) / n:>8.0f}"
              f"{statistics.median(d):>6.0f}{d[int(.99 * (len(d) - 1))]:>6.0f}{perdu:>10.0f}"
              f"{100 * crit_ok / max(1, len(crit)):>9.0f}%")


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("partie", nargs="?", default="12")
    p.add_argument("--sortie")
    args = p.parse_args()
    courbes = {}
    if "1" in args.partie:
        print("1. surcharge : 300/s, 1 000/s de 10 à 30 s, puis 300/s ;")
        print("   capacité 500/s ; délai client 1 s, aucune nouvelle tentative ;")
        print("   méd. et 99e : durée des réussites en ms\n")
        debit = lambda t: 1000 if 10 <= t < 30 else 300
        phases = [("avant", 0, 10), ("surcharge", 10, 30), ("après", 30, 40), ("après+10", 40, 50)]
        for disc in ("fifo", "borne", "codel", "lifo", "priorite"):
            res, srv, finies = await essai(disc, debit, 50, 1)
            resumer(disc, res, srv, phases)
            courbes[disc] = finies
            print()
    if "2" in args.partie:
        print("2. métastable : 300/s, service x3 plus lent de 10 à 20 s ;")
        print("   délai client 1 s, 3 nouvelles tentatives immédiates\n")
        phases = [("avant", 0, 10), ("ralenti", 10, 20), ("après 1", 20, 30), ("après 2", 30, 40)]
        for disc in ("fifo", "codel", "lifo"):
            res, srv, _ = await essai(disc, lambda t: 300, 40, 4, ralenti=(10, 20))
            resumer(disc, res, srv, phases)
            print()
    if args.sortie and courbes:
        lignes = ["% Fichier généré par outils/experiences/ch39/delestage.py : ne pas modifier.",
                  "% partie 1 : requêtes réussies par seconde (seconde de fin)"]
        for disc, nom in (("fifo", "Fifo"), ("borne", "Borne"), ("codel", "Codel"), ("lifo", "Lifo"),
                          ("priorite", "Priorite")):
            if disc in courbes:
                pts = ",".join(f"{s}/{courbes[disc][s]}" for s in range(50))
                lignes.append(f"\\def\\ok{nom}{{{pts}}}")
        Path(args.sortie).write_text("\n".join(lignes) + "\n")


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())
