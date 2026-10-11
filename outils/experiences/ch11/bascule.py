"""Chapitre 11 : disponibilité observée quand un serveur tombe, selon l'architecture.

Un sondeur envoie une requête toutes les 20 ms pendant 20 s à travers Caddy ;
au bout de 5 s, un serveur d'application (../ch10/app.py) est tué (SIGKILL,
comme une panne brutale). On compte les réponses en erreur et la plus longue
coupure vue par le sondeur, pour cinq configurations :
  A. un seul serveur, relancé aussitôt par un superviseur ;
  B. deux serveurs, aucun contrôle de santé ;
  C. deux serveurs, contrôle passif (un échec retire le serveur 10 s) ;
  D. deux serveurs, contrôle actif toutes les 2 s, sans nouvelle tentative ;
  E. deux serveurs, contrôle passif et nouvelle tentative sur l'autre serveur.
Usage : python3 bascule.py   (pip install -r requirements.txt ; caddy installé)
"""
import asyncio
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time

import aiohttp

APP = pathlib.Path(__file__).parent.parent / "ch10" / "app.py"
DUREE, PERIODE, PANNE = 20.0, 0.02, 5.0

CONFIGS = {
    "A. un serveur, relancé": ("    lb_policy round_robin", 1, True),
    "B. deux, sans contrôle": ("    lb_policy round_robin", 2, False),
    "C. deux, contrôle passif": ("    lb_policy round_robin\n    fail_duration 10s", 2, False),
    "D. deux, contrôle actif 2 s": ("    lb_policy round_robin\n    health_uri /calcul\n"
                                    "    health_interval 2s\n    health_timeout 1s", 2, False),
    "E. deux, passif + reprise": ("    lb_policy round_robin\n    fail_duration 10s\n"
                                  "    lb_try_duration 1s\n    lb_try_interval 50ms", 2, False),
}


def demarrer_app(port):
    return subprocess.Popen([sys.executable, str(APP), str(port)], env=dict(os.environ, SESSIONS="memoire"),
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


async def sonder(session, resultats, debut):
    while (t := time.perf_counter() - debut) < DUREE:
        try:
            async with session.get("http://127.0.0.1:9200/calcul",
                                   timeout=aiohttp.ClientTimeout(total=1)) as r:
                await r.read()
                resultats.append((t, r.status == 200))
        except (aiohttp.ClientError, asyncio.TimeoutError):
            resultats.append((t, False))
        await asyncio.sleep(max(0.0, PERIODE - (time.perf_counter() - debut - t)))


async def essai(nom, directives, nb, relancer):
    ports = [9201 + i for i in range(nb)]
    apps = {p: demarrer_app(p) for p in ports}
    caddyfile = pathlib.Path(tempfile.mkdtemp()) / "Caddyfile"
    caddyfile.write_text("{\n  admin off\n  auto_https off\n}\n:9200 {\n  reverse_proxy "
                         + " ".join(f"127.0.0.1:{p}" for p in ports) + " {\n" + directives + "\n  }\n}\n")
    caddy = subprocess.Popen(["caddy", "run", "--config", str(caddyfile), "--adapter", "caddyfile"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    await asyncio.sleep(2.5)
    resultats, debut = [], time.perf_counter()
    async with aiohttp.ClientSession() as session:
        sonde = asyncio.create_task(sonder(session, resultats, debut))
        await asyncio.sleep(PANNE)
        victime = ports[0]
        apps[victime].send_signal(signal.SIGKILL)
        apps[victime].wait()
        if relancer:
            apps[victime] = demarrer_app(victime)
        await sonde
    for a in apps.values():
        a.terminate()
    caddy.terminate()
    await asyncio.sleep(1)

    erreurs = [t for t, ok in resultats if not ok]
    coupure, plus_longue, precedent_ok = 0.0, 0.0, None
    for t, ok in resultats:
        if not ok:
            precedent_ok = precedent_ok if precedent_ok is not None else t
            plus_longue = max(plus_longue, t - precedent_ok + PERIODE)
        else:
            precedent_ok = None
    dispo = 100 * (1 - len(erreurs) / len(resultats))
    print(f"{nom:28} {len(resultats):5d} {len(erreurs):7d} {dispo:8.2f} % {plus_longue * 1000:8.0f} ms")


async def main():
    print(f"sondage toutes les {PERIODE * 1000:.0f} ms pendant {DUREE:.0f} s ; panne à {PANNE:.0f} s")
    print(f"{'configuration':28} {'requêtes':>5} {'erreurs':>7} {'disponibilité':>10} {'coupure max':>11}")
    for nom, (directives, nb, relancer) in CONFIGS.items():
        await essai(nom, directives, nb, relancer)


asyncio.run(main())
