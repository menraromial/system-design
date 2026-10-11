"""Chapitre 10 : où garder les sessions quand il y a plusieurs serveurs.

Deux serveurs app.py derrière Caddy. Un utilisateur se connecte puis demande
20 fois son profil ; au milieu, le serveur qui a ouvert sa session est arrêté.
Trois configurations :
  1. sessions en mémoire, répartition round-robin ;
  2. sessions en mémoire, sessions collantes (cookie posé par le répartiteur) ;
  3. sessions dans Redis, partagé par les deux serveurs, round-robin.
Prérequis : caddy, docker (conteneur Redis « cours-sd-ch10-redis », supprimé à la fin).
Usage : python3 sessions.py   (pip install -r requirements.txt)
"""
import asyncio
import os
import pathlib
import subprocess
import sys
import tempfile
import time

import aiohttp

ICI = pathlib.Path(__file__).parent
PORTS = (9101, 9102)
CADDY = """{{
	admin off
	auto_https off
}}
:9100 {{
	reverse_proxy 127.0.0.1:9101 127.0.0.1:9102 {{
		lb_policy {politique}
		lb_try_duration 2s
		fail_duration 30s
	}}
}}
"""


def lancer(mode, politique):
    env = dict(os.environ, SESSIONS=mode, REDIS="127.0.0.1:6391")
    serveurs = {p: subprocess.Popen([sys.executable, str(ICI / "app.py"), str(p)], env=env)
                for p in PORTS}
    fichier = pathlib.Path(tempfile.mkdtemp()) / "Caddyfile"
    fichier.write_text(CADDY.format(politique=politique))
    caddy = subprocess.Popen(["caddy", "run", "--config", str(fichier), "--adapter", "caddyfile"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    return serveurs, caddy


async def parcours(serveurs):
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as s:
        async with s.post("http://127.0.0.1:9100/connexion", json={"utilisateur": "ana"}) as r:
            origine = (await r.json())["serveur"]
        codes, vus = [], []
        for i in range(20):
            if i == 10:
                serveurs[origine].terminate()   # panne du serveur qui a ouvert la session
                serveurs[origine].wait()
            async with s.get("http://127.0.0.1:9100/profil") as r:
                codes.append(r.status)
                vus.append((await r.json())["serveur"])
        for _ in range(1000):  # mesure à chaud de la lecture de session sur le serveur restant
            async with s.get("http://127.0.0.1:9100/profil") as r:
                await r.read()
        return origine, codes, vus


async def statistiques():
    resultat = []
    async with aiohttp.ClientSession() as s:
        for p in PORTS:
            try:
                async with s.get(f"http://127.0.0.1:{p}/statistiques") as r:
                    resultat.append(await r.json())
            except aiohttp.ClientError:
                pass
    return resultat


async def main():
    subprocess.run(["docker", "run", "-d", "--rm", "--name", "cours-sd-ch10-redis",
                    "-p", "127.0.0.1:6391:6379", "redis:8.8-alpine"], check=True,
                   stdout=subprocess.DEVNULL)
    try:
        for titre, mode, politique in (("mémoire, round-robin", "memoire", "round_robin"),
                                       ("mémoire, sessions collantes", "memoire", "cookie"),
                                       ("Redis partagé, round-robin", "redis", "round_robin")):
            serveurs, caddy = lancer(mode, politique)
            try:
                origine, codes, vus = await parcours(serveurs)
                stats = await statistiques()
            finally:
                for p in serveurs.values():
                    p.terminate()
                caddy.terminate()
                time.sleep(1)
            avant, apres = codes[:10], codes[10:]
            print(f"{titre} : session ouverte par {origine}")
            print(f"  401 avant la panne : {avant.count(401)}/10, après : {apres.count(401)}/10")
            print(f"  serveurs vus : {' '.join(str(v)[-1] for v in vus)}")
            for st in stats:
                print(f"  lecture de session sur {st['serveur']} : médiane "
                      f"{st['lecture_mediane_us']:.1f} µs sur {st['lectures']} lectures")
    finally:
        subprocess.run(["docker", "rm", "-f", "cours-sd-ch10-redis"], stdout=subprocess.DEVNULL)


asyncio.run(main())
