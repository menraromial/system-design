"""Chapitre 30 : le module limit_req de nginx (seau percé), mesuré.

Lance nginx 1.30 dans le conteneur cours-sd-ch30-nginx (port 11031), avec
cinq emplacements limités à 10 requêtes par seconde, chacun avec sa zone :
sans rafale, burst=10, burst=10 nodelay, burst=10 delay=5, et burst=10
nodelay avec limit_req_status 429. Pour chacun, 30 requêtes simultanées,
puis un client régulier à 25 requêtes par seconde pendant 10 s.

    python3 nginx.py
"""
import asyncio
import shutil
import statistics
import subprocess
import time
from pathlib import Path

import aiohttp

NOM, PORT, IMAGE = "cours-sd-ch30-nginx", 11031, "nginx:1.30-alpine"
DOSSIER = Path.home() / ".cache" / "cours-sd-ch30"
URL = f"http://127.0.0.1:{PORT}"

EMPLACEMENTS = {
    "/sans": ("sans rafale", ""),
    "/burst": ("burst=10", " burst=10"),
    "/nodelay": ("burst=10 nodelay", " burst=10 nodelay"),
    "/delay": ("burst=10 delay=5", " burst=10 delay=5"),
    "/s429": ("nodelay, statut 429", " burst=10 nodelay"),
}


def configuration():
    zones, blocs = [], []
    for i, (chemin, (_, options)) in enumerate(EMPLACEMENTS.items()):
        zones.append(f"  limit_req_zone $binary_remote_addr zone=z{i}:1m "
                     "rate=10r/s;")
        statut = "    limit_req_status 429;\n" if chemin == "/s429" else ""
        blocs.append(f"    location {chemin} {{\n      limit_req zone=z{i}"
                     f"{options};\n{statut}      empty_gif;\n    }}")
    return ("events {}\nhttp {\n  access_log off;\n" + "\n".join(zones)
            + "\n  server {\n    listen 80;\n" + "\n".join(blocs)
            + "\n  }\n}\n")


def nettoyer():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)
    shutil.rmtree(DOSSIER, ignore_errors=True)


def lancer():
    nettoyer()
    DOSSIER.mkdir(parents=True, exist_ok=True)
    (DOSSIER / "nginx.conf").write_text(configuration())
    subprocess.run(["docker", "run", "-d", "--name", NOM, "-p",
                    f"{PORT}:80", "-v",
                    f"{DOSSIER / 'nginx.conf'}:/etc/nginx/nginx.conf:ro",
                    IMAGE], check=True, capture_output=True)


async def attendre(s):
    for _ in range(100):
        try:
            async with s.get(URL + "/inconnu") as r:
                await r.read()
                return
        except aiohttp.ClientError:
            await asyncio.sleep(0.1)
    raise RuntimeError("nginx ne répond pas")


async def requete(s, chemin):
    debut = time.monotonic()
    async with s.get(URL + chemin) as r:
        await r.read()
        return r.status, time.monotonic() - debut


async def simultanees(s, chemin):
    res = await asyncio.gather(*(requete(s, chemin) for _ in range(30)))
    codes = {}
    for c, _ in res:
        codes[c] = codes.get(c, 0) + 1
    acc = sorted(d * 1000 for c, d in res if c == 200)
    return codes, acc


async def regulier(s, chemin, debit=25, duree=10):
    debut, taches = time.monotonic(), []
    for k in range(debit * duree):
        attente = debut + k / debit - time.monotonic()
        if attente > 0:
            await asyncio.sleep(attente)
        taches.append(asyncio.create_task(requete(s, chemin)))
    res = await asyncio.gather(*taches)
    acc = [d for c, d in res if c == 200]
    return len(acc) / duree, max(acc) * 1000


async def main():
    lancer()
    try:
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=0)) as s:
            await attendre(s)
            version = subprocess.run(["docker", "exec", NOM, "nginx", "-v"],
                                     capture_output=True, text=True).stderr
            print(version.strip().replace("nginx version: ", ""),
                  "; limite 10r/s par emplacement")
            print("\n1. 30 requêtes simultanées")
            print(f"   {'emplacement':20} {'codes':16} {'attente des acceptées'}")
            for chemin, (nom, _) in EMPLACEMENTS.items():
                codes, acc = await simultanees(s, chemin)
                txt = " ".join(f"{n}×{c}" for c, n in sorted(codes.items()))
                print(f"   {nom:20} {txt:16} {acc[0]:4.0f} à {acc[-1]:4.0f} ms,"
                      f" méd. {statistics.median(acc):4.0f}")
            await asyncio.sleep(3)          # les zones se vident
            print("\n2. client régulier à 25 requêtes par seconde, 10 s")
            for chemin, (nom, _) in EMPLACEMENTS.items():
                if chemin == "/s429":
                    continue
                par_s, pire = await regulier(s, chemin)
                print(f"   {nom:20} {par_s:5.1f} acceptées/s, attente max "
                      f"{pire:5.0f} ms")
    finally:
        nettoyer()


if __name__ == "__main__":
    asyncio.run(main())
