"""Chapitre 13 : retirer un serveur de la rotation sans perdre de requêtes.

Deux serveurs serveur.py (64 fils, 300 ms par requête) derrière Caddy (tourniquet,
puis moins de connexions), avec un
contrôle de santé actif sur /sante toutes les 500 ms et aucune nouvelle tentative.
Vingt clients enchaînent des requêtes pendant 10 s ; à 3 s, le second serveur est
arrêté de l'une de trois façons :
  1. SIGKILL : arrêt brutal, comme une panne ;
  2. SIGTERM, arrêt immédiat : il ferme son port, finit ses requêtes en cours,
     s'arrête, sans rien dire au répartiteur ;
  3. SIGTERM, drainage : /sante répond 503, il sert tout ce qui arrive encore,
     et ne s'arrête qu'après 1 s sans requête.
Prérequis : caddy. Usage : python3 drainage.py
"""
import asyncio
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

import aiohttp

ICI = pathlib.Path(__file__).parent
PORTS = (9321, 9322)
CADDY = """{{
	admin off
	auto_https off
}}
:9320 {{
	reverse_proxy 127.0.0.1:9321 127.0.0.1:9322 {{
		lb_policy {politique}
		health_uri /sante
		health_interval 500ms
		health_timeout 1s
	}}
}}
"""


def attendre(url, delai=10):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            urllib.request.urlopen(url, timeout=0.5)
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"{url} ne répond pas")


async def charge(duree, clients=20):
    resultats = {"ok": 0, "erreurs": 0, "detail": []}
    t0 = time.monotonic()
    fin = t0 + duree
    async with aiohttp.ClientSession() as s:
        async def boucle():
            while time.monotonic() < fin:
                try:
                    async with s.get("http://127.0.0.1:9320/travail") as r:
                        await r.read()
                        if r.status == 200:
                            resultats["ok"] += 1
                        else:
                            resultats["erreurs"] += 1
                            resultats[r.status] = resultats.get(r.status, 0) + 1
                            resultats["detail"].append((time.monotonic() - t0, r.status))
                except aiohttp.ClientError as e:
                    resultats["erreurs"] += 1
                    resultats["detail"].append((time.monotonic() - t0, type(e).__name__))
        await asyncio.gather(*(boucle() for _ in range(clients)))
    return resultats


async def essai(politique, mode):
    env = dict(os.environ, MODE_ARRET="immediat" if mode == "immediat" else "drainage")
    serveurs = [subprocess.Popen([sys.executable, str(ICI / "serveur.py"), str(p), "64", "300"], env=env)
                for p in PORTS]
    for p in PORTS:
        attendre(f"http://127.0.0.1:{p}/sante")
    f = tempfile.NamedTemporaryFile("w", suffix=".caddyfile", delete=False)
    f.write(CADDY.format(politique=politique))
    f.close()
    caddy = subprocess.Popen(["caddy", "run", "--adapter", "caddyfile", "--config", f.name],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    attendre("http://127.0.0.1:9320/sante")
    await asyncio.sleep(1)
    tache = asyncio.create_task(charge(10))
    await asyncio.sleep(3)
    victime = serveurs[1]
    t0 = time.monotonic()
    victime.send_signal(signal.SIGKILL if mode == "kill" else signal.SIGTERM)
    while victime.poll() is None:
        await asyncio.sleep(0.01)
    arret = time.monotonic() - t0
    r = await tache
    caddy.terminate()
    serveurs[0].terminate()
    for p in (caddy, serveurs[0]):
        p.wait()
    os.unlink(f.name)
    return r, arret


async def main():
    print("deux serveurs, 300 ms par requête, 20 clients pendant 10 s ; arrêt du second à 3 s")
    print(f"{'politique':<12}{'arrêt du serveur':<26}{'requêtes':>9}{'502':>7}{'503':>6}{'arrêt en':>11}")
    for politique in ("round_robin", "least_conn"):
        for mode, nom in (("kill", "SIGKILL"), ("immediat", "SIGTERM, arrêt immédiat"),
                          ("drainage", "SIGTERM, drainage")):
            r, arret = await essai(politique, mode)
            total = r["ok"] + r["erreurs"]
            print(f"{politique:<12}{nom:<26}{total:>9}{r.get(502, 0):>7}{r.get(503, 0):>6}"
                  f"{1000 * arret:>8.0f} ms")
        if os.environ.get("DETAIL"):
            from collections import Counter
            print("   ", sorted(Counter((round(t, 1), e) for t, e in r["detail"]).items()))


if __name__ == "__main__":
    asyncio.run(main())
