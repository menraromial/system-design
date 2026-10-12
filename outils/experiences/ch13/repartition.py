"""Chapitre 13 : comparer des algorithmes de répartition sur un parc hétérogène.

Quatre serveurs serveur.py derrière Caddy : trois rapides (4 fils, 10 ms par
requête, soit 400 req/s chacun) et un lent (4 fils, 40 ms, soit 100 req/s).
Capacité totale : 1 300 req/s. Trois essais :
  1. cinq politiques de Caddy sous la charge de wrk (64 connexions, 10 s) ;
  2. hachage d'URI : quelle part de 1 000 URI change de serveur quand on passe
     de 4 à 5 serveurs, avec Caddy (rendez-vous) et avec un modulo ;
  3. répartition par connexion (relais L4 écrit ici) ou par requête (Caddy, L7),
     pour un client qui garde une seule connexion ouverte, puis seize.
Prérequis : caddy, wrk. Usage : python3 repartition.py
"""
import asyncio
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

import aiohttp

ICI = pathlib.Path(__file__).parent
RAPIDE, LENT = (4, 10), (4, 40)
PARC = {9301: RAPIDE, 9302: RAPIDE, 9303: RAPIDE, 9304: LENT}
CADDY = """{{
	admin off
	auto_https off
}}
:9300 {{
	reverse_proxy {amonts} {{
		lb_policy {politique}
	}}
}}
"""


def lancer_serveurs(parc):
    procs = [subprocess.Popen([sys.executable, str(ICI / "serveur.py"), str(p), str(k), str(s)])
             for p, (k, s) in parc.items()]
    for p in parc:
        attendre(f"http://127.0.0.1:{p}/sante")
    return procs


def attendre(url, delai=10):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            urllib.request.urlopen(url, timeout=0.5)
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"{url} ne répond pas")


def lancer_caddy(ports, politique):
    f = tempfile.NamedTemporaryFile("w", suffix=".caddyfile", delete=False)
    f.write(CADDY.format(amonts=" ".join(f"127.0.0.1:{p}" for p in ports), politique=politique))
    f.close()
    proc = subprocess.Popen(["caddy", "run", "--adapter", "caddyfile", "--config", f.name],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    attendre("http://127.0.0.1:9300/sante")
    return proc


def arreter(procs):
    for p in procs:
        p.terminate()
    for p in procs:
        p.wait()


def stats(ports):
    return {p: json.load(urllib.request.urlopen(f"http://127.0.0.1:{p}/stats"))["servies"] for p in ports}


def raz(ports):
    for p in ports:
        urllib.request.urlopen(f"http://127.0.0.1:{p}/raz")


def wrk(url):
    sortie = subprocess.run(["wrk", "-t4", "-c64", "-d10s", "--latency", url],
                            capture_output=True, text=True).stdout
    debit = float(re.search(r"Requests/sec:\s+([\d.]+)", sortie).group(1))

    def centile(c):
        v, u = re.search(rf"^\s+{c}%\s+([\d.]+)(us|ms|s)", sortie, re.M).groups()
        return float(v) * {"us": 0.001, "ms": 1, "s": 1000}[u]
    return debit, centile(50), centile(99)


def essai_politiques():
    print("1. politiques de Caddy ; 3 serveurs à 400 req/s, 1 à 100 req/s (le dernier)")
    print(f"{'politique':<26}{'req/s':>7}{'médiane':>10}{'99e cent.':>11}   part de chaque serveur")
    for politique in ("round_robin", "random", "least_conn", "random_choose 2",
                      "weighted_round_robin 4 4 4 1"):
        caddy = lancer_caddy(PARC, politique)
        raz(PARC)
        debit, p50, p99 = wrk("http://127.0.0.1:9300/travail")
        s = stats(PARC)
        total = sum(s.values())
        parts = " ".join(f"{100 * v / total:3.0f}" for v in s.values())
        print(f"{politique:<26}{debit:>7.0f}{p50:>8.1f} ms{p99:>8.1f} ms   {parts} %")
        arreter([caddy])


async def destinations(url, n=1000):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=32)) as s:
        async def une(i):
            async with s.get(f"{url}/objet/k{i}") as r:
                return await r.text()
        return await asyncio.gather(*(une(i) for i in range(n)))


def essai_hachage():
    print("\n2. hachage de 1 000 URI, de 4 à 5 serveurs")
    parc5 = {9301: RAPIDE, 9302: RAPIDE, 9303: RAPIDE, 9304: RAPIDE, 9305: RAPIDE}
    procs = lancer_serveurs({9305: RAPIDE})
    caddy = lancer_caddy(list(parc5)[:4], "uri_hash")
    avant = asyncio.run(destinations("http://127.0.0.1:9300"))
    arreter([caddy])
    caddy = lancer_caddy(parc5, "uri_hash")
    apres = asyncio.run(destinations("http://127.0.0.1:9300"))
    arreter([caddy, *procs])
    bouge = sum(a != b for a, b in zip(avant, apres))
    vers_nouveau = sum(b == "9305" for b in apres)
    h = [int(hashlib.sha1(f"/objet/k{i}".encode()).hexdigest(), 16) for i in range(1000)]
    modulo = sum(x % 4 != x % 5 for x in h)
    print(f"Caddy uri_hash (rendez-vous) : {bouge / 10:.1f} % des URI changent de serveur,")
    print(f"  dont {vers_nouveau / 10:.1f} % vers le nouveau")
    print(f"modulo (SHA-1 mod n)         : {modulo / 10:.1f} % des URI changent de serveur")


class RelaisL4:
    """Répartiteur de niveau 4 minimal : chaque connexion TCP va, en tourniquet,
    vers le serveur suivant ; les octets sont recopiés sans être lus."""
    def __init__(self, ports):
        self.ports, self.i = list(ports), 0

    async def accueillir(self, l1, e1):
        port = self.ports[self.i % len(self.ports)]
        self.i += 1
        l2, e2 = await asyncio.open_connection("127.0.0.1", port)

        async def copier(l, e):
            try:
                while d := await l.read(65536):
                    e.write(d)
                    await e.drain()
            finally:
                e.close()
        await asyncio.gather(copier(l1, e2), copier(l2, e1), return_exceptions=True)


async def client(url, connexions, requetes):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=connexions)) as s:
        async def boucle():
            for _ in range(requetes // connexions):
                async with s.get(url) as r:
                    await r.read()
        await asyncio.gather(*(boucle() for _ in range(connexions)))


async def essai_l4_l7_async(ports):
    relais = RelaisL4(ports)
    serveur = await asyncio.start_server(relais.accueillir, "127.0.0.1", 9310)
    resultats = []
    for nom, url in (("L4, par connexion", "http://127.0.0.1:9310/travail"),
                     ("L7, par requête  ", "http://127.0.0.1:9300/travail")):
        for connexions in (1, 16):
            await asyncio.to_thread(raz, ports)
            await client(url, connexions, 400)
            s = await asyncio.to_thread(stats, ports)
            resultats.append((nom, connexions, s))
    serveur.close()
    return resultats


def essai_l4_l7():
    print("\n3. 400 requêtes, 4 serveurs identiques ; requêtes reçues par chaque serveur")
    ports = [9301, 9302, 9303, 9305]
    procs = lancer_serveurs({9305: RAPIDE})
    caddy = lancer_caddy(ports, "round_robin")
    for nom, connexions, s in asyncio.run(essai_l4_l7_async(ports)):
        detail = " ".join(f"{v:>4}" for v in s.values())
        print(f"{nom}  {connexions:>2} connexion{'s' if connexions > 1 else ' '} :  {detail}")
    arreter([caddy, *procs])


def main():
    procs = lancer_serveurs(PARC)
    try:
        essai_politiques()
        essai_hachage()
        essai_l4_l7()
    finally:
        arreter(procs)


if __name__ == "__main__":
    main()
