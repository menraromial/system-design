"""Chapitre 42 : la même commande, en monolithe et en microservices.

Une commande de 10 articles passe par cinq domaines : utilisateurs (vérifier
le client), catalogue (prix des articles), stock (réserver), paiement (taxes et
total) et commandes (enregistrer). Chaque domaine fait un petit travail de
processeur (environ 20 µs). Cinq façons de l'assembler :

  monolithe     un processus, appels de fonctions ;
  modulaire     un processus, chaque domaine derrière une interface qui copie
                ses entrées et ses sorties dans des objets à lui ;
  bavard        une passerelle et cinq services HTTP/JSON (aiohttp), un appel
                par article pour le catalogue et le stock (23 appels) ;
  groupé        les mêmes services, un appel par domaine (5 appels), le client
                et le catalogue interrogés en parallèle ;
  groupé, 1 ms  comme « groupé », chaque appel entre services traversant un
                relais qui retarde chaque sens d'au moins 0,5 ms (machines
                différentes) ; le script mesure le délai réellement ajouté.

On mesure la latence d'une commande seule (2 000 commandes l'une après
l'autre), le débit à 50 commandes simultanées pendant 10 s, le temps de
processeur de tous les processus par commande, puis la part de commandes
réussies quand chaque unité déployée (le monolithe, ou chaque service) échoue
une fois sur mille par appel reçu.

    python3 appels.py
"""
import argparse
import asyncio
import json
import random
import resource
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from aiohttp import web

ICI = Path(__file__)
PASSERELLE = 12100
SERVICES = {"utilisateurs": 12101, "catalogue": 12102, "stock": 12103, "paiement": 12104, "commandes": 12105}
RELAIS = {n: p + 10 for n, p in SERVICES.items()}           # 12111 à 12115


# ---- les domaines (le même code partout) ---------------------------------------
def travail(n=300):
    x = 0
    for i in range(n):                                       # environ 20 µs
        x = (x * 31 + i) % 1_000_003
    return x


UTILISATEURS = {i: {"id": i, "actif": True, "pays": "FR"} for i in range(10_000)}
PRIX = {i: 100 + i % 900 for i in range(1_000)}
STOCK = {i: 10**9 for i in range(1_000)}
COMMANDES = {}


def verifier_client(uid):
    travail()
    return UTILISATEURS[uid]["actif"]


def prix(articles):
    travail()
    return {a: PRIX[a] for a in articles}


def reserver(articles):
    travail()
    for a in articles:
        STOCK[a] -= 1
    return True


def payer(prix_articles):
    travail()
    total = sum(prix_articles.values())
    return round(total * 1.2)


def enregistrer(uid, articles, total):
    travail()
    cid = len(COMMANDES) + 1
    COMMANDES[cid] = (uid, tuple(articles), total)
    return cid


def commande_monolithe(uid, articles):
    if not verifier_client(uid):
        raise ValueError("client inactif")
    p = prix(articles)
    reserver(articles)
    total = payer(p)
    return enregistrer(uid, articles, total)


# ---- le monolithe modulaire : interfaces et objets propres à chaque module ------
@dataclass(frozen=True)
class DemandePrix:
    articles: tuple


@dataclass(frozen=True)
class Tarif:
    prix: dict


class ModuleCatalogue:
    def tarifer(self, d: DemandePrix) -> Tarif:
        return Tarif(dict(prix(list(d.articles))))


class ModuleStock:
    def reserver(self, articles: tuple) -> bool:
        return reserver(list(articles))


class ModuleClients:
    def actif(self, uid: int) -> bool:
        return verifier_client(uid)


class ModulePaiement:
    def total(self, t: Tarif) -> int:
        return payer(dict(t.prix))


class ModuleCommandes:
    def creer(self, uid: int, articles: tuple, total: int) -> int:
        return enregistrer(uid, list(articles), total)


MODULES = (ModuleClients(), ModuleCatalogue(), ModuleStock(), ModulePaiement(), ModuleCommandes())


def commande_modulaire(uid, articles):
    clients, catalogue, stock, paiement, commandes = MODULES
    a = tuple(articles)
    if not clients.actif(uid):
        raise ValueError("client inactif")
    t = catalogue.tarifer(DemandePrix(a))
    stock.reserver(a)
    return commandes.creer(uid, a, paiement.total(t))


# ---- les services HTTP ------------------------------------------------------------
def service(nom, taux_echec):
    alea = random.Random(nom)

    async def appel(requete):
        if alea.random() < taux_echec:
            return web.Response(status=500)
        c = await requete.json()
        if nom == "utilisateurs":
            r = verifier_client(c["uid"])
        elif nom == "catalogue":
            r = prix(c["articles"])
        elif nom == "stock":
            r = reserver(c["articles"])
        elif nom == "paiement":
            r = payer({int(k): v for k, v in c["prix"].items()})
        else:
            r = enregistrer(c["uid"], c["articles"], c["total"])
        return web.json_response({"r": r})

    async def cpu(requete):
        u = resource.getrusage(resource.RUSAGE_SELF)
        return web.json_response(u.ru_utime + u.ru_stime)

    app = web.Application()
    app.add_routes([web.post("/", appel), web.get("/cpu", cpu)])
    web.run_app(app, host="127.0.0.1", port=SERVICES[nom], print=None, access_log=None)


def passerelle(mode, taux_echec):
    session = {}
    alea = random.Random(0)

    def url(nom):
        return f"http://127.0.0.1:{(RELAIS if mode == 'groupe-1ms' else SERVICES)[nom]}/"

    async def post(nom, corps):
        async with session["s"].post(url(nom), json=corps) as r:
            if r.status != 200:
                raise RuntimeError(nom)
            return (await r.json())["r"]

    async def commande(requete):
        c = await requete.json()
        uid, articles = c["uid"], c["articles"]
        try:
            if mode == "monolithe":
                if alea.random() < taux_echec:
                    raise RuntimeError("panne")
                cid = commande_monolithe(uid, articles)
            elif mode == "modulaire":
                if alea.random() < taux_echec:
                    raise RuntimeError("panne")
                cid = commande_modulaire(uid, articles)
            elif mode == "bavard":
                await post("utilisateurs", {"uid": uid})
                p = {}
                for a in articles:
                    p.update(await post("catalogue", {"articles": [a]}))
                for a in articles:
                    await post("stock", {"articles": [a]})
                total = await post("paiement", {"prix": p})
                cid = await post("commandes", {"uid": uid, "articles": articles, "total": total})
            else:
                _, p = await asyncio.gather(post("utilisateurs", {"uid": uid}),
                                            post("catalogue", {"articles": articles}))
                await post("stock", {"articles": articles})
                total = await post("paiement", {"prix": p})
                cid = await post("commandes", {"uid": uid, "articles": articles, "total": total})
        except (RuntimeError, ValueError):
            return web.Response(status=503)
        return web.json_response({"commande": cid})

    async def cpu(requete):
        u = resource.getrusage(resource.RUSAGE_SELF)
        return web.json_response(u.ru_utime + u.ru_stime)

    async def ouvrir(app):
        session["s"] = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))
        if mode == "groupe-1ms":
            for nom in SERVICES:
                await asyncio.start_server(relais(SERVICES[nom]), "127.0.0.1", RELAIS[nom])

    app = web.Application()
    app.on_startup.append(ouvrir)
    app.add_routes([web.post("/commande", commande), web.get("/cpu", cpu)])
    web.run_app(app, host="127.0.0.1", port=PASSERELLE, print=None, access_log=None)


def relais(cible, delai=0.0005):
    """Relais TCP qui retarde chaque morceau de `delai` secondes dans chaque sens."""
    async def accueillir(lecteur, ecrivain):
        l2, e2 = await asyncio.open_connection("127.0.0.1", cible)
        boucle = asyncio.get_running_loop()

        async def copier(src, dst):
            try:
                while data := await src.read(65536):
                    boucle.call_later(delai, dst.write, data)
            except ConnectionError:
                pass
            finally:
                boucle.call_later(delai, dst.close)
        await asyncio.gather(copier(lecteur, e2), copier(l2, ecrivain))
    return accueillir


# ---- le client ------------------------------------------------------------------
async def attendre_port(port):
    for _ in range(200):
        try:
            _, e = await asyncio.open_connection("127.0.0.1", port)
            e.close()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise RuntimeError(f"port {port} fermé")


async def cpu_total(s, ports):
    total = 0.0
    for p in ports:
        async with s.get(f"http://127.0.0.1:{p}/cpu") as r:
            total += await r.json()
    return total


async def essai(mode, taux_echec=0.0, n_seq=2000, duree=10):
    procs = []
    distribue = mode not in ("monolithe", "modulaire")
    if distribue:
        for nom in SERVICES:
            procs.append(subprocess.Popen([sys.executable, str(ICI), "--service", nom, "--echec", str(taux_echec)]))
    procs.append(subprocess.Popen([sys.executable, str(ICI), "--passerelle", mode, "--echec", str(taux_echec)]))
    ports = [PASSERELLE] + (list(SERVICES.values()) if distribue else [])
    for p in ports:
        await attendre_port(p)
    alea = random.Random(42)
    corps = lambda: {"uid": alea.randrange(10_000), "articles": alea.sample(range(1_000), 10)}
    url = f"http://127.0.0.1:{PASSERELLE}/commande"
    res = {}
    try:
        async with aiohttp.ClientSession() as s:
            for _ in range(200):                                     # mise en route
                async with s.post(url, json=corps()) as r:
                    await r.read()
            if taux_echec:
                ok = 0
                for _ in range(n_seq):
                    async with s.post(url, json=corps()) as r:
                        await r.read()
                        ok += r.status == 200
                res["reussite"] = ok / n_seq
                return res
            durees = []
            for _ in range(n_seq):
                t = time.perf_counter()
                async with s.post(url, json=corps()) as r:
                    await r.read()
                durees.append((time.perf_counter() - t) * 1000)
            durees.sort()
            res["med"], res["p99"] = statistics.median(durees), durees[int(0.99 * len(durees))]
            cpu0 = await cpu_total(s, ports)
            fait = 0
            fin = time.monotonic() + duree

            async def ouvrier():
                nonlocal fait
                while time.monotonic() < fin:
                    async with s.post(url, json=corps()) as r:
                        await r.read()
                    fait += 1
            await asyncio.gather(*(ouvrier() for _ in range(50)))
            res["debit"] = fait / duree
            res["cpu"] = (await cpu_total(s, ports) - cpu0) / fait * 1e6
            if mode == "groupe-1ms":                             # aller-retour ajouté par le relais
                ecarts = []
                for port in (SERVICES["utilisateurs"], RELAIS["utilisateurs"]) * 300:
                    t = time.perf_counter()
                    async with s.post(f"http://127.0.0.1:{port}/", json={"uid": 1}) as r:
                        await r.read()
                    ecarts.append((port, (time.perf_counter() - t) * 1000))
                direct = statistics.median(d for p_, d in ecarts if p_ == SERVICES["utilisateurs"])
                relaye = statistics.median(d for p_, d in ecarts if p_ == RELAIS["utilisateurs"])
                res["relais"] = (direct, relaye)
    finally:
        for p in procs:
            p.kill()
            p.wait()
    return res


async def client():
    modes = ("monolithe", "modulaire", "bavard", "groupe", "groupe-1ms")
    print("commande de 10 articles, 5 domaines ; latence sur 2 000 commandes successives,")
    print("débit à 50 commandes simultanées pendant 10 s, processeur de tous les processus")
    print(f"\n{'mode':<14}{'appels':>7}{'méd. (ms)':>11}{'99e (ms)':>10}{'débit (/s)':>12}{'CPU/cmd (µs)':>14}")
    appels = {"monolithe": 0, "modulaire": 0, "bavard": 23, "groupe": 5, "groupe-1ms": 5}
    for m in modes:
        r = await essai(m)
        print(f"{m:<14}{appels[m]:>7}{r['med']:>11.3f}{r['p99']:>10.3f}{r['debit']:>12.0f}{r['cpu']:>14.0f}")
        if "relais" in r:
            relais_mesure = r["relais"]
    d, rl = relais_mesure
    print(f"(un appel direct à un service : {d:.2f} ms ; à travers le relais : {rl:.2f} ms)")
    print("\nchaque unité déployée échoue une fois sur mille par appel reçu ; 20 000 commandes")
    for m in ("monolithe", "bavard", "groupe"):
        r = await essai(m, taux_echec=0.001, n_seq=20_000)
        print(f"   {m:<12} {100 * r['reussite']:.2f} % de commandes réussies")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--service")
    p.add_argument("--passerelle")
    p.add_argument("--echec", type=float, default=0.0)
    a = p.parse_args()
    if a.service:
        service(a.service, a.echec)
    elif a.passerelle:
        passerelle(a.passerelle, a.echec)
    else:
        asyncio.run(client())


if __name__ == "__main__":
    main()
