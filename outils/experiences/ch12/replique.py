"""Chapitre 12 : ce qu'un client voit quand il lit une réplique asynchrone.

Un Redis primaire et deux répliques, dans trois conteneurs en réseau hôte.
Chaque réplique se connecte au primaire à travers un relais (ce script) qui
retarde chaque octet d'un délai réglable dans chaque sens, pour imiter une
liaison lointaine. Quatre mesures :
  1. lectures périmées sur la réplique selon le temps écoulé depuis l'écriture ;
  2. coût d'une écriture suivie de WAIT 1 (attendre l'accusé d'une réplique) ;
  3. lectures monotones : un client qui lit tantôt une réplique, tantôt l'autre ;
  4. partition : la liaison est coupée, que font le primaire et la réplique ?
Prérequis : docker, image redis:8.8-alpine ; conteneurs « cours-sd-ch12-* »
supprimés par leur nom à la fin.
Usage : python3 replique.py   (pip install -r requirements.txt)
"""
import asyncio
import random
import statistics
import subprocess
import time

import redis

IMAGE = "redis:8.8-alpine"
PRIMAIRE = 6401
REPLIQUES = {"a": (6402, 6410), "b": (6403, 6411)}  # nom : (port Redis, port du relais)
ESSAIS = 200


# ---- relais à délai ------------------------------------------------------------
class Relais:
    def __init__(self, port_ecoute, port_cible):
        self.port_ecoute, self.port_cible = port_ecoute, port_cible
        self.delai = 0.0  # secondes, dans chaque sens
        self.ouvert = asyncio.Event()  # liaison coupée : les octets attendent, comme TCP qui réémet
        self.ouvert.set()

    async def demarrer(self):
        self.serveur = await asyncio.start_server(self.accueillir, "127.0.0.1", self.port_ecoute)

    async def accueillir(self, lecteur, ecrivain):
        try:
            l2, e2 = await asyncio.open_connection("127.0.0.1", self.port_cible)
        except OSError:
            ecrivain.close()
            return
        await asyncio.gather(self.transmettre(lecteur, e2), self.transmettre(l2, ecrivain),
                             return_exceptions=True)
        for e in (ecrivain, e2):
            e.close()

    async def transmettre(self, lecteur, ecrivain):
        file = asyncio.Queue()

        async def livrer():
            while True:
                echeance, donnees = await file.get()
                if donnees is None:
                    return
                attente = echeance - time.monotonic()
                if attente > 0:
                    await asyncio.sleep(attente)
                await self.ouvert.wait()
                ecrivain.write(donnees)
                await ecrivain.drain()

        livreur = asyncio.create_task(livrer())
        try:
            while True:
                donnees = await lecteur.read(65536)
                if not donnees:
                    break
                file.put_nowait((time.monotonic() + self.delai, donnees))
        finally:
            file.put_nowait((0, None))
            await livreur
            ecrivain.close()

    def couper(self):
        self.ouvert.clear()

    def retablir(self):
        self.ouvert.set()


# ---- conteneurs -----------------------------------------------------------------
def docker(*args):
    subprocess.run(["docker", *args], check=True, stdout=subprocess.DEVNULL)


def lancer_conteneurs():
    base = ["run", "-d", "--network", "host"]
    docker(*base, "--name", "cours-sd-ch12-primaire", IMAGE,
           "redis-server", "--port", str(PRIMAIRE), "--save", "", "--appendonly", "no")
    for nom, (port, relais) in REPLIQUES.items():
        docker(*base, "--name", f"cours-sd-ch12-replique-{nom}", IMAGE,
               "redis-server", "--port", str(port), "--save", "", "--appendonly", "no",
               "--replicaof", "127.0.0.1", str(relais))


def supprimer_conteneurs():
    for nom in ["primaire", *(f"replique-{n}" for n in REPLIQUES)]:
        subprocess.run(["docker", "rm", "-f", f"cours-sd-ch12-{nom}"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


async def attendre_lien(client, etat="up", delai_max=30):
    fin = time.monotonic() + delai_max
    while time.monotonic() < fin:
        try:
            info = await asyncio.to_thread(client.info, "replication")
            if info.get("master_link_status") == etat:
                return
        except redis.ConnectionError:
            pass
        await asyncio.sleep(0.1)
    raise RuntimeError(f"lien de réplication pas {etat}")


# ---- mesures ----------------------------------------------------------------------
async def perimees(p, r, attente_ms):
    """Part des lectures sur la réplique qui renvoient l'ancienne valeur."""
    vieilles = 0
    for i in range(ESSAIS):
        valeur = f"v{i}-{random.random()}"
        await asyncio.to_thread(p.set, "profil:42", valeur)
        await asyncio.sleep(attente_ms / 1000)
        if await asyncio.to_thread(r.get, "profil:42") != valeur:
            vieilles += 1
        await asyncio.sleep(0.005)
    return vieilles


async def ecriture(p, avec_wait):
    for i in range(50):  # échauffement
        await asyncio.to_thread(p.set, "compteur", i)
    durees = []
    for i in range(ESSAIS):
        t0 = time.perf_counter()
        await asyncio.to_thread(p.set, "compteur", i)
        if avec_wait:
            n = await asyncio.to_thread(p.wait, 1, 1000)
            assert n >= 1
        durees.append((time.perf_counter() - t0) * 1000)
    durees.sort()
    return statistics.median(durees), durees[int(0.99 * len(durees)) - 1]


async def monotones(p, ra, rb):
    """Après chaque écriture, deux lectures à 15 ms d'écart, chacune sur une réplique tirée au hasard."""
    retours = 0
    for i in range(ESSAIS):
        valeur = f"v{i}"
        await asyncio.to_thread(p.set, "profil:7", valeur)
        await asyncio.sleep(0.010)
        premiere, seconde = random.choice((ra, rb)), random.choice((ra, rb))
        l1 = await asyncio.to_thread(premiere.get, "profil:7")
        await asyncio.sleep(0.015)
        l2 = await asyncio.to_thread(seconde.get, "profil:7")
        if l1 == valeur and l2 != valeur:
            retours += 1
        await asyncio.sleep(0.005)
    return retours


async def main():
    supprimer_conteneurs()
    relais = {n: Relais(rp, PRIMAIRE) for n, (_, rp) in REPLIQUES.items()}
    for x in relais.values():
        await x.demarrer()
    await asyncio.to_thread(lancer_conteneurs)
    try:
        p = redis.Redis(port=PRIMAIRE, decode_responses=True)
        ra = redis.Redis(port=REPLIQUES["a"][0], decode_responses=True)
        rb = redis.Redis(port=REPLIQUES["b"][0], decode_responses=True)
        for r in (ra, rb):
            await attendre_lien(r)

        print(f"1. lectures périmées sur la réplique a, sur {ESSAIS} essais")
        print("délai de la liaison   lecture après   lectures périmées")
        for delai in (0, 25):
            relais["a"].delai = delai / 1000
            for attente in ((0, 1, 5) if delai == 0 else (0, 10, 20, 30, 50)):
                n = await perimees(p, ra, attente)
                print(f"{delai:>11} ms {attente:>13} ms {n:>11} ({100 * n / ESSAIS:.1f} %)")

        print(f"\n2. durée d'une écriture, {ESSAIS} écritures")
        print("délai de la liaison   écriture    médiane     99e centile")
        for delai in (0, 25):
            relais["a"].delai = relais["b"].delai = delai / 1000
            for avec in (False, True):
                med, p99 = await ecriture(p, avec)
                nom = "SET + WAIT" if avec else "SET seul"
                print(f"{delai:>11} ms   {nom:<10} {med:>7.2f} ms {p99:>10.2f} ms")

        print(f"\n3. lectures monotones, {ESSAIS} essais, répliques à 5 ms et 40 ms")
        relais["a"].delai, relais["b"].delai = 0.005, 0.040
        await asyncio.sleep(0.2)
        print(f"retours en arrière : {await monotones(p, ra, rb)}")

        print("\n4. partition : plus rien ne passe entre le primaire et les deux répliques")
        relais["a"].delai = relais["b"].delai = 0
        await asyncio.to_thread(p.set, "profil:42", "avant")
        await asyncio.sleep(0.5)
        t0 = time.monotonic()
        for x in relais.values():
            x.couper()
        await asyncio.to_thread(p.set, "profil:42", "pendant")
        lu = await asyncio.to_thread(ra.get, "profil:42")
        lien = (await asyncio.to_thread(ra.info, "replication"))["master_link_status"]
        print(f"réglage par défaut : écriture acceptée ; la réplique répond « {lu} »,")
        print(f"  et se croit toujours reliée (master_link_status:{lien})")
        await asyncio.to_thread(p.config_set, "min-replicas-to-write", 1)
        await asyncio.to_thread(p.config_set, "min-replicas-max-lag", 10)
        refus, motif = None, ""
        while time.monotonic() - t0 < 30:
            try:
                await asyncio.to_thread(p.set, "profil:42", "pendant")
            except redis.ResponseError as e:
                refus, motif = time.monotonic() - t0, str(e).split()[0]
                break
            await asyncio.sleep(0.1)
        print(f"min-replicas-to-write 1, max-lag 10 : écritures refusées ({motif})")
        print(f"  {refus:.1f} s après la coupure")
        await asyncio.to_thread(p.config_set, "min-replicas-to-write", 0)
        await asyncio.to_thread(p.set, "profil:42", "apres")
        t1 = time.monotonic()
        for x in relais.values():
            x.retablir()
        while await asyncio.to_thread(ra.get, "profil:42") != "apres":
            await asyncio.sleep(0.01)
        print(f"liaison rétablie : la réplique a la dernière valeur {1000 * (time.monotonic() - t1):.0f} ms après")
    finally:
        await asyncio.to_thread(supprimer_conteneurs)


if __name__ == "__main__":
    asyncio.run(main())
