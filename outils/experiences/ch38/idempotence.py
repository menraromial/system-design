"""Chapitre 38 : retries d'un paiement, avec et sans clé d'idempotence.

PostgreSQL 18 dans le conteneur cours-sd-ch38-pg (port 11832). Le service de
paiement (paiement.py, port 11830) est derrière un relais TCP (port 11831) qui
perd la réponse d'une requête sur 20 : le paiement est fait, mais le client ne
le sait pas. Le client fait 2 000 paiements, 20 à la fois, avec un délai
d'attente de 1 s et jusqu'à 6 tentatives (attente exponentielle avec gigue
complète, base 100 ms, plafond 1 s). On compte ensuite, dans la base, les
paiements faits deux fois.

1. quatre façons de faire : sans retry ; retry sans clé ; clé vérifiée, puis
   enregistrée après le paiement ; clé insérée dans la transaction du paiement.
2. coût de la clé : 2 000 paiements l'un après l'autre, sans relais ni pause.
3. même clé, autre corps.

    python3 idempotence.py
"""
import asyncio
import json
import random
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

import aiohttp
import psycopg

ICI = Path(__file__).parent
NOM, PG, SERVICE, RELAIS = "cours-sd-ch38-pg", 11832, 11830, 11831
DSN = f"host=127.0.0.1 port={PG} dbname=postgres user=postgres"
N, PARALLELE, DELAI, TENTATIVES, PERTE = 2000, 20, 1.0, 6, 0.05


def demarrer_pg():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NOM, "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                    "-p", f"127.0.0.1:{PG}:5432", "postgres:18-alpine",
                    "-c", "max_connections=200"], check=True, capture_output=True)
    for _ in range(120):
        try:
            with psycopg.connect(DSN) as c:
                c.execute("select 1")
                return
        except psycopg.OperationalError:
            time.sleep(0.5)
    raise RuntimeError("PostgreSQL absent")


def preparer():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("drop table if exists comptes, paiements, cles")
        c.execute("create table comptes (id int primary key, solde bigint not null)")
        c.execute("insert into comptes select g, 1000000 from generate_series(1, 50) g")
        c.execute("create table paiements (id bigserial primary key, compte int not null,"
                  " montant int not null, ref text not null)")
        c.execute("create table cles (cle text primary key, empreinte text not null,"
                  " code int, corps jsonb, cree timestamptz not null default now())")


def compter():
    with psycopg.connect(DSN) as c:
        lignes, refs = c.execute("select count(*), count(distinct ref) from paiements").fetchone()
        faites = {r for (r,) in c.execute("select distinct ref from paiements")}
        return lignes, refs, faites


class Relais:
    def __init__(self, perte, graine=38):
        self.perte, self.alea, self.perdues = perte, random.Random(graine), 0

    async def demarrer(self):
        self.serveur = await asyncio.start_server(self.accueillir, "127.0.0.1", RELAIS)

    async def accueillir(self, lecteur, ecrivain):
        perdre = self.alea.random() < self.perte
        try:
            l2, e2 = await asyncio.open_connection("127.0.0.1", SERVICE)
        except OSError:
            ecrivain.close()
            return

        async def copier(src, dst, jeter):
            try:
                while data := await src.read(65536):
                    if jeter:
                        self.perdues += 1
                        break
                    dst.write(data)
                    await dst.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                if not jeter:
                    dst.close()
        await asyncio.gather(copier(lecteur, e2, False), copier(l2, ecrivain, perdre),
                             return_exceptions=True)
        e2.close()

    def fermer(self):
        self.serveur.close()


async def lancer_service(mode, rapide=False):
    cmd = [sys.executable, str(ICI / "paiement.py"), "--port", str(SERVICE), "--pg", str(PG), "--mode", mode]
    if rapide:
        cmd.append("--rapide")
    p = subprocess.Popen(cmd)
    for _ in range(200):
        try:
            _, e = await asyncio.open_connection("127.0.0.1", SERVICE)
            e.close()
            return p
        except OSError:
            await asyncio.sleep(0.05)
    raise RuntimeError("service absent")


async def campagne(nom, mode, retry, cle):
    preparer()
    service = await lancer_service(mode)
    relais = Relais(PERTE)
    await relais.demarrer()
    alea = random.Random(7)
    session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0, force_close=True))
    file = asyncio.Queue()
    for i in range(N):
        file.put_nowait(i)
    vus_ok, tentatives, rejoues = set(), 0, 0

    async def payer(i):
        nonlocal tentatives, rejoues
        corps = {"compte": i % 50 + 1, "montant": 1, "ref": f"p{i}"}
        entetes = {"Idempotency-Key": str(uuid.uuid4())} if cle else {}
        for n in range(TENTATIVES if retry else 1):
            if n:
                await asyncio.sleep(alea.uniform(0, min(1.0, 0.1 * 2 ** n)))
            tentatives += 1
            try:
                async with session.post(f"http://127.0.0.1:{RELAIS}/payer", json=corps, headers=entetes,
                                        timeout=aiohttp.ClientTimeout(total=DELAI)) as r:
                    await r.read()
                    if r.status in (200, 201):
                        rejoues += r.headers.get("Idempotent-Replayed") == "true"
                        vus_ok.add(corps["ref"])
                        return
            except (asyncio.TimeoutError, aiohttp.ClientError):
                pass

    async def ouvrier():
        while not file.empty():
            await payer(file.get_nowait())

    await asyncio.gather(*(ouvrier() for _ in range(PARALLELE)))
    await asyncio.sleep(2.5)                     # laisser finir les paiements lents
    await session.close()
    relais.fermer()
    service.terminate()
    service.wait()
    lignes, refs, faites = compter()
    echec_mais_fait = len(faites - vus_ok)
    print(f"   {nom:<26}{tentatives:>6}{relais.perdues:>8}{len(vus_ok):>6}{N - len(vus_ok):>6}"
          f"{echec_mais_fait:>6}{lignes - refs:>9}{rejoues:>9}")


async def latence(mode):
    preparer()
    service = await lancer_service(mode, rapide=True)
    durees = []
    async with aiohttp.ClientSession() as s:
        for i in range(N + 200):
            corps = {"compte": i % 50 + 1, "montant": 1, "ref": f"l{i}"}
            entetes = {"Idempotency-Key": str(uuid.uuid4())}
            t = time.perf_counter()
            async with s.post(f"http://127.0.0.1:{SERVICE}/payer", json=corps, headers=entetes) as r:
                await r.read()
            if i >= 200:                         # 200 premiers : mise en route
                durees.append((time.perf_counter() - t) * 1000)
    service.terminate()
    service.wait()
    durees.sort()
    return statistics.median(durees), durees[int(0.99 * len(durees))]


async def autre_corps():
    preparer()
    service = await lancer_service("atomique", rapide=True)
    entetes = {"Idempotency-Key": "commande-4521"}
    async with aiohttp.ClientSession() as s:
        for montant in (100, 100, 250):
            corps = {"compte": 7, "montant": montant, "ref": "c4521"}
            async with s.post(f"http://127.0.0.1:{SERVICE}/payer", json=corps, headers=entetes) as r:
                rep = await r.text()
                rejoue = r.headers.get("Idempotent-Replayed", "-")
                print(f"   montant {montant:>3} : {r.status} {rep}")
                print(f"                 Idempotent-Replayed : {rejoue}")
    service.terminate()
    service.wait()
    lignes, _, _ = compter()
    print(f"   paiements en base : {lignes}")


async def main():
    demarrer_pg()
    try:
        print(f"PostgreSQL 18 ; {N} paiements, {PARALLELE} à la fois, délai {DELAI:g} s, "
              f"réponse perdue {PERTE:.0%}\n")
        print("1. paiements faits deux fois")
        print(f"   {'mode':<26}{'tent.':>6}{'perdues':>8}{'ok':>6}{'échec':>6}"
              f"{'dont':>6}{'doublons':>9}{'rejouées':>9}")
        print(f"   {'':<26}{'':>6}{'':>8}{'':>6}{'':>6}{'faits':>6}")
        await campagne("sans retry", "sans", False, False)
        await campagne("retry sans clé", "sans", True, False)
        await campagne("clé vérifiée puis écrite", "naive", True, True)
        await campagne("clé dans la transaction", "atomique", True, True)

        print("\n2. coût de la clé, paiements l'un après l'autre (ms)")
        for mode, nom in (("sans", "sans clé"), ("atomique", "clé dans la transaction")):
            med, p99 = await latence(mode)
            print(f"   {nom:<28} médiane {med:5.2f}   99e centile {p99:5.2f}")

        print("\n3. même clé, trois requêtes")
        await autre_corps()
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)


if __name__ == "__main__":
    asyncio.run(main())
