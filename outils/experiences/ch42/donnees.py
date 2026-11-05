"""Chapitre 42 : une base par service, et ce que devient une jointure.

PostgreSQL 18 (conteneur cours-sd-ch42-pg, port 12132). 100 000 clients et
1 000 000 de commandes. Le monolithe a les deux tables dans la même base ; les
microservices ont chacun la leur (bases « clients » et « commandes ») et un
service HTTP (aiohttp) devant : clients sur le port 12141, commandes sur 12142.

1. page des 50 dernières commandes avec le nom du client :
     jointure SQL dans la base du monolithe ;
     composition : 1 appel aux commandes, puis 1 appel par commande aux clients ;
     composition groupée : 1 appel aux commandes, 1 appel aux clients avec les
     50 identifiants ;
     copie locale : le service des commandes garde le nom du client, recopié à
     chaque changement (par événement), et répond seul.
2. chiffre d'affaires par pays : jointure et agrégation SQL, contre
   composition (totaux par client depuis les commandes, pays depuis les
   clients, agrégation par l'appelant), avec les octets échangés.

    python3 donnees.py
"""
import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
import psycopg
from aiohttp import web
from psycopg_pool import AsyncConnectionPool

ICI = Path(__file__)
NOM, PG = "cours-sd-ch42-pg", 12132
CLIENTS, COMMANDES = 12141, 12142


def dsn(base):
    return f"host=127.0.0.1 port={PG} dbname={base} user=postgres"


def demarrer_pg():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NOM, "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                    "-p", f"127.0.0.1:{PG}:5432", "--shm-size=512m", "postgres:18-alpine",
                    "-c", "shared_buffers=512MB"], check=True, capture_output=True)
    for _ in range(120):
        try:
            with psycopg.connect(dsn("postgres")) as c:
                c.execute("select 1")
                return
        except psycopg.OperationalError:
            time.sleep(0.5)
    raise RuntimeError("PostgreSQL absent")


def preparer():
    with psycopg.connect(dsn("postgres"), autocommit=True) as c:
        for b in ("monolithe", "clients", "commandes"):
            c.execute(f"create database {b}")
    clients_sql = """create table clients (id int primary key, nom text not null, pays text not null);
        insert into clients select g, 'client ' || g,
          (array['FR','DE','ES','IT','BE','CH','CM','SN','CA','US'])[1 + g % 10]
          from generate_series(1, 100000) g;"""
    commandes_sql = """create table commandes (id bigint primary key, client_id int not null,
          montant int not null, cree timestamptz not null{nom});
        insert into commandes select g, 1 + (g::bigint * 7919) % 100000, 100 + g % 9000,
          now() - g * interval '1 second'{valnom} from generate_series(1, 1000000) g;
        create index on commandes (cree desc); create index on commandes (client_id);"""
    with psycopg.connect(dsn("monolithe"), autocommit=True) as c:
        c.execute(clients_sql)
        c.execute(commandes_sql.format(nom="", valnom=""))
        c.execute("analyze")
    with psycopg.connect(dsn("clients"), autocommit=True) as c:
        c.execute(clients_sql)
        c.execute("analyze")
    with psycopg.connect(dsn("commandes"), autocommit=True) as c:
        c.execute(commandes_sql.format(nom=", nom_client text not null",
                                       valnom=", 'client ' || (1 + (g::bigint * 7919) % 100000)"))
        c.execute("analyze")


# ---- les deux services -----------------------------------------------------------
def services():
    pools = {b: AsyncConnectionPool(dsn(b), min_size=10, max_size=10, open=False) for b in ("clients", "commandes")}

    async def client(requete):
        async with pools["clients"].connection() as c:
            r = await (await c.execute("select id, nom, pays from clients where id = %s",
                                       (int(requete.match_info["id"]),))).fetchone()
        return web.json_response({"id": r[0], "nom": r[1], "pays": r[2]})

    async def clients_lot(requete):
        ids = (await requete.json())["ids"]
        async with pools["clients"].connection() as c:
            rows = await (await c.execute("select id, nom, pays from clients where id = any(%s)", (ids,))).fetchall()
        return web.json_response([{"id": a, "nom": b, "pays": p} for a, b, p in rows])

    async def dernieres(requete):
        avec_nom = requete.query.get("nom") == "1"
        cols = "id, client_id, montant, nom_client" if avec_nom else "id, client_id, montant"
        async with pools["commandes"].connection() as c:
            rows = await (await c.execute(f"select {cols} from commandes order by cree desc limit 50")).fetchall()
        return web.json_response([list(r) for r in rows])

    async def totaux(requete):
        async with pools["commandes"].connection() as c:
            rows = await (await c.execute("select client_id, sum(montant) from commandes group by client_id")).fetchall()
        return web.json_response([[a, int(b)] for a, b in rows])

    async def principal():
        for pool in pools.values():
            await pool.open(wait=True)
        for port, routes in ((CLIENTS, [web.get("/clients/{id}", client), web.post("/clients/lot", clients_lot)]),
                             (COMMANDES, [web.get("/commandes/dernieres", dernieres),
                                          web.get("/commandes/totaux", totaux)])):
            app = web.Application()
            app.add_routes(routes)
            r = web.AppRunner(app, access_log=None)
            await r.setup()
            await web.TCPSite(r, "127.0.0.1", port).start()
        await asyncio.Event().wait()

    asyncio.run(principal())


# ---- les mesures -----------------------------------------------------------------
async def mesurer(f, n):
    durees = []
    for _ in range(n):
        t = time.perf_counter()
        await f()
        durees.append((time.perf_counter() - t) * 1000)
    durees.sort()
    return statistics.median(durees), durees[int(0.99 * (len(durees) - 1))]


async def client_mesures():
    mono = await psycopg.AsyncConnection.connect(dsn("monolithe"))
    octets = {"n": 0}
    async with aiohttp.ClientSession() as s:
        for _ in range(100):
            try:
                async with s.get(f"http://127.0.0.1:{COMMANDES}/commandes/dernieres") as r:
                    await r.read()
                break
            except aiohttp.ClientError:
                await asyncio.sleep(0.1)

        async def get(url):
            async with s.get(url) as r:
                corps = await r.read()
                octets["n"] += len(corps)
                return json.loads(corps)

        async def post(url, donnees):
            async with s.post(url, json=donnees) as r:
                corps = await r.read()
                octets["n"] += len(corps)
                return json.loads(corps)

        async def jointure():
            await (await mono.execute("select c.id, c.montant, k.nom from commandes c join clients k"
                                      " on k.id = c.client_id order by c.cree desc limit 50")).fetchall()

        async def n_plus_1():
            cmds = await get(f"http://127.0.0.1:{COMMANDES}/commandes/dernieres")
            for c in cmds:
                await get(f"http://127.0.0.1:{CLIENTS}/clients/{c[1]}")

        async def groupee():
            cmds = await get(f"http://127.0.0.1:{COMMANDES}/commandes/dernieres")
            await post(f"http://127.0.0.1:{CLIENTS}/clients/lot", {"ids": [c[1] for c in cmds]})

        async def copie():
            await get(f"http://127.0.0.1:{COMMANDES}/commandes/dernieres?nom=1")

        print("1. les 50 dernières commandes avec le nom du client (ms, 500 mesures)")
        print(f"   {'méthode':<34}{'appels':>7}{'méd.':>8}{'99e':>8}")
        for nom, f, appels in (("jointure SQL (monolithe)", jointure, 0), ("composition, un appel par ligne", n_plus_1, 51),
                               ("composition groupée", groupee, 2), ("copie locale du nom", copie, 1)):
            for _ in range(20):
                await f()
            med, p99 = await mesurer(f, 500)
            print(f"   {nom:<34}{appels:>7}{med:>8.2f}{p99:>8.2f}")

        print("\n2. chiffre d'affaires par pays, sur 1 000 000 de commandes (5 mesures)")

        async def agreg_sql():
            await (await mono.execute("select k.pays, sum(c.montant) from commandes c join clients k"
                                      " on k.id = c.client_id group by k.pays")).fetchall()

        async def agreg_api():
            totaux = await get(f"http://127.0.0.1:{COMMANDES}/commandes/totaux")
            pays = await post(f"http://127.0.0.1:{CLIENTS}/clients/lot", {"ids": [t[0] for t in totaux]})
            p = {c["id"]: c["pays"] for c in pays}
            res = {}
            for cid, montant in totaux:
                res[p[cid]] = res.get(p[cid], 0) + montant
            return res

        for nom, f in (("jointure et agrégation SQL", agreg_sql), ("composition par l'appelant", agreg_api)):
            await f()
            octets["n"] = 0
            med, _ = await mesurer(f, 5)
            recus = f", {octets['n'] / 5 / 1e6:.2f} Mo reçus en HTTP" if octets["n"] else ""
            print(f"   {nom:<28} médiane {med:5.0f} ms{recus}")
    await mono.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--services", action="store_true")
    if p.parse_args().services:
        services()
        return
    demarrer_pg()
    proc = None
    try:
        t = time.monotonic()
        preparer()
        print(f"PostgreSQL 18 : 100 000 clients, 1 000 000 de commandes, prêts en {time.monotonic() - t:.0f} s\n")
        proc = subprocess.Popen([sys.executable, str(ICI), "--services"])
        asyncio.run(client_mesures())
    finally:
        if proc:
            proc.kill()
            proc.wait()
        subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)


if __name__ == "__main__":
    main()
