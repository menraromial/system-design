"""Chapitre 43 : event sourcing et CQRS sur PostgreSQL 18, mesurés.

Le domaine : des comptes, avec trois événements (ouvert, dépôt, retrait). Le
journal est une table `evenements` (flux, version, type, données, numéro global),
avec une clé primaire (flux, version) qui sert de contrôle de concurrence
optimiste : on ajoute l'événement de version v + 1, et l'insertion échoue si un
autre écrivain l'a fait avant. On compare à une table d'état classique, mise à
jour en place (CRUD). Conteneur cours-sd-ch43-pg, port 12232.

1. écrire : 20 000 opérations sur 1 000 comptes, 8 écrivains ; un retrait n'est
   accepté que si le solde le permet, ce qui oblige l'event sourcing à
   reconstruire l'état avant d'écrire ;
2. 8 écrivains sur un seul compte : conflits de version et nouvelles tentatives ;
3. relire un compte selon la longueur de son flux, avec et sans instantané ;
4. CQRS : reconstruire une projection (modèle de lecture) à partir d'un million
   d'événements, puis la tenir à jour pendant que l'on écrit, et mesurer son
   retard ; une requête sur la projection contre la même requête sur le journal ;
5. faire évoluer un événement : relire des événements v1 par un convertisseur
   (upcasting), ou réécrire tout le journal (copy-and-transform) ;
6. la place occupée.

    python3 evenements.py      (environ 6 minutes)
"""
import asyncio
import json
import random
import statistics
import subprocess
import time

import psycopg
from psycopg_pool import AsyncConnectionPool

NOM, PG = "cours-sd-ch43-pg", 12232
DSN = f"host=127.0.0.1 port={PG} dbname=postgres user=postgres"


def demarrer_pg():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NOM, "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                    "-p", f"127.0.0.1:{PG}:5432", "--shm-size=512m", "postgres:18-alpine",
                    "-c", "shared_buffers=512MB", "-c", "max_connections=100"], check=True, capture_output=True)
    for _ in range(120):
        try:
            with psycopg.connect(DSN) as c:
                c.execute("select 1")
                return
        except psycopg.OperationalError:
            time.sleep(0.5)
    raise RuntimeError("PostgreSQL absent")


SCHEMA = """
create table evenements (seq bigserial unique, flux text not null, version int not null,
  type text not null, donnees jsonb not null, cree timestamptz not null default clock_timestamp(),
  primary key (flux, version));
create table instantanes (flux text primary key, version int not null, etat jsonb not null);
create table etat (id text primary key, solde bigint not null, version int not null);
create table projection (id text primary key, solde bigint not null, operations int not null);
create index on projection (solde desc);
create table position_projection (nom text primary key, seq bigint not null);
"""


def appliquer(etat, typ, d):
    """Plie un événement sur l'état d'un compte (dictionnaire)."""
    if typ == "ouvert":
        return {"solde": 0, "operations": 0}
    if typ == "depot":
        return {"solde": etat["solde"] + d["montant"], "operations": etat["operations"] + 1}
    if typ == "retrait":
        return {"solde": etat["solde"] - d["montant"], "operations": etat["operations"] + 1}
    raise ValueError(typ)


async def charger(c, flux, instantane=True):
    """État et version d'un compte : instantané éventuel, puis événements suivants."""
    etat, version = None, 0
    if instantane:
        r = await (await c.execute("select version, etat from instantanes where flux = %s", (flux,))).fetchone()
        if r:
            version, etat = r
    rows = await (await c.execute("select version, type, donnees from evenements where flux = %s"
                                  " and version > %s order by version", (flux, version))).fetchall()
    for v, typ, d in rows:
        etat, version = appliquer(etat, typ, d), v
    return etat, version, len(rows)


class Conflit(Exception):
    pass


async def ajouter(c, flux, version_attendue, typ, d):
    try:
        await c.execute("insert into evenements (flux, version, type, donnees) values (%s, %s, %s, %s)",
                        (flux, version_attendue + 1, typ, json.dumps(d)))
    except psycopg.errors.UniqueViolation:
        raise Conflit()


# ---- 1 et 2. écrire ------------------------------------------------------------------
async def ecritures(pool, comptes, n_ops, ecrivains, mode):
    alea = random.Random(43)
    taches = [(alea.choice(comptes), alea.choice(("depot", "depot", "retrait")), alea.randint(1, 100))
              for _ in range(n_ops)]
    file = asyncio.Queue()
    for t in taches:
        file.put_nowait(t)
    conflits, refus, durees = 0, 0, []

    async def ouvrier():
        nonlocal conflits, refus
        async with pool.connection() as c:
            await c.set_autocommit(True)
            while not file.empty():
                flux, typ, montant = file.get_nowait()
                t = time.perf_counter()
                if mode == "crud":
                    if typ == "depot":
                        await c.execute("update etat set solde = solde + %s, version = version + 1 where id = %s",
                                        (montant, flux))
                    else:
                        r = await c.execute("update etat set solde = solde - %s, version = version + 1"
                                            " where id = %s and solde >= %s", (montant, flux, montant))
                        refus += r.rowcount == 0
                else:
                    while True:
                        etat, version, _ = await charger(c, flux)
                        if typ == "retrait" and etat["solde"] < montant:
                            refus += 1
                            break
                        try:
                            await ajouter(c, flux, version, typ, {"montant": montant})
                            break
                        except Conflit:
                            conflits += 1
                durees.append((time.perf_counter() - t) * 1000)

    t0 = time.perf_counter()
    await asyncio.gather(*(ouvrier() for _ in range(ecrivains)))
    total = time.perf_counter() - t0
    durees.sort()
    return n_ops / total, statistics.median(durees), durees[int(0.99 * len(durees))], conflits, refus


# ---- 3. relire ---------------------------------------------------------------------
async def relectures(c):
    print("\n3. relire un compte selon la longueur de son flux (ms, médiane de 20)")
    print(f"   {'événements':>11}{'sans instantané':>17}{'avec instantané':>17}{'lus':>6}")
    for n in (10, 100, 1_000, 10_000):
        flux = f"long-{n}"
        await c.execute("insert into evenements (flux, version, type, donnees) values (%s, 1, 'ouvert', '{}')",
                        (flux,))
        await c.execute("insert into evenements (flux, version, type, donnees) select %s, g, 'depot',"
                        " jsonb_build_object('montant', 10) from generate_series(2, %s) g", (flux, n))
        # instantané tous les 100 événements : on garde le dernier
        etat, version, _ = await charger(c, flux, instantane=False)
        v_inst = (version // 100) * 100
        if v_inst:
            e = {"solde": 10 * (v_inst - 1), "operations": v_inst - 1}
            await c.execute("insert into instantanes values (%s, %s, %s)", (flux, v_inst, json.dumps(e)))
        res = []
        for inst in (False, True):
            d = []
            for _ in range(20):
                t = time.perf_counter()
                etat2, _, lus = await charger(c, flux, instantane=inst)
                d.append((time.perf_counter() - t) * 1000)
            assert etat2 == etat
            res.append((statistics.median(d), lus))
        print(f"   {n:>11}{res[0][0]:>17.2f}{res[1][0]:>17.2f}{res[1][1]:>6}")


# ---- 4. CQRS -----------------------------------------------------------------------
async def projeter(c, jusqua=None, lot=5_000):
    """Applique les événements nouveaux à la projection ; renvoie le nombre traité."""
    r = await (await c.execute("select seq from position_projection where nom = 'soldes'")).fetchone()
    pos = r[0] if r else 0
    fait = 0
    while True:
        rows = await (await c.execute("select seq, flux, type, donnees, cree from evenements where seq > %s"
                                      " order by seq limit %s", (pos, lot))).fetchall()
        if not rows:
            return fait, []
        deltas = {}
        for seq, flux, typ, d, cree in rows:
            s, o = deltas.get(flux, (0, 0))
            if typ == "depot":
                deltas[flux] = (s + d["montant"], o + 1)
            elif typ == "retrait":
                deltas[flux] = (s - d["montant"], o + 1)
            else:
                deltas.setdefault(flux, (0, 0))
        pos = rows[-1][0]
        async with c.transaction():
            await c.cursor().executemany(
                "insert into projection values (%s, %s, %s) on conflict (id) do update"
                " set solde = projection.solde + excluded.solde, operations = projection.operations + excluded.operations",
                [(f, s, o) for f, (s, o) in deltas.items()])
            await c.execute("insert into position_projection values ('soldes', %s) on conflict (nom)"
                            " do update set seq = excluded.seq", (pos,))
        fait += len(rows)
        if jusqua is not None:
            return fait, [r[4] for r in rows]


async def cqrs(pool):
    print("\n4. CQRS : projection des soldes (modèle de lecture)")
    async with pool.connection() as c:
        await c.set_autocommit(True)
        n = (await (await c.execute("select count(*) from evenements")).fetchone())[0]
        await c.execute("truncate projection, position_projection")
        t = time.perf_counter()
        fait, _ = await projeter(c)
        print(f"   reconstruction depuis zéro : {fait} événements en {time.perf_counter() - t:.1f} s")
        for nom, sql in (("projection (index)", "select id, solde from projection order by solde desc limit 10"),
                         ("journal (agrégat)", "select flux, sum(case type when 'depot' then (donnees->>'montant')::int"
                                               " when 'retrait' then -(donnees->>'montant')::int else 0 end) s"
                                               " from evenements group by flux order by s desc limit 10")):
            d = []
            for _ in range(5):
                t = time.perf_counter()
                await (await c.execute(sql)).fetchall()
                d.append((time.perf_counter() - t) * 1000)
            print(f"   10 plus gros soldes, {nom:<20}: {statistics.median(d):9.2f} ms")

    # retard de la projection pendant les écritures
    retards = []
    fin = time.monotonic() + 20

    async def ecrivain(i):
        alea = random.Random(i)
        async with pool.connection() as c:
            await c.set_autocommit(True)
            while time.monotonic() < fin:
                flux = f"c{alea.randrange(10_000)}"
                while True:
                    _, version, _ = await charger(c, flux)
                    try:
                        await ajouter(c, flux, version, "depot", {"montant": 1})
                        break
                    except Conflit:
                        pass

    async def projecteur():
        async with pool.connection() as c:
            await c.set_autocommit(True)
            while time.monotonic() < fin + 1:
                fait, crees = await projeter(c, jusqua=True, lot=1_000)
                maintenant = (await (await c.execute("select clock_timestamp()")).fetchone())[0]
                retards.extend((maintenant - x).total_seconds() * 1000 for x in crees)
                if not fait:
                    await asyncio.sleep(0.1)

    async with pool.connection() as c:
        n0 = (await (await c.execute("select count(*) from evenements")).fetchone())[0]
    await asyncio.gather(projecteur(), *(ecrivain(i) for i in range(8)))
    async with pool.connection() as c:
        n1 = (await (await c.execute("select count(*) from evenements")).fetchone())[0]
    retards.sort()
    print(f"   pendant 20 s d'écriture ({(n1 - n0) / 20:.0f} événements/s),")
    print("   projecteur interrogeant le journal toutes les 100 ms :")
    print(f"   retard de la projection : médiane {statistics.median(retards):.0f} ms,")
    print(f"   99e centile {retards[int(.99 * len(retards))]:.0f} ms, maximum {retards[-1]:.0f} ms")


# ---- 5. évolution ------------------------------------------------------------------
def convertir(typ, d, version_schema):
    """v1 : {"montant": euros} ; v2 : {"centimes": int, "devise": "EUR"}."""
    if version_schema == 1 and typ in ("depot", "retrait"):
        return {"centimes": int(round(d["montant"] * 100)), "devise": "EUR"}
    return d


async def evolution(c):
    print("\n5. faire évoluer un événement (montant en euros → centimes et devise)")
    await c.execute("alter table evenements add column schema int not null default 1")
    rows = await (await c.execute("select type, donnees, schema from evenements where flux = 'long-10000'"
                                  " order by version")).fetchall()
    d = []
    for _ in range(10):
        t = time.perf_counter()
        _ = [convertir(typ, x, s) for typ, x, s in rows]
        d.append((time.perf_counter() - t) * 1000)
    print(f"   convertir à la lecture 10 000 événements v1 : {statistics.median(d):.2f} ms en Python")
    taille0 = (await (await c.execute("select pg_total_relation_size('evenements')")).fetchone())[0]
    t = time.perf_counter()
    await c.execute("create table evenements_v2 as select seq, flux, version, type, case when type in ('depot','retrait')"
                    " then jsonb_build_object('centimes', ((donnees->>'montant')::numeric * 100)::bigint, 'devise', 'EUR')"
                    " else donnees end as donnees, cree, 2 as schema from evenements")
    await c.execute("alter table evenements_v2 add primary key (flux, version)")
    duree = time.perf_counter() - t
    taille1 = (await (await c.execute("select pg_total_relation_size('evenements_v2')")).fetchone())[0]
    n = (await (await c.execute("select count(*) from evenements_v2")).fetchone())[0]
    print(f"   recopier et transformer le journal ({n} événements) : {duree:.1f} s,")
    print(f"   {taille1 / 1e6:.0f} Mo de plus (journal d'origine : {taille0 / 1e6:.0f} Mo)")
    await c.execute("drop table evenements_v2")


async def principal():
    pool = AsyncConnectionPool(DSN, min_size=12, max_size=12, open=False)
    await pool.open(wait=True)
    async with pool.connection() as c:
        await c.set_autocommit(True)
        await c.execute(SCHEMA)
        comptes = [f"c{i}" for i in range(1_000)]
        await c.execute("insert into etat select 'c' || g, 100000, 1 from generate_series(0, 999) g")
        await c.execute("insert into evenements (flux, version, type, donnees) select 'c' || g, 1, 'ouvert', '{}'"
                        " from generate_series(0, 999) g")
        await c.execute("insert into evenements (flux, version, type, donnees) select 'c' || g, 2, 'depot',"
                        " '{\"montant\": 100000}' from generate_series(0, 999) g")

    print("1. 20 000 opérations sur 1 000 comptes, 8 écrivains (latences en ms)")
    print(f"   {'mode':<16}{'op/s':>8}{'méd.':>8}{'99e':>8}{'conflits':>10}{'refus':>8}")
    for mode, nom in (("crud", "table d'état"), ("es", "event sourcing")):
        debit, med, p99, conf, refus = await ecritures(pool, comptes, 20_000, 8, mode)
        print(f"   {nom:<16}{debit:>8.0f}{med:>8.2f}{p99:>8.2f}{conf:>10}{refus:>8}")

    print("\n2. 8 écrivains, 2 000 opérations sur un seul compte")
    print(f"   {'mode':<16}{'op/s':>8}{'méd.':>8}{'99e':>8}{'conflits':>10}{'refus':>8}")
    for mode, nom in (("crud", "table d'état"), ("es", "event sourcing")):
        debit, med, p99, conf, refus = await ecritures(pool, ["c0"], 2_000, 8, mode)
        print(f"   {nom:<16}{debit:>8.0f}{med:>8.2f}{p99:>8.2f}{conf:>10}{refus:>8}")

    async with pool.connection() as c:
        await c.set_autocommit(True)
        await relectures(c)
        # un million d'événements sur 10 000 comptes pour la suite
        await c.execute("insert into evenements (flux, version, type, donnees) select 'c' || g, 1, 'ouvert', '{}'"
                        " from generate_series(1000, 9999) g")
        await c.execute("insert into evenements (flux, version, type, donnees) select 'c' || (1000 + g % 9000),"
                        " 2 + g / 9000, 'depot', jsonb_build_object('montant', 1 + g % 100)"
                        " from generate_series(0, 899999) g")
        await c.execute("analyze")
    await cqrs(pool)
    async with pool.connection() as c:
        await c.set_autocommit(True)
        await evolution(c)
        print("\n6. place occupée")
        for t in ("evenements", "etat", "projection", "instantanes"):
            n = (await (await c.execute(f"select count(*) from {t}")).fetchone())[0]
            s = (await (await c.execute(f"select pg_total_relation_size('{t}')")).fetchone())[0]
            print(f"   {t:<12}{n:>10} lignes {s / 1e6:>8.1f} Mo")
    await pool.close()


def main():
    demarrer_pg()
    try:
        asyncio.run(principal())
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)


if __name__ == "__main__":
    main()
