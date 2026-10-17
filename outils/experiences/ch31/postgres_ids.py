"""Chapitre 31 : l'identifiant et l'index B-tree de PostgreSQL 18.

Quatre tables identiques sauf leur clé primaire : bigint par séquence
(identity), UUID v7 (uuidv7()), UUID v4 (gen_random_uuid()), et UUID v4
rangé en texte. Chacune reçoit 5 millions de lignes par lots de 50 000.
Pour chacune : durée, débit des derniers lots, octets de journal (WAL) et
images de pages complètes (pg_stat_wal), lectures hors du cache de
PostgreSQL pendant l'insertion, taille et densité des feuilles de l'index
(pgstatindex), puis pages lues pour relire les 20 000 dernières lignes par
leur clé.

    python3 postgres_ids.py
"""
import time

import psycopg

import commun

LIGNES, LOT = 5_000_000, 50_000
TABLES = {
    "bigint (séquence)": ("t_bigint",
                          "bigint generated always as identity primary key"),
    "UUID v7": ("t_uuid7", "uuid primary key default uuidv7()"),
    "UUID v4": ("t_uuid4", "uuid primary key default gen_random_uuid()"),
    "UUID v4 en texte": ("t_texte",
                         "text primary key default gen_random_uuid()::text"),
}


def une(c, sql, *args):
    return c.execute(sql, args).fetchone()


def wal(c):
    return une(c, "select wal_bytes, wal_fpi from pg_stat_wal")


def lectures_index(c, table):
    c.execute("select pg_stat_force_next_flush()")
    return une(c, "select coalesce(idx_blks_read, 0) from pg_statio_user_indexes "
                  "where relname = %s", table)[0]


def mo(n):
    return f"{n / 2**20:6.0f} Mo"


def inserer(c, nom, table, definition):
    c.execute(f"create table {table} (id {definition}, "
              "cree timestamptz not null default now(), n bigint not null)")
    c.execute("checkpoint")
    w0, r0, t0 = wal(c), lectures_index(c, table), time.monotonic()
    derniers = []
    for k in range(LIGNES // LOT):
        debut = time.monotonic()
        c.execute(f"insert into {table} (n) select %s + g "
                  "from generate_series(1, %s) g", (k * LOT, LOT))
        derniers.append(time.monotonic() - debut)
    duree = time.monotonic() - t0
    w1, r1 = wal(c), lectures_index(c, table)
    taille_idx = une(c, "select pg_relation_size(%s)", f"{table}_pkey")[0]
    taille_tab = une(c, "select pg_relation_size(%s)", table)[0]
    densite, feuilles = une(c, "select avg_leaf_density, leaf_pages "
                               "from pgstatindex(%s)", f"{table}_pkey")
    return {"duree": duree, "fin": LOT * 10 / sum(derniers[-10:]),
            "wal": w1[0] - w0[0], "fpi": w1[1] - w0[1], "lu": r1 - r0,
            "idx": taille_idx, "tab": taille_tab, "dens": densite,
            "feuilles": feuilles}


def relire_recents(c, table):
    """Accès aux pages partagées (index et table) pour relire par leur clé
    les 20 000 lignes les plus récentes, cache de PostgreSQL vidé par un
    redémarrage ; le cache du système reste chaud."""
    c.execute(f"create temp table recents as select id from {table} "
              f"where n > {LIGNES - 20_000}")
    c.execute("analyze recents")
    c.execute("set enable_hashjoin = off; set enable_mergejoin = off; "
              "set enable_seqscan = off")
    plan = c.execute(f"explain (analyze, buffers, format json) "
                     f"select count(*) from recents r join {table} t "
                     f"using (id)").fetchone()[0][0]
    c.execute("drop table recents")
    c.execute("reset all")
    p = plan["Plan"]
    return (plan["Execution Time"], p.get("Shared Hit Blocks", 0)
            + p.get("Shared Read Blocks", 0), p.get("Shared Read Blocks", 0))


def main():
    commun.lancer()
    try:
        with psycopg.connect(commun.DSN, autocommit=True) as c:
            version = une(c, "show server_version")[0]
            c.execute("create extension pgstattuple")
            print(f"PostgreSQL {version}, shared_buffers 128 Mo ; "
                  f"{LIGNES // 10**6} millions de lignes par lots de "
                  f"{LOT // 1000} 000")
            res = {}
            print("\n1. insertion")
            print(f"   {'clé':18} {'durée':>6} {'fin, l/s':>9} {'WAL':>9} "
                  f"{'FPI':>8} {'lus hors cache':>14}")
            for nom, (table, definition) in TABLES.items():
                r = res[nom] = inserer(c, nom, table, definition)
                print(f"   {nom:18} {r['duree']:5.0f}s {r['fin']:9.0f} "
                      f"{mo(r['wal'])} {r['fpi']:8} {r['lu']:14}")
            print("\n2. taille")
            print(f"   {'clé':18} {'index':>9} {'table':>9} {'feuilles':>9} "
                  f"{'densité':>8}")
            for nom, r in res.items():
                print(f"   {nom:18} {mo(r['idx'])} {mo(r['tab'])} "
                      f"{r['feuilles']:9} {r['dens']:7.1f} %")
        commun.tuer_et_relancer()           # cache de PostgreSQL vide
        with psycopg.connect(commun.DSN, autocommit=True) as c:
            print("\n3. relire les 20 000 lignes les plus récentes par leur clé"
                  " (cache vide)")
            for nom, (table, _) in TABLES.items():
                ms, touchees, lues = relire_recents(c, table)
                print(f"   {nom:18} {ms:7.1f} ms, {touchees:6} accès à des "
                      f"pages, {lues:6} hors cache")
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
