"""Chapitre 19 : ce que coûte un index à chaque écriture.

  1. 200 000 insertions dans une table de commandes, par lots de 1 000 dans une
     transaction, avec 0, 1, 3 puis 5 index secondaires en plus de la clé
     primaire : durée, journal écrit (WAL) et taille finale ;
  2. 50 000 mises à jour du montant de commandes existantes (table remplie à 90 %,
     fillfactor 90) : sans index sur montant, PostgreSQL peut faire des mises à
     jour HOT, sans toucher aux index ; avec un index sur montant, il ne le peut
     plus. On compte les mises à jour HOT et le journal écrit.
Prérequis : docker, image postgres:18-alpine. Usage : python3 ecritures.py
"""
import datetime
import random
import time

import psycopg

import commun

INDEX = ("CREATE INDEX ON commandes (client_id)",
         "CREATE INDEX ON commandes (client_id, cree_le)",
         "CREATE INDEX ON commandes (statut)",
         "CREATE INDEX ON commandes (cree_le)",
         "CREATE INDEX ON commandes (montant)")


def creer(cx, n_index, fillfactor=100):
    cx.execute("DROP TABLE IF EXISTS commandes")
    cx.execute(f"""CREATE TABLE commandes (id bigserial PRIMARY KEY, client_id int, cree_le timestamptz,
                   statut text, montant numeric(10,2)) WITH (fillfactor = {fillfactor})""")
    for sql in INDEX[:n_index]:
        cx.execute(sql)


def lignes(n, alea):
    t0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    statuts = ("livree",) * 17 + ("expediee",) * 2 + ("annulee",)
    return [(alea.randint(1, 100_000), t0 + datetime.timedelta(seconds=alea.randint(0, 30_000_000)),
             alea.choice(statuts), round(alea.uniform(0, 300), 2)) for _ in range(n)]


def wal(cx):
    return cx.execute("SELECT pg_current_wal_insert_lsn()").fetchone()[0]


def octets_wal(cx, avant):
    return cx.execute("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), %s)", (avant,)).fetchone()[0]


def main():
    commun.lancer()
    try:
        cx = psycopg.connect(commun.DSN, autocommit=True)
        donnees = lignes(200_000, random.Random(19))
        print("1. 200 000 insertions par lots de 1 000")
        print(f"   {'index secondaires':<20}{'durée':>9}{'lignes/s':>11}{'journal':>11}{'taille':>11}")
        for n in (0, 1, 3, 5):
            creer(cx, n)
            avant = wal(cx)
            t = time.perf_counter()
            with cx.cursor() as cur:
                for i in range(0, len(donnees), 1000):
                    with cx.transaction():
                        cur.executemany("""INSERT INTO commandes (client_id, cree_le, statut, montant)
                                           VALUES (%s, %s, %s, %s)""", donnees[i:i + 1000])
            duree = time.perf_counter() - t
            journal = octets_wal(cx, avant) / 2**20
            taille = cx.execute("SELECT pg_total_relation_size('commandes')").fetchone()[0] / 2**20
            print(f"   {n:<20}{duree:>7.2f} s{len(donnees) / duree:>11,.0f}{journal:>8.1f} Mo{taille:>8.1f} Mo"
                  .replace(",", " "))

        print("\n2. 50 000 mises à jour du montant (fillfactor 90, 4 index, aucun sur montant)")
        print(f"   {'':<28}{'mises à jour':>13}{'dont HOT':>10}{'journal':>11}")
        alea = random.Random(91)
        for avec in (False, True):
            creer(cx, 4, fillfactor=90)
            if avec:
                cx.execute(INDEX[4])
            with cx.cursor() as cur:
                cur.executemany("""INSERT INTO commandes (client_id, cree_le, statut, montant)
                                   VALUES (%s, %s, %s, %s)""", donnees)
            cx.execute("VACUUM ANALYZE commandes")
            ids = alea.sample(range(1, 200_001), 50_000)
            avant = wal(cx)
            with cx.transaction():
                cx.execute("UPDATE commandes SET montant = montant + 1 WHERE id = ANY(%s)", (ids,))
                n_upd, n_hot = cx.execute("""SELECT n_tup_upd, n_tup_hot_upd FROM pg_stat_xact_user_tables
                                             WHERE relname = 'commandes'""").fetchone()
            journal = octets_wal(cx, avant) / 2**20
            nom = "avec un index sur montant" if avec else "sans index sur montant"
            print(f"   {nom:<28}{n_upd:>13}{n_hot:>10}{journal:>8.1f} Mo")
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
