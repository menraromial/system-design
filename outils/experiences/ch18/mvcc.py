"""Chapitre 18 : ce que fait le MVCC de PostgreSQL quand on modifie une ligne.

  1. une ligne modifiée change d'emplacement (ctid) et de transaction créatrice
     (xmin) : PostgreSQL écrit une nouvelle version, l'ancienne reste ;
  2. une table de 100 000 lignes, modifiées toutes une fois : versions mortes,
     taille de la table, puis effet de VACUUM ;
  3. la même chose pendant qu'une transaction REPEATABLE READ, ouverte avant la
     modification, garde son instantané : VACUUM ne peut rien retirer ;
  4. un lecteur ne bloque pas un écrivain : une transaction lit pendant qu'une
     autre modifie la même ligne, et on mesure l'attente de l'écrivain.
L'autovacuum est désactivé sur la table pour que les mesures ne bougent pas, et
les versions mortes sont comptées exactement avec l'extension pgstattuple.
Prérequis : docker, image postgres:18-alpine. Usage : python3 mvcc.py
"""
import time

import psycopg

import commun


def un(cx, sql, *args):
    return cx.execute(sql, args).fetchone()


def etat(cx):
    mortes, = un(cx, "SELECT dead_tuple_count FROM pgstattuple('t')")
    taille, = un(cx, "SELECT pg_relation_size('t')")
    return mortes, taille / 2**20


def main():
    commun.lancer()
    try:
        cx = psycopg.connect(commun.DSN, autocommit=True)
        cx.execute("CREATE EXTENSION pgstattuple")
        cx.execute("CREATE TABLE t (id int PRIMARY KEY, v int) WITH (autovacuum_enabled = false)")
        cx.execute("INSERT INTO t SELECT i, 0 FROM generate_series(1, 100000) i")
        cx.execute("VACUUM t")

        print("1. une ligne modifiée deux fois")
        for etape in ("avant", "après UPDATE", "après UPDATE"):
            if etape != "avant":
                cx.execute("UPDATE t SET v = v + 1 WHERE id = 1")
            ctid, xmin, v = un(cx, "SELECT ctid, xmin, v FROM t WHERE id = 1")
            print(f"   {etape:<14} ctid {str(ctid):<10} xmin {str(xmin):<6} v = {v}")

        cx.execute("VACUUM t")
        print("\n2. 100 000 lignes modifiées une fois chacune")
        print(f"   {'':<36}{'versions mortes':>16}{'taille':>12}")
        m, s = etat(cx); print(f"   {'au départ':<36}{m:>16}{s:>9.1f} Mo")
        cx.execute("UPDATE t SET v = v + 1")
        m, s = etat(cx); print(f"   {'après UPDATE de toute la table':<36}{m:>16}{s:>9.1f} Mo")
        cx.execute("VACUUM t")
        m, s = etat(cx); print(f"   {'après VACUUM':<36}{m:>16}{s:>9.1f} Mo")
        cx.execute("UPDATE t SET v = v + 1")
        cx.execute("VACUUM t")
        m, s = etat(cx); print(f"   {'nouvel UPDATE puis VACUUM':<36}{m:>16}{s:>9.1f} Mo")

        print("\n3. une transaction ouverte garde son instantané")
        vieux = psycopg.connect(commun.DSN, autocommit=True)
        vieux.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        avant, = un(vieux, "SELECT sum(v) FROM t")
        cx.execute("UPDATE t SET v = v + 1")
        cx.execute("VACUUM t")
        m, s = etat(cx); print(f"   {'UPDATE puis VACUUM, lecteur ouvert':<36}{m:>16}{s:>9.1f} Mo")
        apres, = un(vieux, "SELECT sum(v) FROM t")
        print(f"   le lecteur voit toujours sum(v) = {apres} (il voyait {avant} au début)")
        vieux.execute("COMMIT")
        cx.execute("VACUUM t")
        m, s = etat(cx); print(f"   {'lecteur fermé, puis VACUUM':<36}{m:>16}{s:>9.1f} Mo")

        print("\n4. un lecteur ne bloque pas un écrivain")
        lecteur = psycopg.connect(commun.DSN, autocommit=True)
        lecteur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        un(lecteur, "SELECT v FROM t WHERE id = 1")
        t0 = time.perf_counter()
        cx.execute("UPDATE t SET v = v + 1 WHERE id = 1")
        print(f"   UPDATE pendant une lecture ouverte : {1000 * (time.perf_counter() - t0):.2f} ms")
        lecteur.execute("COMMIT")
        bloqueur = psycopg.connect(commun.DSN, autocommit=True)
        bloqueur.execute("BEGIN")
        un(bloqueur, "SELECT v FROM t WHERE id = 1 FOR UPDATE")
        cx.execute("SET lock_timeout = '500ms'")
        t0 = time.perf_counter()
        try:
            cx.execute("UPDATE t SET v = v + 1 WHERE id = 1")
            print("   UPDATE pendant un SELECT ... FOR UPDATE : pas bloqué")
        except psycopg.errors.LockNotAvailable:
            print(f"   UPDATE pendant un SELECT ... FOR UPDATE : bloqué, abandonné après "
                  f"{1000 * (time.perf_counter() - t0):.0f} ms (lock_timeout)")
        bloqueur.execute("ROLLBACK")
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
