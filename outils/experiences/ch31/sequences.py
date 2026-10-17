"""Chapitre 31 : séquences de PostgreSQL 18 et allocation par blocs.

1. trous : annulation, violation de contrainte, arrêt brutal du serveur ;
2. séquence avec CACHE 100 et deux sessions : valeurs hors d'ordre ;
3. ordre d'attribution contre ordre de validation : un lecteur qui suit
   « id > dernier vu » manque une ligne ;
4. débit avec 8 clients : un nextval par identifiant, un aller-retour pour
   1 000 identifiants, et allocation par blocs dans une table (hi/lo).

    python3 sequences.py
"""
import threading
import time

import psycopg

import commun


def une(c, sql, *args):
    return c.execute(sql, args).fetchone()


def trous():
    print("1. trous dans une séquence")
    with psycopg.connect(commun.DSN, autocommit=True) as c:
        c.execute("create table commandes (id bigint generated always as "
                  "identity primary key, ref text unique)")
        c.execute("insert into commandes (ref) values ('a'), ('b')")
        with c.transaction(force_rollback=True):
            c.execute("insert into commandes (ref) values ('annulée')")
        try:
            c.execute("insert into commandes (ref) values ('a')")
        except psycopg.errors.UniqueViolation:
            pass
        c.execute("insert into commandes (ref) values ('c')")
        ids = [r[0] for r in c.execute("select id from commandes order by id")]
        print(f"   après annulation et doublon refusé : {ids}")
        for i in range(5):
            c.execute("insert into commandes (ref) values (%s)", (f"d{i}",))
        avant = une(c, "select max(id) from commandes")[0]
    commun.tuer_et_relancer()
    with psycopg.connect(commun.DSN, autocommit=True) as c:
        c.execute("insert into commandes (ref) values ('après')")
        apres = une(c, "select max(id) from commandes")[0]
        print(f"   arrêt brutal (SIGKILL) : dernier id {avant}, "
              f"suivant {apres}")


def cache():
    print("\n2. séquence CACHE 100, deux sessions qui alternent")
    with psycopg.connect(commun.DSN, autocommit=True) as a, \
            psycopg.connect(commun.DSN, autocommit=True) as b:
        a.execute("create sequence s_cache cache 100")
        vus = []
        for _ in range(3):
            vus.append(("A", une(a, "select nextval('s_cache')")[0]))
            vus.append(("B", une(b, "select nextval('s_cache')")[0]))
        print("   " + ", ".join(f"{s}:{v}" for s, v in vus))
    with psycopg.connect(commun.DSN, autocommit=True) as c:
        print(f"   après fermeture des sessions, nextval = "
              f"{une(c, "select nextval('s_cache')")[0]}")


def validation():
    print("\n3. ordre d'attribution contre ordre de validation")
    with psycopg.connect(commun.DSN, autocommit=True) as c:
        c.execute("create table evenements (id bigint generated always as "
                  "identity primary key, quoi text)")
    a = psycopg.connect(commun.DSN)
    b = psycopg.connect(commun.DSN, autocommit=True)
    lecteur = psycopg.connect(commun.DSN, autocommit=True)
    a.execute("insert into evenements (quoi) values ('A, lent')")
    b.execute("insert into evenements (quoi) values ('B, rapide')")
    dernier, vus = 0, []
    for r in lecteur.execute("select id, quoi from evenements where id > %s "
                             "order by id", (dernier,)):
        vus.append(r)
        dernier = r[0]
    a.commit()
    for r in lecteur.execute("select id, quoi from evenements where id > %s "
                             "order by id", (dernier,)):
        vus.append(r)
        dernier = r[0]
    tout = lecteur.execute("select id, quoi from evenements order by id"
                           ).fetchall()
    print(f"   dans la table : {tout}")
    print(f"   vu par le lecteur (id > dernier vu) : {vus}")
    for x in (a, b, lecteur):
        x.close()


def debit(nom, prendre, duree=5, clients=8, reglage=None):
    """prendre(connexion) renvoie une liste d'identifiants ; reglage est une
    instruction exécutée une fois par connexion."""
    total, verrou, fin = [], threading.Lock(), time.monotonic() + duree
    allers = [0]

    def client():
        with psycopg.connect(commun.DSN, autocommit=True) as c:
            if reglage:
                c.execute(reglage)
            n, a = 0, 0
            ids = set()
            while time.monotonic() < fin:
                lot = prendre(c)
                a += 1
                n += len(lot)
                ids.update(lot)
            with verrou:
                total.append(ids)
                allers[0] += a
    fils = [threading.Thread(target=client) for _ in range(clients)]
    for f in fils:
        f.start()
    for f in fils:
        f.join()
    tous = set().union(*total)
    n = sum(map(len, total))
    print(f"   {nom:34} {n / duree:9.0f} {allers[0] / duree:9.0f} "
          f"{n - len(tous):9}")


def blocs():
    print("\n4. débit, 8 clients Python pendant 5 s")
    print(f"   {'':34} {'id/s':>9} {'allers/s':>9} {'doublons':>9}")
    with psycopg.connect(commun.DSN, autocommit=True) as c:
        c.execute("create sequence s1")
        c.execute("create sequence s1000 increment 1000")
        c.execute("create table allocateur (nom text primary key, "
                  "prochain bigint not null)")
        c.execute("insert into allocateur values ('commandes', 1)")
    debit("nextval par identifiant",
          lambda c: [une(c, "select nextval('s1')")[0]])
    debit("1 000 nextval par aller-retour",
          lambda c: [r[0] for r in c.execute(
              "select nextval('s1') from generate_series(1, 1000)")])
    debit("séquence INCREMENT 1000 (hi/lo)",
          lambda c: (lambda h: list(range(h, h + 1000)))(
              une(c, "select nextval('s1000')")[0]))
    debit("table allocateur, bloc de 1 000",
          lambda c: (lambda h: list(range(h - 1000, h)))(
              une(c, "update allocateur set prochain = prochain + 1000 "
                     "where nom = 'commandes' returning prochain")[0]))
    debit("même table, synchronous_commit off",
          lambda c: (lambda h: list(range(h - 1000, h)))(
              une(c, "update allocateur set prochain = prochain + 1000 "
                     "where nom = 'commandes' returning prochain")[0]),
          reglage="set synchronous_commit = off")


def main():
    commun.lancer()
    try:
        trous()
        cache()
        validation()
        blocs()
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
