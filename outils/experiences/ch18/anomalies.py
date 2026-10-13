"""Chapitre 18 : les anomalies d'isolation, provoquées une par une dans PostgreSQL.

Deux connexions, A et B, exécutent leurs instructions dans un ordre imposé, à
chacun des quatre niveaux d'isolation de SQL. Cinq scénarios :
  - lecture sale : B lit une modification de A que A annulera ;
  - lecture non répétable : B relit une ligne que A a modifiée entre-temps ;
  - fantôme : B recompte des lignes alors que A en a inséré une ;
  - mise à jour perdue : A et B lisent un solde, ajoutent chacun un montant
    calculé par l'application, écrivent ; l'un des ajouts disparaît ;
  - write skew : deux médecins de garde ; chacun vérifie qu'il en reste un
    autre, puis se retire ; il n'en reste plus aucun.
Pour chaque case : l'anomalie se produit, ou elle est empêchée (silencieusement,
ou par une erreur de sérialisation 40001 que l'application doit rejouer).
Prérequis : docker, image postgres:18-alpine. Usage : python3 anomalies.py
"""
import psycopg
from psycopg import errors

import commun

NIVEAUX = ("READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE")


def preparer(cx):
    cx.execute("""
        DROP TABLE IF EXISTS comptes, produits, gardes;
        CREATE TABLE comptes (id int PRIMARY KEY, solde int);
        INSERT INTO comptes VALUES (1, 100);
        CREATE TABLE produits (id serial PRIMARY KEY, prix int);
        INSERT INTO produits (prix) VALUES (5), (8), (12), (30);
        CREATE TABLE gardes (medecin text PRIMARY KEY, de_garde bool);
        INSERT INTO gardes VALUES ('alice', true), ('bob', true);
    """)


def un(cx, sql, *args):
    return cx.execute(sql, args).fetchone()[0]


def debut(cx, niveau):
    cx.execute(f"BEGIN ISOLATION LEVEL {niveau}")


def lecture_sale(a, b, niveau):
    debut(a, niveau); debut(b, niveau)
    a.execute("UPDATE comptes SET solde = 0 WHERE id = 1")
    vu = un(b, "SELECT solde FROM comptes WHERE id = 1")
    a.execute("ROLLBACK"); b.execute("COMMIT")
    return "se produit" if vu == 0 else "empêchée"


def non_repetable(a, b, niveau):
    debut(b, niveau)
    v1 = un(b, "SELECT solde FROM comptes WHERE id = 1")
    a.execute("BEGIN"); a.execute("UPDATE comptes SET solde = solde + 50 WHERE id = 1"); a.execute("COMMIT")
    v2 = un(b, "SELECT solde FROM comptes WHERE id = 1")
    b.execute("COMMIT")
    return "se produit" if v1 != v2 else "empêchée"


def fantome(a, b, niveau):
    debut(b, niveau)
    n1 = un(b, "SELECT count(*) FROM produits WHERE prix < 10")
    a.execute("BEGIN"); a.execute("INSERT INTO produits (prix) VALUES (3)"); a.execute("COMMIT")
    n2 = un(b, "SELECT count(*) FROM produits WHERE prix < 10")
    b.execute("COMMIT")
    return "se produit" if n1 != n2 else "empêchée"


def mise_a_jour_perdue(a, b, niveau):
    debut(a, niveau); debut(b, niveau)
    sa = un(a, "SELECT solde FROM comptes WHERE id = 1")
    sb = un(b, "SELECT solde FROM comptes WHERE id = 1")
    a.execute("UPDATE comptes SET solde = %s WHERE id = 1", (sa + 10,)); a.execute("COMMIT")
    try:
        b.execute("UPDATE comptes SET solde = %s WHERE id = 1", (sb + 20,)); b.execute("COMMIT")
    except errors.SerializationFailure:
        b.execute("ROLLBACK")
        return "empêchée (erreur 40001)"
    final = un(a, "SELECT solde FROM comptes WHERE id = 1"); a.execute("COMMIT")
    return "se produit" if final != sa + 30 else "empêchée"


def write_skew(a, b, niveau):
    debut(a, niveau); debut(b, niveau)
    na = un(a, "SELECT count(*) FROM gardes WHERE de_garde")
    nb = un(b, "SELECT count(*) FROM gardes WHERE de_garde")
    if na >= 2:
        a.execute("UPDATE gardes SET de_garde = false WHERE medecin = 'alice'")
    if nb >= 2:
        b.execute("UPDATE gardes SET de_garde = false WHERE medecin = 'bob'")
    try:
        a.execute("COMMIT")
        b.execute("COMMIT")
    except errors.SerializationFailure:
        a.execute("ROLLBACK"); b.execute("ROLLBACK")
        return "empêchée (erreur 40001)"
    reste = un(a, "SELECT count(*) FROM gardes WHERE de_garde"); a.execute("COMMIT")
    return "se produit" if reste == 0 else "empêchée"


def main():
    commun.lancer()
    try:
        a = psycopg.connect(commun.DSN, autocommit=True)
        b = psycopg.connect(commun.DSN, autocommit=True)
        scenarios = (("lecture sale", lecture_sale), ("lecture non répétable", non_repetable),
                     ("fantôme", fantome), ("mise à jour perdue", mise_a_jour_perdue),
                     ("write skew", write_skew))
        version = un(a, "SHOW server_version")
        print(f"PostgreSQL {version} : chaque scénario à chaque niveau d'isolation")
        print(f"{'anomalie':<23}" + "".join(f"{n:>14}" for n in ("READ UNCOMM.", "READ COMMIT.",
                                                                "REPEAT. READ", "SERIALIZABLE")))
        for nom, f in scenarios:
            cases = []
            for niveau in NIVEAUX:
                preparer(a)
                cases.append(f(a, b, niveau).replace("empêchée (erreur 40001)", "erreur 40001"))
            print(f"{nom:<23}" + "".join(f"{c:>14}" for c in cases))
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
