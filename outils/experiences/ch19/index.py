"""Chapitre 19 : ce que change un index, et lequel, sur 2 millions de commandes.

Table commandes (2 millions de lignes, 100 000 clients, deux ans de dates,
statut « livree » à 85 %, « expediee » à 10 %, « annulee » à 5 %). Chaque
requête est exécutée 30 fois (5 pour un parcours complet), avec un client tiré au
hasard ; on publie la médiane du temps d'exécution donné par EXPLAIN ANALYZE, le
type de plan choisi par PostgreSQL et le nombre de pages de 8 Kio lues.
  A. les commandes d'un client : sans index, puis avec un index sur client_id ;
  B. les 10 dernières commandes d'un client sur 90 jours : index (cree_le,
     client_id), puis (client_id, cree_le), puis (client_id, cree_le) INCLUDE
     (montant), un index couvrant ;
  C. le montant total des commandes d'un statut, avec un index sur statut :
     « annulee » (5 %) et « livree » (85 %).
Enfin, la hauteur de quelques index B-tree (extension pageinspect), et le plan
complet de la requête B pour le client 42 avec ses deux premiers index.
Prérequis : docker, image postgres:18-alpine. Usage : python3 index.py
"""
import random
import statistics

import psycopg

import commun

alea = random.Random(19)


def charger(cx):
    cx.execute("""
        CREATE TABLE commandes (
            id bigserial PRIMARY KEY, client_id int NOT NULL, cree_le timestamptz NOT NULL,
            statut text NOT NULL, montant numeric(10,2) NOT NULL);
        INSERT INTO commandes (client_id, cree_le, statut, montant)
        SELECT 1 + (random() * 99999)::int,
               now() - random() * interval '730 days',
               CASE WHEN r < 0.85 THEN 'livree' WHEN r < 0.95 THEN 'expediee' ELSE 'annulee' END,
               round((random() * 300)::numeric, 2)
        FROM (SELECT random() AS r FROM generate_series(1, 2000000)) s;
    """)
    cx.execute("VACUUM ANALYZE commandes")       # VACUUM : hors de toute transaction
    cx.execute("CREATE EXTENSION pageinspect")


def mesurer(cx, sql, args, n=30):
    durees, plan, pages = [], None, []
    for _ in range(n):
        res = cx.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, args() if args else ()).fetchone()[0]
        racine = res[0]
        durees.append(racine["Execution Time"])
        noeud = racine["Plan"]
        while noeud.get("Node Type") in ("Limit", "Aggregate", "Gather", "Finalize Aggregate",
                                         "Partial Aggregate", "Gather Merge", "Sort") and noeud.get("Plans"):
            noeud = noeud["Plans"][0]
        plan = noeud["Node Type"]
        p = racine["Plan"]
        pages.append(p.get("Shared Hit Blocks", 0) + p.get("Shared Read Blocks", 0))
    return statistics.median(durees), plan, int(statistics.median(pages))


def plan(cx, sql, args):
    return [l for l, in cx.execute("EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) " + sql, args).fetchall()]


def ligne(nom, res):
    t, plan, pages = res
    print(f"   {nom:<40}{plan:<19}{t:>9.3f} ms{pages:>9}")


def hauteur(cx, index):
    niveau, = cx.execute("SELECT level FROM bt_metap(%s)", (index,)).fetchone()
    taille, = cx.execute("SELECT pg_relation_size(%s)", (index,)).fetchone()
    return niveau + 1, taille / 2**20


def main():
    commun.lancer()
    try:
        cx = psycopg.connect(commun.DSN, autocommit=True)
        charger(cx)
        client = lambda: (alea.randint(1, 100000),)
        print("2 millions de commandes ; médiane du temps d'exécution (EXPLAIN ANALYZE)")
        print(f"   {'requête et index':<40}{'plan':<19}{'temps':>12}{'pages':>9}")

        print("A. WHERE client_id = ?")
        qa = "SELECT * FROM commandes WHERE client_id = %s"
        ligne("sans index", mesurer(cx, qa, client, n=5))
        cx.execute("CREATE INDEX i_client ON commandes (client_id)")
        ligne("index (client_id)", mesurer(cx, qa, client))

        print("B. WHERE client_id = ? AND 90 derniers jours, ORDER BY cree_le DESC LIMIT 10")
        qb = """SELECT cree_le, montant FROM commandes WHERE client_id = %s
                AND cree_le >= now() - interval '90 days' ORDER BY cree_le DESC LIMIT 10"""
        cx.execute("DROP INDEX i_client")
        cx.execute("CREATE INDEX i_date_client ON commandes (cree_le, client_id)")
        ligne("index (cree_le, client_id)", mesurer(cx, qb, client))
        plans = [("index (cree_le, client_id)", plan(cx, qb, (42,)))]
        cx.execute("DROP INDEX i_date_client")
        cx.execute("CREATE INDEX i_client_date ON commandes (client_id, cree_le)")
        ligne("index (client_id, cree_le)", mesurer(cx, qb, client))
        plans.append(("index (client_id, cree_le)", plan(cx, qb, (42,))))
        cx.execute("CREATE INDEX i_couvrant ON commandes (client_id, cree_le) INCLUDE (montant)")
        cx.execute("DROP INDEX i_client_date")
        cx.execute("VACUUM commandes")
        ligne("(client_id, cree_le) INCLUDE (montant)", mesurer(cx, qb, client))

        print("C. SELECT sum(montant) WHERE statut = ?, index (statut)")
        cx.execute("CREATE INDEX i_statut ON commandes (statut)")
        cx.execute("ANALYZE commandes")
        qc = "SELECT sum(montant) FROM commandes WHERE statut = %s"
        ligne("statut = 'annulee' (5 %)", mesurer(cx, qc, lambda: ("annulee",), n=5))
        ligne("statut = 'livree' (85 %)", mesurer(cx, qc, lambda: ("livree",), n=5))

        print("hauteur des index B-tree (niveaux, racine comprise) et taille")
        for idx in ("commandes_pkey", "i_couvrant", "i_statut"):
            h, mo = hauteur(cx, idx)
            print(f"   {idx:<40}{h:>3} niveaux{mo:>10.1f} Mo")
        taille, = cx.execute("SELECT pg_relation_size('commandes')").fetchone()
        print(f"   {'table commandes':<40}{'':>11}{taille / 2**20:>10.1f} Mo")

        for nom, lignes in plans:
            print(f"\nplan de la requête B, client 42, {nom}")
            for l in lignes:
                print("   " + l)
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
