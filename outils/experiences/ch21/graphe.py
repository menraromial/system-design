"""Chapitre 21 : traverser un graphe, dans Neo4j et dans PostgreSQL.

Un réseau de 100 000 personnes, chacune liée à 10 autres tirées au hasard, soit
environ un million d'amitiés (deux millions de liens orientés dans PostgreSQL,
un million de relations non orientées dans Neo4j).
  1. combien de personnes sont à au plus k sauts d'une personne donnée, pour
     k = 1 à 4 : requête récursive (WITH RECURSIVE) dans PostgreSQL, motif de
     longueur variable dans Neo4j ; médiane sur 10 personnes tirées au hasard ;
  2. le plus court chemin entre deux personnes : shortestPath dans Neo4j,
     parcours en largeur dans PostgreSQL, une requête par niveau (une requête
     récursive ne peut pas s'arrêter dès que la cible est trouvée) ; médiane
     sur 10 couples.
Prérequis : docker, images postgres:18-alpine et neo4j:5, pip install -r
requirements.txt. Usage : python3 graphe.py
"""
import random
import statistics
import time

import psycopg
from neo4j import GraphDatabase

import commun

N, AMIS = 100_000, 10


def aretes():
    alea = random.Random(21)
    paires = set()
    for a in range(N):
        for b in alea.sample(range(N), AMIS):
            if a != b:
                paires.add((min(a, b), max(a, b)))
    return sorted(paires)


def charger_pg(paires):
    cx = commun.lancer("pg", lambda: psycopg.connect(commun.PG, autocommit=True))
    cx.execute("CREATE TABLE amis (a int, b int, PRIMARY KEY (a, b))")
    with cx.cursor() as cur, cur.copy("COPY amis FROM STDIN") as cp:
        for a, b in paires:
            cp.write_row((a, b))
            cp.write_row((b, a))
    cx.execute("VACUUM ANALYZE amis")
    return cx


def charger_neo4j(paires):
    pilote = commun.lancer("neo4j", lambda: (lambda d: d.verify_connectivity() or d)(
        GraphDatabase.driver("bolt://127.0.0.1:10187", auth=("neo4j", "cours-ch21"))), delai=240)
    with pilote.session() as s:
        s.run("CREATE CONSTRAINT personne_id FOR (p:Personne) REQUIRE p.id IS UNIQUE").consume()
        for i in range(0, N, 10_000):
            s.run("UNWIND range($a, $b) AS i CREATE (:Personne {id: i})", a=i, b=min(i + 10_000, N) - 1).consume()
        for i in range(0, len(paires), 10_000):
            lot = [{"a": a, "b": b} for a, b in paires[i:i + 10_000]]
            s.run("UNWIND $lot AS r MATCH (x:Personne {id: r.a}), (y:Personne {id: r.b}) "
                  "CREATE (x)-[:AMI]->(y)", lot=lot).consume()
    return pilote


def nombre(x):
    return f"{x:,}".replace(",", " ")


def mediane_ms(f, args):
    durees = []
    for a in args:
        t = time.perf_counter()
        resultat = f(a)
        durees.append(1000 * (time.perf_counter() - t))
    return statistics.median(durees), resultat


def main():
    paires = aretes()
    alea = random.Random(9)
    personnes = [alea.randrange(N) for _ in range(10)]
    couples = [(alea.randrange(N), alea.randrange(N)) for _ in range(10)]
    try:
        t = time.perf_counter()
        cx = charger_pg(paires)
        print(f"{nombre(N)} personnes, {nombre(len(paires))} amitiés ;"
              f" PostgreSQL chargé en {time.perf_counter() - t:.0f} s")
        t = time.perf_counter()
        pilote = charger_neo4j(paires)
        print(f"Neo4j chargé en {time.perf_counter() - t:.0f} s (démarrage compris)")

        print("\n1. personnes à au plus k sauts (médiane sur 10 personnes)")
        print(f"   {'k':<4}{'atteintes':>11}{'PostgreSQL':>14}{'Neo4j':>12}")
        q_pg = """WITH RECURSIVE r(id, d) AS (
                      SELECT %(p)s::int, 0
                      UNION
                      SELECT a.b, r.d + 1 FROM r JOIN amis a ON a.a = r.id WHERE r.d < %(k)s)
                  SELECT count(DISTINCT id) - 1 FROM r"""
        for k in range(1, 5):
            pg, n_pg = mediane_ms(lambda p: cx.execute(q_pg, {"p": p, "k": k}).fetchone()[0], personnes)
            with pilote.session() as s:
                q = f"MATCH (u:Personne {{id: $p}})-[:AMI*1..{k}]-(v) WHERE v <> u RETURN count(DISTINCT v)"
                neo, n_neo = mediane_ms(lambda p: s.run(q, p=p).single()[0], personnes)
            print(f"   {k:<4}{nombre(n_pg):>11}{pg:>11.1f} ms{neo:>9.1f} ms")
            if n_pg != n_neo:
                print(f"      (écart de décompte : PostgreSQL {n_pg}, Neo4j {n_neo})")

        print("\n2. plus court chemin entre deux personnes (médiane sur 10 couples)")
        def bfs_pg(c):
            """Parcours en largeur, un niveau par requête SQL, jusqu'à trouver la cible."""
            depart, cible = c
            vus, frontiere, d = {depart}, [depart], 0
            while frontiere and d < 6:
                d += 1
                suivants = [b for b, in cx.execute(
                    "SELECT DISTINCT b FROM amis WHERE a = ANY(%s)", (frontiere,)).fetchall()]
                if cible in suivants:
                    return d
                frontiere = [b for b in suivants if b not in vus]
                vus.update(frontiere)
            return None

        pg, d_pg = mediane_ms(bfs_pg, couples)
        with pilote.session() as s:
            q = ("MATCH (x:Personne {id: $a}), (y:Personne {id: $b}), "
                 "p = shortestPath((x)-[:AMI*..6]-(y)) RETURN length(p)")
            neo, d_neo = mediane_ms(lambda c: s.run(q, a=c[0], b=c[1]).single()[0], couples)
        print(f"   PostgreSQL {pg:8.1f} ms")
        print(f"   Neo4j      {neo:8.1f} ms   (dernier couple : {d_pg} et {d_neo} sauts)")
    finally:
        commun.arreter("pg", "neo4j")


if __name__ == "__main__":
    main()
