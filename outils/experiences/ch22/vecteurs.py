"""Chapitre 22 : chercher les plus proches voisins, exactement ou approximativement.

200 000 vecteurs de dimension 128, tirés autour de 2 000 centres (des groupes de
points voisins, comme le sont des plongements de textes ou d'images), sont
rangés dans PostgreSQL avec l'extension pgvector. 200 requêtes, tirées de la
même façon, cherchent leurs 10 plus proches voisins (distance euclidienne).
La vérité de référence est calculée par force brute avec numpy.
  1. recherche exacte : parcours complet de la table (pas d'index) ;
  2. index HNSW (m = 16, ef_construction = 64) : durée de construction, taille,
     puis rappel@10 et latence pour ef_search = 10 à 400 ;
  3. index IVFFlat (lists = 450, environ racine de N) : construction, taille,
     puis rappel@10 et latence pour probes = 1 à 64.
Le rappel@10 est la part des 10 vrais plus proches voisins que la réponse contient.
Prérequis : docker, image pgvector/pgvector:pg18, pip install -r requirements.txt.
Usage : python3 vecteurs.py
"""
import statistics
import time

import numpy as np
import psycopg

import commun

N, DIM, CENTRES, REQUETES, K = 200_000, 128, 2_000, 200, 10
BRUIT = 0.6                    # écart type autour des centres, qui sont tirés avec un écart type de 1


def donnees():
    rng = np.random.default_rng(22)
    centres = rng.normal(0, 1, (CENTRES, DIM)).astype(np.float32)
    def tirer(n):
        return (centres[rng.integers(0, CENTRES, n)] + rng.normal(0, BRUIT, (n, DIM))).astype(np.float32)
    return tirer(N), tirer(REQUETES)


def verite(base, requetes):
    vrais = []
    for q in requetes:
        d = ((base - q) ** 2).sum(axis=1)
        vrais.append(set(np.argpartition(d, K)[:K].tolist()))
    return vrais


def texte(v):
    return "[" + ",".join(f"{x:.5f}" for x in v) + "]"


def mesurer(cx, requetes, vrais, reglage=None):
    durees, rappels = [], []
    for q, v in zip(requetes, vrais):
        t = time.perf_counter()
        ids = [i for i, in cx.execute(
            "SELECT id FROM vecteurs ORDER BY v <-> %s::vector LIMIT %s", (texte(q), K)).fetchall()]
        durees.append(1000 * (time.perf_counter() - t))
        rappels.append(len(v & set(ids)) / K)
    return statistics.mean(rappels), statistics.median(durees)


def mo(octets):
    return f"{octets / 2**20:.0f} Mo"


def main():
    base, requetes = donnees()
    vrais = verite(base, requetes)
    cx = commun.lancer("pgvector", lambda: psycopg.connect(commun.dsn("pgvector"), autocommit=True))
    try:
        cx.execute("CREATE EXTENSION vector")
        version, = cx.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'").fetchone()
        cx.execute(f"CREATE TABLE vecteurs (id int PRIMARY KEY, v vector({DIM}))")
        with cx.cursor() as cur, cur.copy("COPY vecteurs (id, v) FROM STDIN") as cp:
            for i, v in enumerate(base):
                cp.write_row((i, texte(v)))
        cx.execute("VACUUM ANALYZE vecteurs")
        table, = cx.execute("SELECT pg_relation_size('vecteurs')").fetchone()
        print(f"{commun.nombre(N)} vecteurs de dimension {DIM} ({mo(table)}), {REQUETES} requêtes,"
              f" {K} voisins")
        print(f"pgvector {version}")

        print(f"\n   {'méthode et réglage':<30}{'rappel@10':>10}{'latence':>12}")
        rappel, lat = mesurer(cx, requetes[:20], vrais[:20])
        print(f"   {'exacte (parcours complet)':<30}{rappel:>10.3f}{lat:>9.1f} ms   (20 requêtes)")

        t = time.perf_counter()
        cx.execute("CREATE INDEX i_hnsw ON vecteurs USING hnsw (v vector_l2_ops) WITH (m = 16, ef_construction = 64)")
        d = time.perf_counter() - t
        taille, = cx.execute("SELECT pg_relation_size('i_hnsw')").fetchone()
        print(f"\n   HNSW : construit en {d:.0f} s, {mo(taille)}")
        for ef in (10, 20, 40, 80, 160, 400):
            cx.execute(f"SET hnsw.ef_search = {ef}")
            rappel, lat = mesurer(cx, requetes, vrais)
            print(f"   {'ef_search = ' + str(ef):<30}{rappel:>10.3f}{lat:>9.2f} ms")
        cx.execute("DROP INDEX i_hnsw")

        t = time.perf_counter()
        cx.execute("CREATE INDEX i_ivf ON vecteurs USING ivfflat (v vector_l2_ops) WITH (lists = 450)")
        d = time.perf_counter() - t
        taille, = cx.execute("SELECT pg_relation_size('i_ivf')").fetchone()
        print(f"\n   IVFFlat : construit en {d:.0f} s, {mo(taille)}")
        for probes in (1, 4, 16, 64):
            cx.execute(f"SET ivfflat.probes = {probes}")
            rappel, lat = mesurer(cx, requetes, vrais)
            print(f"   {'probes = ' + str(probes):<30}{rappel:>10.3f}{lat:>9.2f} ms")
    finally:
        commun.arreter("pgvector")


if __name__ == "__main__":
    main()
