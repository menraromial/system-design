"""Chapitre 22 : chercher des mots dans 500 000 textes.

Les textes sont faits de mots inventés (des syllabes assemblées), tirés selon une
loi de Zipf sur un vocabulaire de 30 000 mots, comme les mots d'une langue : 40
mots par texte. Ils sont rangés dans PostgreSQL et dans OpenSearch.
  0. l'analyse d'une phrase française par l'analyseur « french » d'OpenSearch et
     par la configuration « french » de PostgreSQL : ce que devient le texte
     avant d'entrer dans l'index ;
  1. chargement : durée et place, table et index GIN sur un tsvector dans
     PostgreSQL, index inversé d'OpenSearch ;
  2. trois mots de fréquences différentes, puis deux mots ensemble :
     - PostgreSQL, ILIKE '%mot%' : parcours de tous les textes ;
     - PostgreSQL, recherche plein texte : index GIN, 10 meilleurs par ts_rank ;
     - OpenSearch, requête match : index inversé, 10 meilleurs par BM25 ;
     durée médiane sur 5 répétitions vue par le client (et, pour OpenSearch,
     annoncée par le serveur), et nombre de textes qui contiennent le mot.
Prérequis : docker, images pgvector/pgvector:pg18 (un PostgreSQL 18 suffit) et
opensearchproject/opensearch:2, pip install -r requirements.txt.
Usage : python3 recherche.py
"""
import random
import time

import psycopg
from opensearchpy import OpenSearch, helpers

import commun

TEXTES, MOTS_PAR_TEXTE, VOCABULAIRE = 500_000, 40, 30_000
SYLLABES = ["ba", "co", "di", "fu", "ga", "lo", "mi", "nu", "pa", "ro", "si", "tu", "ve", "za",
            "bri", "cla", "dro", "fle", "gri", "plu", "sta", "tre", "vo", "ka"]
PHRASE = "Les chevaux galopaient librement dans les grandes prairies normandes"


def vocabulaire():
    mots, vus, alea = [], set(), random.Random(1)
    while len(mots) < VOCABULAIRE:
        m = "".join(alea.choice(SYLLABES) for _ in range(alea.randint(2, 4)))
        if m not in vus:
            vus.add(m)
            mots.append(m)
    return mots


def textes(mots):
    alea = random.Random(22)
    cumul, total = [], 0.0
    for k in range(VOCABULAIRE):
        total += 1 / (k + 1)
        cumul.append(total)
    for i in range(TEXTES):
        yield i, " ".join(alea.choices(mots, cum_weights=cumul, k=MOTS_PAR_TEXTE))


def chercher(os_, corps):
    """Durée médiane vue par le client et durée médiane annoncée par le serveur (took)."""
    vus, serveur = [], []
    for _ in range(5):
        t = time.perf_counter()
        r = os_.search(index="textes", body={"size": 10, "query": {"match": {"corps": corps}}})
        vus.append(1000 * (time.perf_counter() - t))
        serveur.append(r["took"])
    return sorted(vus)[2], sorted(serveur)[2]


def main():
    mots = vocabulaire()
    cx = commun.lancer("pgvector", lambda: psycopg.connect(commun.dsn("pgvector"), autocommit=True))
    os_ = commun.lancer("opensearch", lambda: (lambda c: c.info() and c)(
        OpenSearch("http://127.0.0.1:10292", timeout=120)), delai=240)
    try:
        version = os_.info()["version"]["number"]
        print(f"{commun.nombre(TEXTES)} textes de {MOTS_PAR_TEXTE} mots, vocabulaire de"
              f" {commun.nombre(VOCABULAIRE)} mots (Zipf)")
        print(f"OpenSearch {version}")

        print("\n0. analyse de la phrase")
        print(f"   « {PHRASE} »")
        jetons = [j["token"] for j in os_.indices.analyze(body={"analyzer": "french", "text": PHRASE})["tokens"]]
        print("   OpenSearch (french) :")
        print("      " + " ".join(jetons))
        tsv, = cx.execute("SELECT to_tsvector('french', %s)::text", (PHRASE,)).fetchone()
        print("   PostgreSQL (french) :")
        print("      " + tsv)
        jeton = [j["token"] for j in os_.indices.analyze(body={"analyzer": "french", "text": "galoper"})["tokens"]]
        pg, = cx.execute("SELECT to_tsvector('french', 'galoper')::text").fetchone()
        print(f"   « galoper » : OpenSearch {' '.join(jeton)}, PostgreSQL {pg}")

        print("\n1. chargement")
        cx.execute("CREATE TABLE textes (id int PRIMARY KEY, corps text, tsv tsvector)")
        t = time.perf_counter()
        with cx.cursor() as cur, cur.copy("COPY textes (id, corps) FROM STDIN") as cp:
            for ligne in textes(mots):
                cp.write_row(ligne)
        cx.execute("UPDATE textes SET tsv = to_tsvector('simple', corps)")
        cx.execute("CREATE INDEX i_tsv ON textes USING gin (tsv)")
        cx.execute("VACUUM ANALYZE textes")
        d = time.perf_counter() - t
        table, index = cx.execute("SELECT pg_table_size('textes'), pg_relation_size('i_tsv')").fetchone()
        print(f"   PostgreSQL : {d:4.0f} s, table {table / 2**20:.0f} Mo, index GIN {index / 2**20:.0f} Mo")

        os_.indices.create(index="textes", body={"settings": {"number_of_shards": 1, "number_of_replicas": 0,
                                                        "refresh_interval": "-1"},
                                           "mappings": {"properties": {"corps": {"type": "text"}}}})
        t = time.perf_counter()
        helpers.bulk(os_, ({"_index": "textes", "_id": i, "corps": c} for i, c in textes(mots)),
                     chunk_size=5000, request_timeout=120)
        os_.indices.refresh(index="textes")
        os_.indices.forcemerge(index="textes", max_num_segments=1, request_timeout=600)
        d = time.perf_counter() - t
        taille = os_.indices.stats(index="textes")["_all"]["primaries"]["store"]["size_in_bytes"]
        print(f"   OpenSearch : {d:4.0f} s, index {taille / 2**20:.0f} Mo (textes et index inversé)")

        print("\n2. recherche, durée médiane sur 5")
        print(f"   {'mot (rang)':<22}{'textes':>9}{'ILIKE':>9}{'plein texte':>13}{'OpenSearch':>12}")
        print(f"   {'':<22}{'':>9}{'':>9}{'':>13}{'(serveur)':>12}")
        for rang in (10, 1000, 20000):
            mot = mots[rang - 1]
            n, = cx.execute("SELECT count(*) FROM textes WHERE tsv @@ to_tsquery('simple', %s)", (mot,)).fetchone()
            like = commun.chrono(lambda: cx.execute(
                "SELECT count(*) FROM textes WHERE corps ILIKE %s", (f"%{mot}%",)).fetchone(), 5)
            ft = commun.chrono(lambda: cx.execute(
                "SELECT id, ts_rank(tsv, q) r FROM textes, to_tsquery('simple', %s) q "
                "WHERE tsv @@ q ORDER BY r DESC LIMIT 10", (mot,)).fetchall(), 5)
            osr, took = chercher(os_, mot)
            print(f"   {mot + ' (' + str(rang) + ')':<22}{commun.nombre(n):>9}{like:>6.0f} ms"
                  f"{ft:>10.1f} ms{osr:>9.1f} ms")
            print(f"   {'':<22}{'':>9}{'':>9}{'':>13}{'(' + str(took) + ' ms)':>12}")
        a, b = mots[999], mots[1999]
        n, = cx.execute("SELECT count(*) FROM textes WHERE tsv @@ to_tsquery('simple', %s)", (f"{a} & {b}",)).fetchone()
        ft = commun.chrono(lambda: cx.execute(
            "SELECT id, ts_rank(tsv, q) r FROM textes, to_tsquery('simple', %s) q "
            "WHERE tsv @@ q ORDER BY r DESC LIMIT 10", (f"{a} & {b}",)).fetchall(), 5)
        osr, took = chercher(os_, {"query": f"{a} {b}", "operator": "and"})
        print(f"   {'deux mots (1000, 2000)':<22}{commun.nombre(n):>9}{'':>9}{ft:>10.1f} ms{osr:>9.1f} ms")
        print(f"   {'':<22}{'':>9}{'':>9}{'':>13}{'(' + str(took) + ' ms)':>12}")
    finally:
        commun.arreter("pgvector", "opensearch")


if __name__ == "__main__":
    main()
