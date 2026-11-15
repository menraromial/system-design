"""Chapitre 49 : modélisation en étoile, schéma normalisé et table large, ETL et
ELT, dans DuckDB (en mémoire, 2 Go au plus).

Données : 10 millions de ventes, 730 jours, 500 magasins dans 100 villes et 12
régions, 50 000 produits de 2 000 marques et 40 catégories, 1 million de clients.

  1. trois schémas pour les mêmes faits : normalisé (le produit pointe vers sa
     marque et sa catégorie, le magasin vers sa ville, la ville vers sa région),
     en étoile (une table de faits et des dimensions dénormalisées), table large
     (tous les attributs recopiés dans chaque vente) ; taille en Parquet Zstd et
     durée d'une requête « chiffre d'affaires par catégorie et par région au
     premier trimestre 2025 » ;
  2. construire la table large en transformant les données avant de les charger
     (ETL, jointures dans pyarrow) ou après (ELT, jointures en SQL dans DuckDB).

    python3 etoile.py            (environ 2 minutes)
"""
import statistics
import time
from pathlib import Path

import duckdb
import pyarrow as pa

DOSSIER = Path.home() / ".cache" / "cours-sd-ch49" / "etoile"


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def mediane(f, n=5):
    d = []
    for _ in range(n):
        t = time.perf_counter()
        f()
        d.append(time.perf_counter() - t)
    return statistics.median(d)


def generer(con):
    con.execute("select setseed(0.49)")
    con.execute("create table region as select i as region_id, 'région ' || i as region from range(12) t(i)")
    con.execute("create table ville as select i as ville_id, 'ville ' || i as ville, i % 12 as region_id"
                " from range(100) t(i)")
    con.execute("create table magasin as select i as magasin_id, 'magasin ' || i as magasin, i % 100 as ville_id"
                " from range(500) t(i)")
    con.execute("create table categorie as select i as categorie_id, 'catégorie ' || i as categorie"
                " from range(40) t(i)")
    con.execute("create table marque as select i as marque_id, 'marque ' || i as marque from range(2000) t(i)")
    con.execute("create table produit as select i as produit_id, 'produit ' || i as produit,"
                " i % 2000 as marque_id, i % 40 as categorie_id from range(50000) t(i)")
    con.execute("create table client as select i as client_id, (['particulier', 'pro', 'association'])"
                "[1 + i % 3] as segment from range(1000000) t(i)")
    con.execute("""create table jour as select d as jour, year(d) as annee, quarter(d) as trimestre,
                   month(d) as mois, dayofweek(d) as jour_semaine
                   from (select date '2024-01-01' + cast(i as integer) as d from range(730) t(i))""")
    con.execute("""create table ventes as select i as vente_id,
                   date '2024-01-01' + cast(floor(i * 730.0 / 10000000) as integer) as jour,
                   cast(floor(random() * 500) as bigint) as magasin_id,
                   cast(floor(pow(random(), 3) * 50000) as bigint) as produit_id,
                   cast(floor(random() * 1000000) as bigint) as client_id,
                   1 + cast(floor(random() * 5) as integer) as quantite,
                   cast(round(random() * 500, 2) as decimal(10, 2)) as montant
                   from range(10000000) t(i)""")
    # étoile : dimensions dénormalisées
    con.execute("""create table dim_produit as select p.produit_id, p.produit, m.marque, c.categorie
                   from produit p join marque m using (marque_id) join categorie c using (categorie_id)""")
    con.execute("""create table dim_magasin as select s.magasin_id, s.magasin, v.ville, r.region
                   from magasin s join ville v using (ville_id) join region r using (region_id)""")


REQUETES = {
    "normalisé": """select c.categorie, r.region, sum(f.montant) from ventes f
        join jour d on f.jour = d.jour join produit p using (produit_id) join categorie c using (categorie_id)
        join magasin s using (magasin_id) join ville v using (ville_id) join region r using (region_id)
        where d.annee = 2025 and d.trimestre = 1 group by all""",
    "étoile": """select p.categorie, s.region, sum(f.montant) from ventes f
        join jour d on f.jour = d.jour join dim_produit p using (produit_id) join dim_magasin s using (magasin_id)
        where d.annee = 2025 and d.trimestre = 1 group by all""",
    "table large": """select categorie, region, sum(montant) from large
        where annee = 2025 and trimestre = 1 group by all""",
}
TABLES = {"normalisé": ["ventes", "jour", "produit", "marque", "categorie", "magasin", "ville", "region", "client"],
          "étoile": ["ventes", "jour", "dim_produit", "dim_magasin", "client"],
          "table large": ["large"]}
JOINTURES = {"normalisé": 6, "étoile": 3, "table large": 0}

ELT = """create or replace table large as
    select f.*, d.annee, d.trimestre, d.mois, p.produit, p.marque, p.categorie, s.magasin, s.ville, s.region,
           c.segment
    from ventes f join jour d on f.jour = d.jour join dim_produit p using (produit_id)
    join dim_magasin s using (magasin_id) join client c using (client_id)"""


def taille(con, tables):
    total = 0
    for t in tables:
        f = DOSSIER / f"{t}.parquet"
        con.execute(f"copy {t} to '{f}' (format parquet, compression zstd)")
        total += f.stat().st_size
        f.unlink()
    return total


def main():
    DOSSIER.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("set memory_limit = '2GB'; set threads = 8")
    generer(con)
    con.execute(ELT)
    print("1. chiffre d'affaires par catégorie et par région, premier trimestre 2025")
    print(f"   {'schéma':<14}{'jointures':>10}{'taille Zstd':>13}{'requête':>10}")
    for nom, req in REQUETES.items():
        d = mediane(lambda: con.execute(req).fetchall())
        n = len(con.execute(req).fetchall())
        print(f"   {nom:<14}{JOINTURES[nom]:>10}{taille(con, TABLES[nom]) / 1e6:>10.0f} Mo{1000 * d:>7.0f} ms")
    print(f"   ({n} lignes de résultat, médiane de 5)")

    print("\n2. construire la table large (10 millions de lignes, 5 jointures) :")
    print("   ETL : extraire, joindre hors de l'entrepôt, charger ; ELT : charger les")
    print("   données brutes, puis transformer dans l'entrepôt")
    # ETL : extraire par lots d'un million de lignes, joindre hors de l'entrepôt (pyarrow), charger
    con.execute("drop table large")                       # libérer la mémoire de la table déjà construite
    t = time.perf_counter()
    dims = {n: con.sql(f"select * from {n}").to_arrow_table()
            for n in ("jour", "dim_produit", "dim_magasin", "client")}
    lecteur = con.sql("select * from ventes").to_arrow_reader(1_000_000)
    premier = True
    for lot in lecteur:
        w = pa.Table.from_batches([lot]).join(dims["jour"], "jour").join(dims["dim_produit"], "produit_id") \
              .join(dims["dim_magasin"], "magasin_id").join(dims["client"], "client_id")
        con2 = con.cursor()
        con2.register("w", w)
        con2.execute("create table large_etl as select * from w" if premier else
                     "insert into large_etl select * from w")
        con2.unregister("w")
        premier = False
    etl = time.perf_counter() - t
    t = time.perf_counter()
    con.execute(ELT)
    elt = time.perf_counter() - t
    n1 = con.execute("select count(*) from large_etl").fetchone()[0]
    n2 = con.execute("select count(*) from large").fetchone()[0]
    print(f"   ETL, jointures dans pyarrow par lots : {etl:>5.1f} s, {nb(n1)} lignes")
    print(f"   ELT, jointures en SQL dans DuckDB    : {elt:>5.1f} s, {nb(n2)} lignes")
    DOSSIER.rmdir()


if __name__ == "__main__":
    main()
