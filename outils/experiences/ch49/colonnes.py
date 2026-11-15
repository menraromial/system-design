"""Chapitre 49 : stockage en lignes et en colonnes, OLTP et OLAP.

Une table de 10 millions de ventes (date, magasin, produit, client, quantité,
prix, montant, canal, commentaire) est écrite en CSV, en Parquet sans
compression, en Parquet Snappy et en Parquet Zstd (DuckDB), et chargée dans
PostgreSQL 18 (conteneur cours-sd-ch49-pg, port 12932), stockage en lignes avec
un index sur l'identifiant. Données dans ~/.cache/cours-sd-ch49 (pas dans /tmp).

  1. taille de chaque format ;
  2. requête analytique (chiffre d'affaires par mois, 2 colonnes sur 10) :
     DuckDB sur CSV et sur Parquet, PostgreSQL ; octets de colonnes à lire ;
  3. requête sélective (une semaine de ventes) : groupes de lignes sautés grâce
     aux minimums et maximums du pied de fichier, fichier trié par date ou non ;
  4. lecture et mise à jour d'une vente par son identifiant (OLTP).

    python3 colonnes.py          (environ 6 minutes)
"""
import statistics
import subprocess
import time
from pathlib import Path

import duckdb
import psycopg
import pyarrow.parquet as pq

DOSSIER = Path.home() / ".cache" / "cours-sd-ch49"
PORT = 12932
N = 10_000_000


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def docker(*a, check=True):
    return subprocess.run(["docker", *a], capture_output=True, text=True, check=check)


def chrono(f, n=3):
    """Médiane de n exécutions, en secondes, et dernier résultat."""
    d, r = [], None
    for _ in range(n):
        t = time.perf_counter()
        r = f()
        d.append(time.perf_counter() - t)
    return statistics.median(d), r


def generer(con):
    con.execute("select setseed(0.49)")
    con.execute(f"""create table ventes as
      with b as (
        select i as id,
               date '2024-01-01' + cast(floor(i * 730.0 / {N}) as integer) as jour,
               cast(floor(random() * 500) as integer) as magasin,
               cast(floor(pow(random(), 3) * 50000) as integer) as produit,
               cast(floor(random() * 5000000) as integer) as client,
               1 + cast(floor(random() * 5) as integer) as quantite,
               cast(round(1 + random() * 199, 2) as decimal(8, 2)) as prix,
               (['magasin', 'web', 'appli'])[1 + cast(floor(random() * 3) as integer)] as canal,
               md5(cast(i as varchar)) as commentaire
        from range({N}) t(i))
      select id, jour, magasin, produit, client, quantite, prix,
             cast(quantite * prix as decimal(10, 2)) as montant, canal, commentaire
      from b""")


def main():
    DOSSIER.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("set memory_limit = '2GB'; set threads = 8")
    generer(con)
    fichiers = {"CSV": DOSSIER / "ventes.csv", "Parquet brut": DOSSIER / "ventes-brut.parquet",
                "Parquet Snappy": DOSSIER / "ventes-snappy.parquet",
                "Parquet Zstd": DOSSIER / "ventes-zstd.parquet",
                "Parquet Zstd, non trié": DOSSIER / "ventes-melange.parquet"}
    con.execute(f"copy ventes to '{fichiers['CSV']}' (header)")
    for nom, comp in (("Parquet brut", "uncompressed"), ("Parquet Snappy", "snappy"), ("Parquet Zstd", "zstd")):
        con.execute(f"copy ventes to '{fichiers[nom]}' (format parquet, compression {comp}, row_group_size 122880)")
    con.execute(f"copy (select * from ventes order by hash(id)) to '{fichiers['Parquet Zstd, non trié']}'"
                " (format parquet, compression zstd, row_group_size 122880)")

    # PostgreSQL
    docker("rm", "-f", "-v", "cours-sd-ch49-pg", check=False)
    docker("run", "-d", "--name", "cours-sd-ch49-pg", "-p", f"{PORT}:5432", "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
           "--shm-size=1g", "postgres:18-alpine", "-c", "shared_buffers=1GB", "-c", "max_wal_size=8GB")
    for _ in range(120):
        try:
            pg = psycopg.connect(f"host=127.0.0.1 port={PORT} user=postgres", autocommit=True)
            break
        except psycopg.OperationalError:
            time.sleep(0.5)
    pg.execute("""create table ventes (id bigint primary key, jour date, magasin int, produit int, client int,
                  quantite int, prix numeric(8,2), montant numeric(10,2), canal text, commentaire text)""")
    t = time.perf_counter()
    with pg.cursor().copy("copy ventes from stdin (format csv, header)") as cp, open(fichiers["CSV"], "rb") as f:
        while bloc := f.read(1 << 20):
            cp.write(bloc)
    pg.execute("vacuum analyze ventes")
    charge = time.perf_counter() - t
    taille_pg = pg.execute("select pg_total_relation_size('ventes')").fetchone()[0]

    print(f"1. {nb(N)} ventes, 10 colonnes")
    print(f"   {'format':<26}{'taille':>11}")
    for nom, f in fichiers.items():
        if "non trié" not in nom:
            print(f"   {nom:<26}{f.stat().st_size / 1e6:>8.0f} Mo")
    print(f"   {'PostgreSQL (table + index)':<26}{taille_pg / 1e6:>8.0f} Mo   (chargée en {charge:.0f} s)")

    meta = pq.ParquetFile(fichiers["Parquet Zstd"]).metadata
    cols = {meta.schema.column(j).name: j for j in range(meta.num_columns)}
    utiles = sum(meta.row_group(g).column(cols[c]).total_compressed_size
                 for g in range(meta.num_row_groups) for c in ("jour", "montant"))
    print("\n2. chiffre d'affaires par mois (jour, montant), médiane de 3")
    print(f"   {'moteur et format':<34}{'durée':>9}")
    req = "select date_trunc('month', jour) m, sum(montant) from {} group by m order by m"
    cas = [("DuckDB, CSV", lambda: con.execute(req.format(f"read_csv('{fichiers['CSV']}')")).fetchall()),
           ("DuckDB, Parquet Zstd", lambda: con.execute(req.format(f"'{fichiers['Parquet Zstd']}'")).fetchall()),
           ("PostgreSQL, lignes", lambda: pg.execute(req.format("ventes")).fetchall())]
    for nom, f in cas:
        d, r = chrono(f)
        print(f"   {nom:<34}{1000 * d:>7.0f} ms")
    print(f"   colonnes jour et montant dans le Parquet Zstd : {utiles / 1e6:.0f} Mo sur"
          f" {fichiers['Parquet Zstd'].stat().st_size / 1e6:.0f} Mo")

    print("\n3. ventes d'une semaine (jour entre le 3 et le 9 mars 2025), médiane de 3")
    print(f"   {'fichier':<26}{'groupes lus':>13}{'durée':>9}")
    sel = "select count(*), sum(montant) from '{}' where jour between date '2025-03-03' and date '2025-03-09'"
    for nom in ("Parquet Zstd", "Parquet Zstd, non trié"):
        m = pq.ParquetFile(fichiers[nom]).metadata
        j = {m.schema.column(k).name: k for k in range(m.num_columns)}["jour"]
        lus = sum(1 for g in range(m.num_row_groups)
                  if str(m.row_group(g).column(j).statistics.min) <= "2025-03-09"
                  and str(m.row_group(g).column(j).statistics.max) >= "2025-03-03")
        d, r = chrono(lambda: con.execute(sel.format(fichiers[nom])).fetchall())
        print(f"   {nom:<26}{f'{lus} / {m.num_row_groups}':>13}{1000 * d:>7.0f} ms")
    d, _ = chrono(lambda: pg.execute(sel.format("x").replace("'x'", "ventes")).fetchall())
    print(f"   {'PostgreSQL, sans index':<26}{'':>13}{1000 * d:>7.0f} ms")

    print("\n4. une vente par son identifiant (médiane de 200 tirages, ms)")
    ids = [(k * 7919) % N for k in range(200)]
    def mesure(f):
        d = []
        for i in ids:
            t = time.perf_counter()
            f(i)
            d.append((time.perf_counter() - t) * 1000)
        return statistics.median(d)
    pq_lit = mesure(lambda i: con.execute(f"select * from '{fichiers['Parquet Zstd']}' where id = {i}").fetchall())
    pg_lit = mesure(lambda i: pg.execute("select * from ventes where id = %s", (i,)).fetchall())
    pg_maj = mesure(lambda i: pg.execute("update ventes set quantite = quantite + 1 where id = %s", (i,)))
    print(f"   {'lecture, DuckDB sur Parquet':<34}{pq_lit:>8.2f}")
    print(f"   {'lecture, PostgreSQL (index)':<34}{pg_lit:>8.2f}")
    print(f"   {'mise à jour, PostgreSQL':<34}{pg_maj:>8.2f}")
    print("   mise à jour dans un Parquet : réécrire le fichier (voir tables.py)")
    pg.close()
    docker("rm", "-f", "-v", "cours-sd-ch49-pg", check=False)
    for f in fichiers.values():
        f.unlink()


if __name__ == "__main__":
    main()
