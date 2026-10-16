"""Chapitre 22 : une série temporelle dans PostgreSQL, puis dans TimescaleDB.

200 capteurs envoient une mesure toutes les 10 secondes pendant 7 jours
(température et humidité, au centième près, qui varient lentement), soit
12 096 000 lignes. Elles sont écrites dans une table PostgreSQL ordinaire, avec
un index (capteur, ts), et dans une hypertable TimescaleDB découpée en tranches
d'un jour, avec le même index.
  1. la place occupée : table ordinaire, hypertable, puis hypertable compressée
     (compression par colonnes, segmentée par capteur, ordonnée par date), et la
     même compressée avec des valeurs entières en centièmes au lieu de flottants ;
  2. deux requêtes : les dernières 24 heures d'un capteur, et la moyenne horaire
     de chaque capteur sur les 7 jours, sur la table ordinaire, sur l'hypertable
     compressée et sur un agrégat continu (sous-échantillonnage par heure) ;
  3. la rétention : supprimer le jour le plus ancien, par DELETE dans la table
     ordinaire, par drop_chunks dans l'hypertable ; durée et journal écrit.
Prérequis : docker, image timescale/timescaledb:latest-pg17, pip install -r
requirements.txt. Usage : python3 series.py
"""
import time

import psycopg

import commun

CAPTEURS, JOURS, PAS = 200, 7, 10

GENERER = f"""
    SELECT t.ts, c.capteur,
           round((20 + 5 * sin(2 * pi() * extract(epoch FROM t.ts) / 86400) + c.capteur * 0.01
                  + random() * 0.3)::numeric, 2)::float8,
           round((50 + 10 * cos(2 * pi() * extract(epoch FROM t.ts) / 86400) + random())::numeric, 2)::float8
    FROM generate_series('2026-01-01'::timestamptz, '2026-01-01'::timestamptz
                         + interval '{JOURS} days' - interval '{PAS} seconds', interval '{PAS} seconds') t(ts)
    CROSS JOIN generate_series(1, {CAPTEURS}) c(capteur)
"""


def wal(cx):
    return cx.execute("SELECT pg_current_wal_lsn()").fetchone()[0]


def wal_depuis(cx, avant):
    return cx.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), %s)", (avant,)).fetchone()[0] / 2**20


def mo(octets):
    return f"{octets / 2**20:,.0f} Mo".replace(",", " ")


def main():
    cx = commun.lancer("timescale", lambda: psycopg.connect(commun.dsn("timescale"), autocommit=True))
    try:
        cx.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        version, = cx.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").fetchone()
        n = CAPTEURS * JOURS * 86400 // PAS
        print(f"{commun.nombre(n)} mesures ({CAPTEURS} capteurs, une toutes les {PAS} s, {JOURS} jours)")
        print(f"TimescaleDB {version}")

        print("\n1. place occupée et durée du chargement")
        cx.execute("CREATE TABLE mesures_pg (ts timestamptz, capteur int, temperature float8, humidite float8)")
        t = time.perf_counter()
        cx.execute("INSERT INTO mesures_pg " + GENERER)
        cx.execute("CREATE INDEX ON mesures_pg (capteur, ts)")
        d = time.perf_counter() - t
        cx.execute("VACUUM ANALYZE mesures_pg")
        taille, = cx.execute("SELECT pg_total_relation_size('mesures_pg')").fetchone()
        print(f"   {'table PostgreSQL ordinaire':<34}{mo(taille):>10}{d:>8.0f} s")

        cx.execute("CREATE TABLE mesures (ts timestamptz NOT NULL, capteur int, temperature float8, humidite float8)")
        cx.execute("SELECT create_hypertable('mesures', by_range('ts', interval '1 day'))")
        cx.execute("CREATE INDEX ON mesures (capteur, ts)")
        t = time.perf_counter()
        cx.execute("INSERT INTO mesures SELECT * FROM mesures_pg")
        d = time.perf_counter() - t
        cx.execute("VACUUM ANALYZE mesures")
        taille, = cx.execute("SELECT hypertable_size('mesures')").fetchone()
        print(f"   {'hypertable, 7 tranches':<34}{mo(taille):>10}{d:>8.0f} s")

        cx.execute("""ALTER TABLE mesures SET (timescaledb.compress,
                      timescaledb.compress_segmentby = 'capteur', timescaledb.compress_orderby = 'ts')""")
        t = time.perf_counter()
        cx.execute("SELECT compress_chunk(c) FROM show_chunks('mesures') c")
        d = time.perf_counter() - t
        cx.execute("VACUUM ANALYZE mesures")
        compresse, = cx.execute("SELECT hypertable_size('mesures')").fetchone()
        print(f"   {'hypertable compressée':<34}{mo(compresse):>10}{d:>8.0f} s (compression)")
        cx.execute("""CREATE TABLE mesures_entiers (ts timestamptz NOT NULL, capteur int,
                      temperature int, humidite int)""")
        cx.execute("SELECT create_hypertable('mesures_entiers', by_range('ts', interval '1 day'))")
        cx.execute("""INSERT INTO mesures_entiers SELECT ts, capteur, round(temperature * 100),
                      round(humidite * 100) FROM mesures_pg""")
        cx.execute("""ALTER TABLE mesures_entiers SET (timescaledb.compress,
                      timescaledb.compress_segmentby = 'capteur', timescaledb.compress_orderby = 'ts')""")
        cx.execute("SELECT compress_chunk(c) FROM show_chunks('mesures_entiers') c")
        entiers, = cx.execute("SELECT hypertable_size('mesures_entiers')").fetchone()
        print(f"   {'compressée, entiers (centièmes)':<34}{mo(entiers):>10}")
        ordinaire, = cx.execute("SELECT pg_total_relation_size('mesures_pg')").fetchone()
        print(f"   octets par mesure : {ordinaire / n:.1f} (ordinaire), {compresse / n:.1f} (compressée),")
        print(f"   {entiers / n:.1f} (compressée, entiers)")

        cx.execute("""CREATE MATERIALIZED VIEW mesures_heure WITH (timescaledb.continuous) AS
                      SELECT time_bucket('1 hour', ts) AS heure, capteur,
                             avg(temperature) AS temperature, avg(humidite) AS humidite
                      FROM mesures GROUP BY 1, 2 WITH DATA""")
        lignes, = cx.execute("SELECT count(*) FROM mesures_heure").fetchone()

        print("\n2. requêtes, durée médiane")
        print(f"   {'':<34}{'24 h, 1 capteur':>16}{'horaire, 7 j':>16}")
        fin = "'2026-01-08'::timestamptz"
        q24 = f"SELECT ts, temperature FROM {{t}} WHERE capteur = 42 AND ts >= {fin} - interval '24 hours'"
        qh = ("SELECT time_bucket('1 hour', ts), capteur, avg(temperature) FROM {t} GROUP BY 1, 2")
        for nom, table in (("table PostgreSQL ordinaire", "mesures_pg"), ("hypertable compressée", "mesures")):
            a = commun.chrono(lambda: cx.execute(q24.format(t=table)).fetchall(), 20)
            b = commun.chrono(lambda: cx.execute(qh.format(t=table)).fetchall(), 3)
            print(f"   {nom:<34}{a:>13.1f} ms{b:>13.0f} ms")
        b = commun.chrono(lambda: cx.execute("SELECT heure, capteur, temperature FROM mesures_heure").fetchall(), 10)
        print(f"   {'agrégat continu (par heure)':<34}{'':>16}{b:>13.0f} ms")
        print(f"   ({commun.nombre(lignes)} lignes dans l'agrégat, contre {commun.nombre(n)} mesures)")

        print("\n3. rétention : supprimer le jour le plus ancien")
        avant = wal(cx)
        t = time.perf_counter()
        r = cx.execute("DELETE FROM mesures_pg WHERE ts < '2026-01-02'")
        d = time.perf_counter() - t
        print(f"   {'DELETE, table ordinaire':<26}{d * 1000:>8.0f} ms{wal_depuis(cx, avant):>8.0f} Mo de journal")
        print(f"      ({commun.nombre(r.rowcount)} lignes supprimées)")
        avant = wal(cx)
        t = time.perf_counter()
        r = cx.execute("SELECT drop_chunks('mesures', older_than => '2026-01-02'::timestamptz)").fetchall()
        d = time.perf_counter() - t
        print(f"   {'drop_chunks, hypertable':<26}{d * 1000:>8.0f} ms{wal_depuis(cx, avant):>8.1f} Mo de journal")
        print(f"      ({len(r)} tranche supprimée)")
    finally:
        commun.arreter("timescale")


if __name__ == "__main__":
    main()
