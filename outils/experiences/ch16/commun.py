"""Chapitre 16 : lancement des conteneurs PostgreSQL et Redis des expériences.

Les conteneurs « cours-sd-ch16-pg » (port 9632) et « cours-sd-ch16-redis »
(port 9679) sont en réseau hôte, pour éviter la traduction d'adresses de Docker,
et sont supprimés par leur nom à la fin de chaque script.
"""
import subprocess
import time

import psycopg
import redis

PG, REDIS = 9632, 9679
CONTENEURS = ("cours-sd-ch16-pg", "cours-sd-ch16-redis")


def lancer(redis_args=()):
    arreter()
    subprocess.run(["docker", "run", "-d", "--network", "host", "--name", CONTENEURS[0],
                    "-e", "POSTGRES_PASSWORD=cours", "-e", "POSTGRES_DB=cours",
                    "postgres:18-alpine", "-c", f"port={PG}"], check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "-d", "--network", "host", "--name", CONTENEURS[1],
                    "redis:8.8-alpine", "redis-server", "--port", str(REDIS), "--save", "",
                    "--appendonly", "no", *redis_args], check=True, stdout=subprocess.DEVNULL)
    fin = time.monotonic() + 60
    while time.monotonic() < fin:
        try:
            cx = psycopg.connect(f"host=127.0.0.1 port={PG} dbname=cours user=postgres password=cours",
                                 autocommit=True)
            r = redis.Redis(port=REDIS)
            r.ping()
            return cx, r
        except (psycopg.OperationalError, redis.ConnectionError):
            time.sleep(0.5)
    raise RuntimeError("PostgreSQL ou Redis ne répond pas")


def arreter():
    for nom in CONTENEURS:
        subprocess.run(["docker", "rm", "-f", nom], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def catalogue(cx, produits=100_000, avis_par_produit=20):
    """100 000 produits en 10 catégories, 20 avis chacun (2 millions d'avis)."""
    cx.execute("""
        CREATE TABLE produits (id int PRIMARY KEY, nom text, prix numeric, categorie int);
        CREATE TABLE avis (id bigserial PRIMARY KEY, produit_id int, note int, texte text);
    """)
    cx.execute("""INSERT INTO produits SELECT i, 'produit ' || i, (i %% 500) + 0.99, i %% 10
                  FROM generate_series(1, %s) i""", (produits,))
    cx.execute("""INSERT INTO avis (produit_id, note, texte)
                  SELECT p, 1 + (p * 7 + k) %% 5, repeat('x', 200)
                  FROM generate_series(1, %s) p, generate_series(1, %s) k""",
               (produits, avis_par_produit))
    cx.execute("CREATE INDEX ON avis (produit_id); ANALYZE;")
