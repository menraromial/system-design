"""Chapitre 22 : les conteneurs des expériences.

Chaque base tourne dans un conteneur nommé cours-sd-ch22-..., publié sur un port
local entre 10232 et 10292, et supprimé par son nom à la fin de chaque script.
"""
import subprocess
import time

CONTENEURS = {
    "timescale": ["-p", "10232:5432", "-e", "POSTGRES_PASSWORD=cours", "-e", "POSTGRES_DB=cours",
                  "--shm-size=1g", "timescale/timescaledb:latest-pg17",
                  "-c", "shared_buffers=512MB", "-c", "max_wal_size=4GB"],
    "pgvector": ["-p", "10233:5432", "-e", "POSTGRES_PASSWORD=cours", "-e", "POSTGRES_DB=cours",
                 "--shm-size=1g", "pgvector/pgvector:pg18",
                 "-c", "shared_buffers=512MB", "-c", "maintenance_work_mem=1GB"],
    "opensearch": ["-p", "10292:9200", "-e", "discovery.type=single-node",
                   "-e", "DISABLE_SECURITY_PLUGIN=true", "-e", "DISABLE_INSTALL_DEMO_CONFIG=true",
                   "-e", "OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g", "opensearchproject/opensearch:2"],
}


def dsn(base):
    port = {"timescale": 10232, "pgvector": 10233}[base]
    return f"host=127.0.0.1 port={port} dbname=cours user=postgres password=cours"


def nom(base):
    return f"cours-sd-ch22-{base}"


def lancer(base, pret, delai=180):
    """Démarre le conteneur, puis appelle pret() jusqu'à ce qu'il réussisse."""
    arreter(base)
    subprocess.run(["docker", "run", "-d", "--name", nom(base)] + CONTENEURS[base],
                   check=True, stdout=subprocess.DEVNULL)
    fin = time.monotonic() + delai
    while True:
        try:
            return pret()
        except Exception:
            if time.monotonic() > fin:
                raise
            time.sleep(1)


def arreter(*bases):
    for base in bases:
        subprocess.run(["docker", "rm", "-f", nom(base)], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)


def nombre(x):
    return f"{x:,}".replace(",", " ")


def chrono(f, n=1):
    """Durée médiane d'un appel à f, en millisecondes, sur n appels."""
    durees = []
    for _ in range(n):
        t = time.perf_counter()
        f()
        durees.append(1000 * (time.perf_counter() - t))
    durees.sort()
    return durees[len(durees) // 2]
