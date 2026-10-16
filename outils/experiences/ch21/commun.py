"""Chapitre 21 : les conteneurs des expériences.

Chaque base tourne dans un conteneur nommé cours-sd-ch21-..., publié sur un port
local entre 10117 et 10187, et supprimé par son nom à la fin de chaque script.
"""
import subprocess
import time

CONTENEURS = {
    "pg": (["-p", "10132:5432", "-e", "POSTGRES_PASSWORD=cours", "-e", "POSTGRES_DB=cours",
            "--shm-size=512m", "postgres:18-alpine", "-c", "shared_buffers=256MB"]),
    "redis": (["-p", "10179:6379", "redis:8.8-alpine"]),
    "mongo": (["-p", "10117:27017", "mongo:7", "--wiredTigerCacheSizeGB", "0.5"]),
    "cassandra": (["-p", "10142:9042", "-e", "MAX_HEAP_SIZE=1G", "-e", "HEAP_NEWSIZE=200M",
                   "cassandra:5"]),
    "neo4j": (["-p", "10187:7687", "-e", "NEO4J_AUTH=neo4j/cours-ch21",
               "-e", "NEO4J_server_memory_heap_max__size=1G",
               "-e", "NEO4J_server_memory_pagecache_size=512M", "neo4j:5"]),
}
PG = "host=127.0.0.1 port=10132 dbname=cours user=postgres password=cours"


def nom(base):
    return f"cours-sd-ch21-{base}"


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


def chrono(f, n=1):
    """Durée médiane d'un appel à f, en millisecondes, sur n appels."""
    durees = []
    for _ in range(n):
        t = time.perf_counter()
        f()
        durees.append(1000 * (time.perf_counter() - t))
    durees.sort()
    return durees[len(durees) // 2]
