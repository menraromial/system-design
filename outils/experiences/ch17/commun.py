"""Chapitre 17 : conteneurs PostgreSQL et Redis des expériences.

« cours-sd-ch17-pg » (port 9732) et « cours-sd-ch17-redis » (port 9779), en
réseau hôte, supprimés par leur nom à la fin de chaque script.
"""
import subprocess
import time

import psycopg
import redis

PG, REDIS = 9732, 9779
DSN = f"host=127.0.0.1 port={PG} dbname=cours user=postgres password=cours"
CONTENEURS = ("cours-sd-ch17-pg", "cours-sd-ch17-redis")


def lancer():
    arreter()
    subprocess.run(["docker", "run", "-d", "--network", "host", "--name", CONTENEURS[0],
                    "-e", "POSTGRES_PASSWORD=cours", "-e", "POSTGRES_DB=cours",
                    "postgres:18-alpine", "-c", f"port={PG}", "-c", "max_connections=200"],
                   check=True, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "-d", "--network", "host", "--name", CONTENEURS[1],
                    "redis:8.8-alpine", "redis-server", "--port", str(REDIS), "--save", "",
                    "--appendonly", "no"], check=True, stdout=subprocess.DEVNULL)
    fin = time.monotonic() + 60
    while time.monotonic() < fin:
        try:
            psycopg.connect(DSN).close()
            redis.Redis(port=REDIS).ping()
            return
        except (psycopg.OperationalError, redis.ConnectionError):
            time.sleep(0.5)
    raise RuntimeError("PostgreSQL ou Redis ne répond pas")


def arreter():
    for nom in CONTENEURS:
        subprocess.run(["docker", "rm", "-f", nom], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
