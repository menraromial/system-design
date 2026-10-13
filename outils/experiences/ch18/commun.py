"""Chapitre 18 : le conteneur PostgreSQL des expériences.

« cours-sd-ch18-pg » (port 9832), en réseau hôte, supprimé par son nom à la fin
de chaque script.
"""
import subprocess
import time

import psycopg

PG = 9832
DSN = f"host=127.0.0.1 port={PG} dbname=cours user=postgres password=cours"
NOM = "cours-sd-ch18-pg"


def lancer():
    arreter()
    subprocess.run(["docker", "run", "-d", "--network", "host", "--name", NOM,
                    "-e", "POSTGRES_PASSWORD=cours", "-e", "POSTGRES_DB=cours",
                    "postgres:18-alpine", "-c", f"port={PG}", "-c", "max_connections=200"],
                   check=True, stdout=subprocess.DEVNULL)
    fin = time.monotonic() + 60
    while time.monotonic() < fin:
        try:
            psycopg.connect(DSN).close()
            return
        except psycopg.OperationalError:
            time.sleep(0.5)
    raise RuntimeError("PostgreSQL ne répond pas")


def arreter():
    subprocess.run(["docker", "rm", "-f", NOM], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
