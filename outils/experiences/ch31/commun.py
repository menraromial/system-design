"""Chapitre 31 : le conteneur PostgreSQL des expériences.

« cours-sd-ch31-pg » (port 11132), en réseau hôte, avec un cache de pages
réduit (shared_buffers = 128 Mo) pour que les index dépassent la mémoire de
PostgreSQL ; supprimé par son nom, avec son volume, à la fin de chaque script.
"""
import subprocess
import time

import psycopg

PG = 11132
DSN = f"host=127.0.0.1 port={PG} dbname=cours user=postgres password=cours"
NOM = "cours-sd-ch31-pg"


def attendre():
    fin = time.monotonic() + 60
    while time.monotonic() < fin:
        try:
            psycopg.connect(DSN).close()
            return
        except psycopg.OperationalError:
            time.sleep(0.3)
    raise RuntimeError("PostgreSQL ne répond pas")


def lancer():
    arreter()
    subprocess.run(["docker", "run", "-d", "--network", "host",
                    "--shm-size=512m", "--name", NOM,
                    "-e", "POSTGRES_PASSWORD=cours", "-e", "POSTGRES_DB=cours",
                    "postgres:18-alpine", "-c", f"port={PG}",
                    "-c", "shared_buffers=128MB"],
                   check=True, stdout=subprocess.DEVNULL)
    attendre()


def tuer_et_relancer():
    """Arrêt brutal (SIGKILL) puis redémarrage du même conteneur."""
    subprocess.run(["docker", "kill", "-s", "KILL", NOM], check=True,
                   stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "start", NOM], check=True,
                   stdout=subprocess.DEVNULL)
    attendre()


def arreter():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)
