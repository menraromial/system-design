"""Chapitre 23 : ce qu'une seule machine PostgreSQL tient.

pgbench, l'outil de mesure fourni avec PostgreSQL 18, crée une base de facteur
100 (10 millions de comptes, environ 1,5 Go) puis mesure, pendant 30 secondes
chacune, deux charges avec 1, 8, 32 et 64 clients :
  - lecture seule (-S) : une lecture d'un compte par sa clé primaire ;
  - transaction de type TPC-B (par défaut) : trois mises à jour, une lecture et
    une insertion dans une transaction, validée de façon durable.
Le serveur et pgbench tournent sur le même portable (16 cœurs, 22 fils, SSD
NVMe) ; pgbench prend donc une partie du processeur.
Prérequis : docker, image postgres:18-alpine. Usage : python3 capacite.py
"""
import re
import subprocess
import time

NOM, ECHELLE, DUREE = "cours-sd-ch23-pg", 100, 30


def nombre(x):
    return f"{x:,.0f}".replace(",", " ")


def docker(*args, **kw):
    return subprocess.run(["docker", *args], capture_output=True, text=True, **kw)


def pgbench(*args):
    r = docker("exec", NOM, "pgbench", "-U", "postgres", *args, "cours")
    tps = float(re.search(r"tps = ([\d.]+)", r.stdout).group(1))
    lat = float(re.search(r"latency average = ([\d.]+) ms", r.stdout).group(1))
    return tps, lat


def main():
    docker("rm", "-f", NOM)
    docker("run", "-d", "--name", NOM, "--shm-size=2g", "-e", "POSTGRES_PASSWORD=cours",
           "-e", "POSTGRES_DB=cours", "postgres:18-alpine", "-c", "shared_buffers=2GB",
           "-c", "max_connections=200", "-c", "max_wal_size=8GB", check=True)
    try:
        for _ in range(60):
            if docker("exec", NOM, "pg_isready", "-U", "postgres").returncode == 0:
                break
            time.sleep(1)
        time.sleep(3)
        t = time.perf_counter()
        docker("exec", NOM, "pgbench", "-U", "postgres", "-i", "-s", str(ECHELLE), "-q", "cours", check=True)
        taille = docker("exec", NOM, "psql", "-U", "postgres", "-At", "-c",
                        "SELECT pg_size_pretty(pg_database_size('cours'))", "cours").stdout.strip()
        version = docker("exec", NOM, "pgbench", "--version").stdout.split()[-1]
        print(f"pgbench {version}, facteur {ECHELLE} (10 millions de comptes, {taille})")
        print(f"base créée en {time.perf_counter() - t:.0f} s ; {DUREE} s par mesure")
        print(f"   {'clients':>8}{'lecture seule':>20}{'TPC-B':>24}")
        print(f"   {'':>8}{'tps':>11}{'latence':>11}{'tps':>13}{'latence':>11}")
        for clients in (1, 8, 32, 64):
            fils = str(min(clients, 16))
            lec = pgbench("-S", "-c", str(clients), "-j", fils, "-T", str(DUREE))
            ecr = pgbench("-c", str(clients), "-j", fils, "-T", str(DUREE))
            print(f"   {clients:>8}{nombre(lec[0]):>11}{lec[1]:>8.2f} ms{nombre(ecr[0]):>13}{ecr[1]:>8.2f} ms")
    finally:
        docker("rm", "-f", NOM)


if __name__ == "__main__":
    main()
