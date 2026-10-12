"""Chapitre 16 : écrire dans un système qui a un cache.

Une table de 10 000 comptes (un million pour l'essai de panne) dans PostgreSQL,
et leur copie dans Redis. Trois stratégies d'écriture :
  - cache-aside : mettre à jour la base, puis supprimer la clé du cache ;
  - write-through : mettre à jour la base, puis écrire la nouvelle valeur dans le cache ;
  - write-back : écrire dans le cache et rendre la main ; un fil d'arrière-plan
    reporte les écritures dans la base par lots, toutes les 200 ms.
Mesures : durée d'une écriture vue par l'application (2 000 écritures), nombre
de transactions envoyées à la base ; puis panne : un processus qui écrit pendant
une seconde est tué (SIGKILL), et l'on compte les écritures confirmées à
l'application qui ne sont pas arrivées dans la base.
Prérequis : docker, images postgres:18-alpine et redis:8.8-alpine.
Usage : python3 ecriture.py   (pip install -r requirements.txt)
"""
import random
import signal
import statistics
import subprocess
import sys
import threading
import time

import psycopg
import redis

import commun

DSN = f"host=127.0.0.1 port={commun.PG} dbname=cours user=postgres password=cours"
N_COMPTES = 10_000


class Ecrivain:
    def __init__(self, strategie):
        self.strategie = strategie
        self.cx = psycopg.connect(DSN, autocommit=True)
        self.r = redis.Redis(port=commun.REDIS)
        self.transactions = 0
        self.tampon, self.verrou = {}, threading.Lock()
        if strategie == "write-back":
            threading.Thread(target=self.vider_en_boucle, daemon=True).start()

    def ecrire(self, compte, solde):
        if self.strategie == "cache-aside":
            self.cx.execute("UPDATE comptes SET solde = %s WHERE id = %s", (solde, compte), prepare=True)
            self.transactions += 1
            self.r.delete(f"compte:{compte}")
        elif self.strategie == "write-through":
            self.cx.execute("UPDATE comptes SET solde = %s WHERE id = %s", (solde, compte), prepare=True)
            self.transactions += 1
            self.r.set(f"compte:{compte}", solde)
        else:
            self.r.set(f"compte:{compte}", solde)
            with self.verrou:
                self.tampon[compte] = solde

    def vider_en_boucle(self):
        cx = psycopg.connect(DSN, autocommit=True)
        while True:
            time.sleep(0.2)
            with self.verrou:
                lot, self.tampon = self.tampon, {}
            if lot:
                with cx.transaction():
                    cx.cursor().executemany("UPDATE comptes SET solde = %s WHERE id = %s",
                                            [(s, c) for c, s in lot.items()])
                self.transactions += 1


def preparer(cx, n=N_COMPTES):
    cx.execute("DROP TABLE IF EXISTS comptes")
    cx.execute("CREATE TABLE comptes (id int PRIMARY KEY, solde bigint)")
    cx.execute("INSERT INTO comptes SELECT i, 0 FROM generate_series(1, %s) i", (n,))


def processus_ecrivain(strategie):
    """Écrit jusqu'à être tué ; signale chaque écriture confirmée sur la sortie standard."""
    e = Ecrivain(strategie)
    n = 0
    while True:
        n += 1
        e.ecrire(n, n)                         # compte n reçoit le solde n
        print(n, flush=True)


def panne(strategie):
    proc = subprocess.Popen([sys.executable, __file__, "ecrivain", strategie],
                            stdout=subprocess.PIPE, text=True)
    confirmees = 0
    fin = time.monotonic() + 1.0
    while time.monotonic() < fin:
        ligne = proc.stdout.readline()
        if ligne:
            confirmees = int(ligne)
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    for ligne in proc.stdout.read().split():
        confirmees = int(ligne)                # celles écrites dans le tube avant la mort
    return confirmees


def main():
    cx, _ = commun.lancer()
    try:
        alea = random.Random(16)
        print("2 000 écritures ; PostgreSQL et Redis sur la machine du cours")
        print(f"{'stratégie':<16}{'médiane':>11}{'99e centile':>14}{'transactions':>14}")
        for strategie in ("cache-aside", "write-through", "write-back"):
            preparer(cx)
            e = Ecrivain(strategie)
            durees = []
            for _ in range(2_000):
                t = time.perf_counter()
                e.ecrire(alea.randint(1, N_COMPTES), alea.randint(1, 10**6))
                durees.append((time.perf_counter() - t) * 1000)
            time.sleep(0.5)                    # laisser passer le dernier lot de write-back
            durees.sort()
            print(f"{strategie:<16}{statistics.median(durees):>8.3f} ms{durees[1979]:>11.3f} ms"
                  f"{e.transactions:>14}")
        print("\npanne : l'écrivain est tué au bout d'une seconde (SIGKILL)")
        print(f"{'stratégie':<16}{'confirmées':>12}{'dans la base':>14}{'perdues':>10}")
        for strategie in ("write-through", "write-back"):
            preparer(cx, 1_000_000)             # assez de comptes pour une seconde d'écritures
            confirmees = panne(strategie)
            dans_base = cx.execute("SELECT count(*) FROM comptes WHERE solde > 0").fetchone()[0]
            perdues = max(0, confirmees - dans_base)
            print(f"{strategie:<16}{confirmees:>12}{dans_base:>14}{perdues:>10}")
    finally:
        commun.arreter()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "ecrivain":
        processus_ecrivain(sys.argv[2])
    else:
        main()
