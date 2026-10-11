"""Chapitre 10 : loi d'Amdahl et contention, mesurées sur de vrais cœurs.

Un travail de calcul fixe (environ 3 s sur un cœur) est réparti entre P
processus, P = 1 à 6, chacun épinglé sur un cœur de performance différent.
  - « parallèle » : tout le travail est parallélisable ;
  - « 10 % séquentiel » : 10 % du travail est fait seul, avant la partie parallèle ;
  - « verrou partagé » : chaque petit morceau de travail se termine par une section
    critique protégée par un verrou commun à tous les processus.
Usage : python3 amdahl.py
"""
import multiprocessing as mp
import os
import time

COEURS = [0, 1, 3, 6, 8, 10]   # un fil par cœur physique de performance
UNITES = 3_000                  # unités de travail en tout
TAILLE = 20_000                 # itérations par unité (≈ 1 ms sur un cœur de performance)


def travail(n):
    x = 0
    for i in range(n):
        x += i * i % 7
    return x


def ouvrier(rang, unites, verrou, compteur):
    os.sched_setaffinity(0, {COEURS[rang]})
    for _ in range(unites):
        travail(TAILLE)
        if verrou is not None:
            with verrou:                  # section critique : un seul processus à la fois
                compteur.value += 1
                travail(TAILLE // 5)      # 20 % de l'unité passée sous le verrou


def executer(p, sequentiel, avec_verrou):
    os.sched_setaffinity(0, {COEURS[0]})
    debut = time.perf_counter()
    travail(int(TAILLE * UNITES * sequentiel))          # partie séquentielle
    reste = int(UNITES * (1 - sequentiel))
    verrou = mp.Lock() if avec_verrou else None
    compteur = mp.Value("i", 0, lock=False)
    procs = [mp.Process(target=ouvrier, args=(r, reste // p, verrou, compteur)) for r in range(p)]
    for pr in procs:
        pr.start()
    for pr in procs:
        pr.join()
    return time.perf_counter() - debut


def main():
    print(f"{'cœurs':>5} {'parallèle':>16} {'10 % séquentiel':>22} {'Amdahl 10 %':>11} {'verrou partagé':>20}")
    base = {}
    for p in range(1, len(COEURS) + 1):
        lignes = []
        for cle, seq, verrou in (("par", 0.0, False), ("seq", 0.1, False), ("ver", 0.0, True)):
            t = executer(p, seq, verrou)
            base.setdefault(cle, t)
            lignes.append((t, base[cle] / t))
        amdahl = 1 / (0.1 + 0.9 / p)
        (t1, a1), (t2, a2), (t3, a3) = lignes
        print(f"{p:5d} {t1:7.2f} s ×{a1:4.2f}  {t2:9.2f} s ×{a2:4.2f}    ×{amdahl:5.2f}   "
              f"{t3:9.2f} s ×{a3:4.2f}")


if __name__ == "__main__":
    main()
