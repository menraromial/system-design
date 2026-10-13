"""Chapitre 20 : ce que coûte la durabilité d'une écriture.

On ajoute 5 000 enregistrements de 200 octets à la fin d'un fichier, comme un
journal d'écriture anticipée (WAL), de quatre façons :
  - sans fsync : l'écriture s'arrête dans le cache du système ;
  - fdatasync après chaque enregistrement : chacun est sur le disque avant qu'on
    réponde, comme une base qui confirme chaque transaction ;
  - fdatasync tous les 10, puis tous les 100 enregistrements : la validation
    groupée (group commit), qui partage une synchronisation entre plusieurs
    transactions ;
puis on réécrit 5 000 pages de 4 Kio à des endroits tirés au hasard d'un fichier
de 256 Mio, avec fdatasync après chacune, comme un moteur qui modifierait ses
pages en place sans journal.
Mesures : enregistrements par seconde, latence médiane et 99e centile d'un
enregistrement, attente de la synchronisation comprise.
Prérequis : Linux. Usage : python3 journal.py
"""
import os
import random
import statistics
import time

import commun

N, TAILLE = 5_000, 200


def ajouter(chemin, tous_les):
    fd = os.open(chemin, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    enregistrement = os.urandom(TAILLE - 1) + b"\n"
    latences, attente = [], []
    debut = time.perf_counter()
    for i in range(1, N + 1):
        t = time.perf_counter()
        os.write(fd, enregistrement)
        if not tous_les:
            latences.append(time.perf_counter() - t)
            continue
        attente.append(t)
        if i % tous_les == 0:
            os.fdatasync(fd)
            fin = time.perf_counter()
            latences += [fin - a for a in attente]      # chacun attend la synchronisation
            attente = []
    fin = time.perf_counter()
    latences += [fin - a for a in attente]
    os.close(fd)
    return N / (fin - debut), latences


def en_place(chemin, alea):
    pages = 256 * 2**20 // 4096
    with open(chemin, "wb") as f:
        f.truncate(pages * 4096)
    fd = os.open(chemin, os.O_WRONLY)
    page = os.urandom(4096)
    latences = []
    debut = time.perf_counter()
    for _ in range(N):
        t = time.perf_counter()
        os.pwrite(fd, page, alea.randrange(pages) * 4096)
        os.fdatasync(fd)
        latences.append(time.perf_counter() - t)
    duree = time.perf_counter() - debut
    os.close(fd)
    return N / duree, latences


def nombre(x):
    return f"{x:,.0f}".replace(",", " ")


def ligne(nom, debit, latences):
    lat = sorted(latences)
    print(f"{nom:<38}{nombre(debit):>12}{nombre(1e6 * statistics.median(lat)):>10} µs"
          f"{nombre(1e6 * lat[int(0.99 * len(lat))]):>8} µs")


def main():
    dossier = commun.preparer()
    try:
        print(f"{N} enregistrements de {TAILLE} octets ; latence d'un enregistrement")
        print(f"{'':<38}{'par seconde':>12}{'médiane':>13}{'p99':>11}")
        for nom, k in (("ajout, sans fsync", 0), ("ajout, fdatasync à chaque fois", 1),
                       ("ajout, fdatasync tous les 10", 10), ("ajout, fdatasync tous les 100", 100)):
            chemin = os.path.join(dossier, f"journal-{k}")
            ligne(nom, *ajouter(chemin, k))
            os.remove(chemin)
        ligne("page de 4 Kio en place, fdatasync", *en_place(os.path.join(dossier, "pages"), random.Random(20)))
    finally:
        commun.nettoyer()


if __name__ == "__main__":
    main()
