"""Chapitre 45 : combinatoire du shuffle sharding.

Chaque locataire reçoit k serveurs tirés au hasard parmi N. Si un locataire
fait tomber ses k serveurs, un autre locataire perd tout seulement s'il a
exactement les mêmes k serveurs ; la loi hypergéométrique donne la part des
locataires qui partagent j serveurs avec lui. On compare au découpage en N/k
cellules fixes, où toute la cellule tombe. Un tirage de Monte-Carlo
(1 000 000 de locataires) vérifie la formule sur les petites configurations.

    python3 melange.py           (une dizaine de secondes)
"""
import random
from math import comb


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def pct(p):
    """Pourcentage lisible, en notation scientifique sous 0,01 %."""
    p *= 100
    return f"{p:.2f} %" if p >= 0.01 else f"{p:.1e} %"


def part_commune(n, k, j):
    """Probabilité qu'un groupe tiré au hasard partage exactement j serveurs avec un groupe donné."""
    return comb(k, j) * comb(n - k, k - j) / comb(n, k)


def monte_carlo(n, k, tirages, alea):
    cible = set(range(k))
    meme = au_moins_un = 0
    for _ in range(tirages):
        g = set(alea.sample(range(n), k))
        c = len(g & cible)
        meme += c == k
        au_moins_un += c >= 1
    return meme / tirages, au_moins_un / tirages


def main():
    alea = random.Random(45)
    print("un locataire fait tomber ses k serveurs : part des autres locataires")
    print(f"{'N':>6}{'k':>3}{'groupes':>18}{'cellule':>9}{'même groupe':>14}{'k-1 communs':>13}"
          f"{'≥ 1':>9}")
    for n, k in ((8, 2), (16, 4), (100, 5), (2048, 4)):
        g = comb(n, k)
        meme = 1 / g
        km1 = part_commune(n, k, k - 1)
        un = 1 - part_commune(n, k, 0)
        print(f"{nb(n):>6}{k:>3}{nb(g):>18}{pct(k / n):>9}{pct(meme):>14}{pct(km1):>13}"
              f"{pct(un):>9}")
    print("cellule : découpage fixe en N/k cellules de k serveurs ; même groupe :")
    print("shuffle sharding, les k serveurs en commun ; ≥ 1 : au moins un serveur")
    print("commun")
    print("\nvérification par tirage de 1 000 000 de locataires")
    print(f"{'N':>6}{'k':>3}{'même groupe':>14}{'formule':>10}{'≥ 1':>9}{'formule':>10}")
    for n, k in ((8, 2), (16, 4)):
        m, u = monte_carlo(n, k, 1_000_000, alea)
        print(f"{n:>6}{k:>3}{100 * m:>13.3f}%{100 / comb(n, k):>9.3f}%{100 * u:>8.2f}%"
              f"{100 * (1 - part_commune(n, k, 0)):>9.2f}%")


if __name__ == "__main__":
    main()
