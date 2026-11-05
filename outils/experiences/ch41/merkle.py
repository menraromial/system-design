"""Chapitre 41 : comparer deux répliques d'un million de clés par arbre de Merkle.

Deux répliques A et B ont les mêmes 1 000 000 de clés, sauf d clés dont B a
une valeur différente (une écriture manquée). Chaque clé est rangée dans une
feuille selon un hachage de la clé seule (comme un intervalle de jetons) ; une
feuille contient le XOR des hachages (clé, valeur) de ses clés, un nœud
interne le hachage de ses deux enfants. La comparaison descend niveau par
niveau : A envoie les hachages des enfants des nœuds qui diffèrent, B répond
ceux qui diffèrent encore, un aller-retour par niveau. On compte les
allers-retours, les octets échangés (hachage de 16 octets, numéro de nœud de
4 octets), les feuilles différentes et les clés qu'elles contiennent.

Deux façons de finir : envoyer les hachages des clés des feuilles différentes
pour trouver les clés exactes (« clé par clé »), ou recopier toutes les lignes
des feuilles différentes (« feuille entière », comme la réparation de Cassandra).

    python3 merkle.py
"""
import hashlib
import random
import time

N = 1_000_000
HACHAGE, NUMERO, CLE, LIGNE = 16, 4, 12, 100      # octets


def nb(x):
    return f"{x:,.0f}".replace(",", " ")


def h(b):
    return hashlib.blake2b(b, digest_size=16).digest()


def prepare():
    cles = [f"cle:{i}".encode() for i in range(N)]
    place = [int.from_bytes(hashlib.blake2b(c, digest_size=8).digest(), "big") for c in cles]
    ha = [int.from_bytes(h(c + b"=v1"), "big") for c in cles]
    return cles, place, ha


def arbre(place, hk, prof):
    feuilles = [0] * (1 << prof)
    for p, x in zip(place, hk):
        feuilles[p >> (64 - prof)] ^= x
    niveaux = [[x.to_bytes(16, "big") for x in feuilles]]
    while len(niveaux[-1]) > 1:
        n = niveaux[-1]
        niveaux.append([h(n[i] + n[i + 1]) for i in range(0, len(n), 2)])
    return niveaux[::-1]                                  # racine en tête


def comparer(ta, tb):
    """Descente niveau par niveau ; renvoie allers-retours, octets, feuilles différentes."""
    octets, ar = 2 * HACHAGE, 1                          # échange des racines
    if ta[0][0] == tb[0][0]:
        return ar, octets, []
    diff = [0]
    for niveau in range(1, len(ta)):
        enfants = [e for i in diff for e in (2 * i, 2 * i + 1)]
        octets += len(enfants) * (HACHAGE + NUMERO)      # A envoie
        diff = [e for e in enfants if ta[niveau][e] != tb[niveau][e]]
        octets += len(diff) * NUMERO                     # B répond
        ar += 1
    return ar, octets, diff


def main():
    t = time.perf_counter()
    cles, place, ha = prepare()
    print(f"{nb(N)} clés ; lignes de {LIGNE} o, soit {N * LIGNE / 1e6:.0f} Mo par réplique")
    print(f"comparaison naïve : hachages de toutes les clés, {N * (CLE + HACHAGE) / 1e6:.0f} Mo, 1 aller-retour")
    alea = random.Random(41)
    arbres_a = {}
    for prof in (10, 15, 20):
        t0 = time.perf_counter()
        arbres_a[prof] = arbre(place, ha, prof)
        print(f"profondeur {prof} : {nb(1 << prof)} feuilles, {N / (1 << prof):.1f} clés/feuille,"
              f" {time.perf_counter() - t0:.1f} s (Python)")
    print(f"\n{'diff.':>7}{'prof.':>6}{'A-R':>5}{'arbre':>10}{'feuilles':>9}{'clés':>10}"
          f"{'clé par clé':>13}{'feuille entière':>17}")
    for d in (0, 1, 10, 100, 1_000, 10_000, 100_000):
        hb = list(ha)
        for i in alea.sample(range(N), d):
            hb[i] = int.from_bytes(h(cles[i] + b"=v2"), "big")
        for prof in (10, 15, 20):
            tb = arbre(place, hb, prof)
            ar, octets, diff = comparer(arbres_a[prof], tb)
            ens = set(diff)
            dans = sum(1 for p in place if (p >> (64 - prof)) in ens)
            par_cle = octets + dans * (CLE + HACHAGE) + d * LIGNE
            entiere = octets + dans * LIGNE
            print(f"{d:>7}{prof:>6}{ar:>5}{octets / 1e3:>8.1f}ko{len(diff):>9}{dans:>10}"
                  f"{par_cle / 1e6:>11.2f}Mo{entiere / 1e6:>15.2f}Mo")
    print(f"\n({time.perf_counter() - t:.0f} s au total)")


if __name__ == "__main__":
    main()
