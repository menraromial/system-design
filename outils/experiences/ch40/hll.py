"""Chapitre 40 : HyperLogLog, compter les éléments distincts en mémoire fixe.

1. Un HyperLogLog en Python (Flajolet et al., 2007) : 2^p registres d'un octet,
   hachage de 64 bits (BLAKE2b) ; les p premiers bits choisissent le registre,
   qui garde la plus longue suite de zéros vue dans les bits restants, plus un.
   Correction des petites cardinalités par comptage linéaire.
   a. p = 14 (16 384 registres), de 100 à 10 millions d'éléments distincts ;
   b. erreur relative selon p, sur 20 flux de 100 000 éléments ;
   c. mémoire d'un set Python d'un million d'identifiants.
2. Redis 8.8 : PFADD de 10 millions d'éléments, PFCOUNT en chemin, mémoire de
   la clé (représentation creuse puis dense) ; sept HyperLogLog quotidiens d'un
   million d'utilisateurs chacun, tirés d'une population de 3 millions,
   réunis par PFMERGE et comparés à l'union exacte ; ensemble exact (SADD).

    python3 hll.py [1|2]      (sans argument : les deux parties)
"""
import hashlib
import math
import random
import statistics
import sys
import time

from commun import arreter_redis, demarrer_redis, mo, nb, par_lots


def h64(texte, sel=b""):
    return int.from_bytes(hashlib.blake2b(texte.encode(), digest_size=8,
                                          salt=sel.ljust(16, b"\0")[:16]).digest(), "big")


class HLL:
    def __init__(self, p):
        self.p, self.m = p, 1 << p
        self.reg = bytearray(self.m)
        self.alpha = 0.7213 / (1 + 1.079 / self.m)

    def ajouter(self, x):
        j = x >> (64 - self.p)
        reste = x & ((1 << (64 - self.p)) - 1)
        rho = (64 - self.p) - reste.bit_length() + 1
        if rho > self.reg[j]:
            self.reg[j] = rho

    def estimer(self):
        m = self.m
        brut = self.alpha * m * m / sum(2.0 ** -r for r in self.reg)
        vides = self.reg.count(0)
        if brut <= 2.5 * m and vides:
            return m * math.log(m / vides)          # comptage linéaire
        return brut


def partie_python():
    print("1. HyperLogLog en Python (hachage de 64 bits)")
    print("\n   a. p = 14 : 16 384 registres, 16 Ko ici (12 Ko à 6 bits par registre)")
    print(f"   {'distincts':>12}{'estimation':>13}{'erreur':>9}")
    h = HLL(14)
    reperes = {100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000}
    for i in range(1, 10_000_001):
        h.ajouter(h64(f"visiteur:{i}"))
        if i in reperes:
            e = h.estimer()
            print(f"   {nb(i, 12)}{nb(e, 13)}{100 * (e - i) / i:>8.2f}%")
    print("\n   b. erreur relative selon p, 20 flux de 100 000 éléments distincts")
    print(f"   {'p':>4}{'registres':>11}{'écart type':>12}{'1,04/√m':>10}{'pire':>8}")
    n, flux = 100_000, 20
    erreurs = {p: [] for p in range(8, 17, 2)}
    for f in range(flux):
        sel = f"flux{f}".encode()
        hs = {p: HLL(p) for p in erreurs}
        for i in range(n):
            x = h64(f"visiteur:{i}", sel)
            for hh in hs.values():
                hh.ajouter(x)
        for p, hh in hs.items():
            erreurs[p].append((hh.estimer() - n) / n)
    for p, e in erreurs.items():
        m = 1 << p
        print(f"   {p:>4}{nb(m, 11)}{100 * statistics.pstdev(e):>11.2f}%{100 * 1.04 / math.sqrt(m):>9.2f}%"
              f"{100 * max(abs(x) for x in e):>7.2f}%")
    ids = {f"visiteur:{i}" for i in range(1_000_000)}
    taille = sys.getsizeof(ids) + sum(sys.getsizeof(c) for c in ids)
    print(f"\n   c. set Python d'un million d'identifiants : {mo(taille):.1f} Mo")


def partie_redis():
    print("\n2. Redis 8.8")
    r = demarrer_redis()
    try:
        print(f"   {'PFADD':>12}{'PFCOUNT':>13}{'erreur':>9}{'mémoire':>11}")
        reperes = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]
        fait = 0
        t = time.perf_counter()
        for cible in reperes:
            p = r.pipeline(transaction=False)
            for lot in par_lots((f"visiteur:{i}" for i in range(fait, cible)), 10_000):
                p.pfadd("hll", *lot)
            p.execute()
            fait = cible
            c = r.pfcount("hll")
            m = r.memory_usage("hll", samples=0)
            print(f"   {nb(cible, 12)}{nb(c, 13)}{100 * (c - cible) / cible:>8.2f}%{nb(m, 9)} o")
        print(f"   ({time.perf_counter() - t:.0f} s pour 10 millions de PFADD par lots de 10 000)")

        alea = random.Random(40)
        jours, exact = [], set()
        for j in range(7):
            ids = alea.sample(range(3_000_000), 1_000_000)
            exact.update(ids)
            p = r.pipeline(transaction=False)
            for lot in par_lots((f"u:{i}" for i in ids), 10_000):
                p.pfadd(f"jour:{j}", *lot)
            p.execute()
            jours.append(f"jour:{j}")
        r.pfmerge("semaine", *jours)
        u = r.pfcount("semaine")
        print(f"\n   7 jours d'un million d'utilisateurs, population de 3 millions")
        print(f"   union exacte {nb(len(exact))}, PFMERGE puis PFCOUNT {nb(u)},"
              f" erreur {100 * (u - len(exact)) / len(exact):.2f} %")
        print(f"   un jour : PFCOUNT {nb(r.pfcount('jour:0'))} pour 1 000 000")
        p = r.pipeline(transaction=False)
        for lot in par_lots((f"visiteur:{i}" for i in range(1_000_000)), 10_000):
            p.sadd("ensemble", *lot)
        p.execute()
        m = r.memory_usage("ensemble", samples=0)
        print(f"   ensemble exact d'un million d'identifiants (SADD) : {mo(m):.1f} Mo")
        print(f"   HyperLogLog de 10 millions d'éléments : {nb(r.memory_usage('hll', samples=0))} o")
    finally:
        arreter_redis()


if __name__ == "__main__":
    parties = sys.argv[1] if len(sys.argv) > 1 else "12"
    if "1" in parties:
        partie_python()
    if "2" in parties:
        partie_redis()
