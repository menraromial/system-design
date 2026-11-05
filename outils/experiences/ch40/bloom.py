"""Chapitre 40 : le filtre de Bloom, mesuré contre la théorie, puis dans Redis.

1. Un filtre de Bloom en Python : un tableau de m bits et k fonctions de
   hachage obtenues par double hachage (Kirsch et Mitzenmacher) à partir d'un
   BLAKE2b de 128 bits. On y insère 1 000 000 de clés « utilisateur:i », puis
   on interroge 1 000 000 de clés absentes et l'on compte les faux positifs.
   a. bits par clé de 4 à 20, avec le nombre de fonctions optimal ;
   b. 10 bits par clé, k de 1 à 10 ;
   c. un filtre dimensionné pour 1 million de clés à 1 %, rempli au-delà ;
   d. mémoire d'un ensemble Python (set) des mêmes clés.
2. Redis 8.8 : BF.RESERVE à 1 % et à 0,1 %, comparé à un ensemble Redis (SADD) ;
   filtre extensible (EXPANSION 2) et non extensible (NONSCALING) prévus pour
   100 000 clés et remplis d'un million ; filtre coucou (CF) et suppression.

    python3 bloom.py [1|2]      (sans argument : les deux parties)
"""
import math
import sys
import time

from commun import arreter_redis, demarrer_redis, hache128, mo, nb, par_lots

N = 1_000_000
PRESENTES = [f"utilisateur:{i}" for i in range(N)]
ABSENTES = [f"absent:{i}" for i in range(N)]


class Bloom:
    def __init__(self, m, k):
        self.m, self.k = m, k
        self.bits = bytearray((m + 7) // 8)

    def ajouter(self, h1, h2):
        m, bits = self.m, self.bits
        for i in range(self.k):
            p = (h1 + i * h2) % m
            bits[p >> 3] |= 1 << (p & 7)

    def contient(self, h1, h2):
        m, bits = self.m, self.bits
        for i in range(self.k):
            p = (h1 + i * h2) % m
            if not bits[p >> 3] & (1 << (p & 7)):
                return False
        return True


def theorie(bits_par_cle, k):
    return (1 - math.exp(-k / bits_par_cle)) ** k


def essai(hp, ha, bits_par_cle, k, n=N):
    f = Bloom(int(bits_par_cle * N), k)
    for h1, h2 in hp[:n]:
        f.ajouter(h1, h2)
    t = time.perf_counter()
    faux = sum(f.contient(h1, h2) for h1, h2 in ha)
    duree = (time.perf_counter() - t) / len(ha) * 1e6
    return faux / len(ha), len(f.bits), duree


def partie_python():
    print("1. Bloom en Python : 1 000 000 de clés insérées, 1 000 000 absentes")
    hp = [hache128(c) for c in PRESENTES]
    ha = [hache128(c) for c in ABSENTES]
    print("\n   a. bits par clé et nombre optimal de fonctions (k = b ln 2)")
    print(f"   {'bits/clé':>9}{'k':>4}{'mémoire':>10}{'faux pos.':>11}{'théorie':>10}{'µs/requête':>12}")
    for b in (4, 6, 8, 9.6, 10, 12, 16, 20):
        k = max(1, round(b * math.log(2)))
        fp, octets, us = essai(hp, ha, b, k)
        print(f"   {b:>9g}{k:>4}{mo(octets):>8.2f}Mo{100 * fp:>10.3f}%{100 * theorie(b, k):>9.3f}%{us:>12.2f}")
    print("\n   b. 10 bits par clé, nombre de fonctions imposé")
    print(f"   {'k':>4}{'faux pos.':>11}{'théorie':>10}")
    for k in range(1, 11):
        fp, _, _ = essai(hp, ha, 10, k)
        print(f"   {k:>4}{100 * fp:>10.3f}%{100 * theorie(10, k):>9.3f}%")
    print("\n   c. prévu pour 1 000 000 de clés à 1 % (9,6 bits/clé, k = 7)")
    print(f"   {'clés':>11}{'faux pos.':>11}{'théorie':>10}")
    plus = [hache128(f"utilisateur:{i}") for i in range(N, 3 * N)]
    tout = hp + plus
    for n in (500_000, 1_000_000, 1_500_000, 2_000_000, 3_000_000):
        fp, _, _ = essai(tout, ha, 9.6, 7, n)
        print(f"   {nb(n, 11)}{100 * fp:>10.3f}%{100 * theorie(9.6 * N / n, 7):>9.3f}%")
    ensemble = set(PRESENTES)
    taille = sys.getsizeof(ensemble) + sum(sys.getsizeof(c) for c in PRESENTES)
    brut = sum(len(c) for c in PRESENTES)
    print(f"\n   d. set Python des mêmes clés : {mo(taille):.1f} Mo ; clés seules : {mo(brut):.1f} Mo")
    print(f"      filtre à 1 % : {mo(9.6 * N / 8):.1f} Mo")


def partie_redis():
    print("\n2. Redis 8.8 (RedisBloom)")
    r = demarrer_redis()
    try:
        def remplir(cmd, cle, cles, lot=10_000):
            erreurs = 0
            p = r.pipeline(transaction=False)
            for morceau in par_lots(cles, lot):
                if cmd == "CF.INSERT":
                    p.execute_command(cmd, cle, "ITEMS", *morceau)
                else:
                    p.execute_command(cmd, cle, *morceau)
            for rep in p.execute(raise_on_error=False):
                if isinstance(rep, Exception):
                    erreurs += lot
                elif isinstance(rep, list):
                    erreurs += sum(isinstance(x, Exception) for x in rep)
            return erreurs

        def faux_positifs(cmd, cle):
            p = r.pipeline(transaction=False)
            for morceau in par_lots(ABSENTES, 10_000):
                p.execute_command(cmd, cle, *morceau)
            return sum(sum(int(x) for x in rep) for rep in p.execute()) / len(ABSENTES)

        def memoire(cle):
            return r.memory_usage(cle, samples=0)

        print(f"   {'structure':<36}{'mémoire':>10}{'octets/clé':>11}{'faux pos.':>11}")
        for taux in ("0.01", "0.001"):
            cle = f"bf:{taux}"
            r.execute_command("BF.RESERVE", cle, taux, N)
            t = time.perf_counter()
            remplir("BF.MADD", cle, PRESENTES)
            ajout = time.perf_counter() - t
            fp = faux_positifs("BF.MEXISTS", cle)
            m = memoire(cle)
            print(f"   {'BF.RESERVE ' + taux + ' 1000000':<36}{mo(m):>8.2f}Mo{m / N:>11.2f}{100 * fp:>10.3f}%")
        remplir("SADD", "ensemble", PRESENTES)
        m = memoire("ensemble")
        print(f"   {'SADD (ensemble exact)':<36}{mo(m):>8.2f}Mo{m / N:>11.2f}{0:>10.3f}%")
        print(f"   (un million d'ajouts dans le filtre à 1 % : {ajout:.1f} s, lots de 10 000)")

        print("\n   filtres prévus pour 100 000 clés à 1 %, remplis d'un million")
        r.execute_command("BF.RESERVE", "bf:ext", "0.01", 100_000, "EXPANSION", 2)
        remplir("BF.MADD", "bf:ext", PRESENTES)
        info = r.bf().info("bf:ext")
        fp = faux_positifs("BF.MEXISTS", "bf:ext")
        m = memoire("bf:ext")
        print(f"   EXPANSION 2 : {info.filterNum} sous-filtres, {mo(m):.2f} Mo,"
              f" faux positifs {100 * fp:.3f} %")
        r.execute_command("BF.RESERVE", "bf:fixe", "0.01", 100_000, "NONSCALING")
        remplir("BF.MADD", "bf:fixe", PRESENTES)     # BF.MADD s'arrête au premier refus du lot
        inseres = r.bf().info("bf:fixe").insertedNum
        fp = faux_positifs("BF.MEXISTS", "bf:fixe")
        m = memoire("bf:fixe")
        print(f"   NONSCALING  : {inseres} clés acceptées, {N - inseres} refusées")
        print(f"                 (« non scaling filter is full ») ; {mo(m):.2f} Mo,")
        print(f"                 faux positifs {100 * fp:.3f} %")

        print("\n   filtre coucou (CF.RESERVE 1000000, empreintes de 8 bits)")
        r.execute_command("CF.RESERVE", "cf", N)
        refus = remplir("CF.INSERT", "cf", PRESENTES)
        fp = faux_positifs("CF.MEXISTS", "cf")
        m = memoire("cf")
        avant = r.execute_command("CF.EXISTS", "cf", "utilisateur:42")
        r.execute_command("CF.DEL", "cf", "utilisateur:42")
        apres = r.execute_command("CF.EXISTS", "cf", "utilisateur:42")
        print(f"   {mo(m):.2f} Mo ({m / N:.2f} octets/clé), {refus} refus, faux positifs {100 * fp:.3f} %")
        print(f"   CF.DEL utilisateur:42 : présent {avant} avant, {apres} après")
    finally:
        arreter_redis()


if __name__ == "__main__":
    parties = sys.argv[1] if len(sys.argv) > 1 else "12"
    if "1" in parties:
        partie_python()
    if "2" in parties:
        partie_redis()
