"""Chapitre 40 : Count-Min Sketch et éléments les plus fréquents.

Un flux de 2 000 000 d'événements sur 100 000 clés distinctes, tirées selon
une loi de Zipf d'exposant 1 (la clé de rang r revient en proportion de 1/r),
comme des pages vues ou des mots recherchés.

1. Count-Min Sketch en Python (Cormode et Muthukrishnan, 2005) : d lignes de w
   compteurs, w = ⌈e/ε⌉, d = ⌈ln(1/δ)⌉ ; estimation = minimum des d compteurs.
   a. ε de 0,01 à 0,0001, δ = 0,01 : mémoire et erreurs ;
   b. mise à jour conservatrice (on n'augmente que les compteurs minimaux) ;
   c. les 10 clés les plus fréquentes, suivies par le sketch et une liste de 10.
2. Redis 8.8 : CMS.INITBYPROB 0.001 0.01 ; TOPK.RESERVE 10 avec ses réglages
   par défaut (largeur 7, profondeur 8), puis avec une largeur de 100 et une
   profondeur de 5 ; décompte exact dans un hachage Redis (HINCRBY).

    python3 countmin.py
"""
import collections
import hashlib
import itertools
import math
import random
import sys
import time

from commun import arreter_redis, demarrer_redis, mo, nb, par_lots

EVENEMENTS, CLES = 2_000_000, 100_000


def flux():
    alea = random.Random(40)
    poids = list(itertools.accumulate(1 / r for r in range(1, CLES + 1)))
    return [f"page:{r}" for r in alea.choices(range(1, CLES + 1), cum_weights=poids, k=EVENEMENTS)]


class CountMin:
    def __init__(self, eps, delta, conservateur=False):
        self.w, self.d = math.ceil(math.e / eps), math.ceil(math.log(1 / delta))
        self.t = [[0] * self.w for _ in range(self.d)]
        self.conservateur = conservateur
        self.cache = {}

    def _pos(self, cle):
        p = self.cache.get(cle)
        if p is None:
            h = hashlib.blake2b(cle.encode(), digest_size=16).digest()
            h1, h2 = int.from_bytes(h[:8], "little"), int.from_bytes(h[8:], "little") | 1
            p = self.cache[cle] = [(h1 + i * h2) % self.w for i in range(self.d)]
        return p

    def ajouter(self, cle, c=1):
        pos = self._pos(cle)
        if self.conservateur:
            v = min(self.t[i][j] for i, j in enumerate(pos)) + c
            for i, j in enumerate(pos):
                if self.t[i][j] < v:
                    self.t[i][j] = v
        else:
            for i, j in enumerate(pos):
                self.t[i][j] += c

    def estimer(self, cle):
        return min(self.t[i][j] for i, j in enumerate(self._pos(cle)))


def bilan(nom, estimer, exact, eps, octets):
    seuil = eps * EVENEMENTS
    err = [estimer(c) - n for c, n in exact.items()]
    dans = sum(e <= seuil for e in err) / len(err)
    rangs = sorted(exact, key=exact.get, reverse=True)
    rel_tete = max((estimer(c) - exact[c]) / exact[c] for c in rangs[:100])
    petites = [c for c in exact if exact[c] <= 5]
    est_queue = sum(estimer(c) for c in petites) / len(petites)
    print(f"   {nom:<26}{mo(octets) * 1000:>5.0f} Ko{sum(err) / len(err):>8.1f}{max(err):>7}"
          f"{100 * dans:>7.1f}%{100 * rel_tete:>8.2f}%{est_queue:>8.1f}")


def partie_python(evenements, exact):
    print(f"1. {nb(EVENEMENTS)} événements, {nb(len(exact))} clés vues ;"
          f" la plus fréquente : {nb(max(exact.values()))}")
    taille = sys.getsizeof(exact) + sum(sys.getsizeof(c) + 28 for c in exact)
    print(f"   décompte exact (dict Python) : {mo(taille):.1f} Mo")
    print("\n   a. et b. erreur = estimation − vrai compte (jamais négative)")
    print(f"   {'sketch (δ = 0,01)':<26}{'mémoire':>8}{'moy.':>8}{'max':>7}{'≤ εN':>8}"
          f"{'tête':>9}{'queue':>8}")
    for eps, cons in ((0.01, False), (0.001, False), (0.0001, False), (0.001, True)):
        cms = CountMin(eps, 0.01, cons)
        for c in evenements:
            cms.ajouter(c)
        nom = f"ε = {eps:g}, w×d = {cms.w}×{cms.d}" if not cons else "ε = 0.001 conservateur"
        bilan(nom.replace("0.0", "0,0"), cms.estimer, exact, eps, 4 * cms.w * cms.d)
    print("   (tête : pire erreur relative des 100 clés les plus fréquentes ;")
    print("    queue : estimation moyenne des clés vues au plus 5 fois)")

    print("\n   c. 10 clés les plus fréquentes (sketch ε = 0,001, liste de 10)")
    cms = CountMin(0.001, 0.01)
    dans = {}                          # au plus 10 clés et leur estimation
    t = time.perf_counter()
    for c in evenements:
        cms.ajouter(c)
        v = cms.estimer(c)
        if c in dans or len(dans) < 10:
            dans[c] = v
        else:
            plus_petite = min(dans, key=dans.get)
            if v > dans[plus_petite]:
                del dans[plus_petite]
                dans[c] = v
    vrai = [c for c, _ in exact.most_common(10)]
    trouve = sorted(dans, key=dans.get, reverse=True)
    print(f"   exactes : {', '.join(x.split(':')[1] for x in vrai)}")
    print(f"   sketch  : {', '.join(x.split(':')[1] for x in trouve)}")
    print(f"   {len(set(vrai) & set(trouve))} sur 10 retrouvées ; {time.perf_counter() - t:.1f} s")


def partie_redis(evenements, exact):
    print("\n2. Redis 8.8")
    r = demarrer_redis()
    try:
        r.execute_command("CMS.INITBYPROB", "cms", 0.001, 0.01)
        r.execute_command("TOPK.RESERVE", "topk", 10)
        r.execute_command("TOPK.RESERVE", "topk100", 10, 100, 5, 0.9)
        p = r.pipeline(transaction=False)
        for lot in par_lots(evenements, 10_000):
            compte = collections.Counter(lot)
            args = [x for c, n in compte.items() for x in (c, n)]
            p.execute_command("CMS.INCRBY", "cms", *args)
            p.execute_command("TOPK.ADD", "topk", *lot)
            p.execute_command("TOPK.ADD", "topk100", *lot)
            for c, n in compte.items():
                p.hincrby("exact", c, n)
            if len(p) > 50_000:
                p.execute()
        p.execute()
        info = r.cms().info("cms")
        cles = list(exact)
        est = []
        for lot in par_lots(cles, 10_000):
            est += r.execute_command("CMS.QUERY", "cms", *lot)
        estimations = dict(zip(cles, est))
        print(f"   CMS.INITBYPROB 0.001 0.01 : largeur {info.width}, profondeur {info.depth}")
        print(f"   {'':<26}{'mémoire':>8}{'moy.':>8}{'max':>7}{'≤ εN':>8}{'tête':>9}{'queue':>8}")
        bilan("CMS Redis", estimations.get, exact, 0.001, r.memory_usage("cms", samples=0))
        vrai = [c for c, _ in exact.most_common(10)]
        for cle, nom in (("topk", "défaut 7×8"), ("topk100", "100×5")):
            top = [x.decode() for x in r.execute_command("TOPK.LIST", cle)]
            print(f"   TOPK {nom:<10}: {', '.join(x.split(':')[1] for x in top)}"
                  f" ({len(set(top) & set(vrai))} sur 10)")
        for cle in ("cms", "topk", "topk100", "exact"):
            print(f"   mémoire {cle:<8}: {nb(r.memory_usage(cle, samples=0), 12)} o")
    finally:
        arreter_redis()


if __name__ == "__main__":
    ev = flux()
    ex = collections.Counter(ev)
    partie_python(ev, ex)
    partie_redis(ev, ex)
