"""Chapitre 30 : cinq algorithmes de limitation de débit, comparés en simulation.

Chaque limiteur répond à autoriser(cle, t), où t est l'heure de la requête en
secondes ; l'horloge est simulée, les résultats des parties 1 à 3 sont donc
identiques d'un passage à l'autre. La partie 4 mesure la mémoire réelle des
structures Python, la partie 5 le nombre de décisions par seconde.

    python3 algorithmes.py
"""
import math
import random
import time
import tracemalloc
from array import array
from collections import deque


class FenetreFixe:
    """Un compteur par fenêtre de `fenetre` secondes alignée sur l'horloge."""

    def __init__(self, limite, fenetre):
        self.limite, self.fenetre = limite, fenetre
        self.etat = {}                      # cle -> [numéro de fenêtre, compte]

    def autoriser(self, cle, t):
        n = int(t // self.fenetre)
        e = self.etat.get(cle)
        if e is None or e[0] != n:
            e = self.etat[cle] = [n, 0]
        if e[1] < self.limite:
            e[1] += 1
            return True
        return False


class JournalGlissant:
    """Garde l'heure de chaque requête acceptée dans la dernière fenêtre."""

    def __init__(self, limite, fenetre, compact=False):
        self.limite, self.fenetre, self.compact = limite, fenetre, compact
        self.etat = {}

    def autoriser(self, cle, t):
        j = self.etat.get(cle)
        if j is None:
            j = self.etat[cle] = array("d") if self.compact else deque()
        if self.compact:                    # tableau de flottants de 8 octets
            i = 0
            while i < len(j) and j[i] <= t - self.fenetre:
                i += 1
            if i:
                del j[:i]
        else:
            while j and j[0] <= t - self.fenetre:
                j.popleft()
        if len(j) < self.limite:
            j.append(t)
            return True
        return False


class CompteurGlissant:
    """Compte de la fenêtre courante plus compte de la précédente, pondéré par
    la part de la fenêtre précédente encore couverte (approximation de
    Cloudflare)."""

    def __init__(self, limite, fenetre):
        self.limite, self.fenetre = limite, fenetre
        self.etat = {}                      # cle -> [fenêtre, précédent, courant]

    def autoriser(self, cle, t):
        n = int(t // self.fenetre)
        e = self.etat.get(cle)
        if e is None:
            e = self.etat[cle] = [n, 0, 0]
        if e[0] != n:
            e[1] = e[2] if e[0] == n - 1 else 0
            e[0], e[2] = n, 0
        reste = 1 - (t - n * self.fenetre) / self.fenetre
        if e[1] * reste + e[2] < self.limite:
            e[2] += 1
            return True
        return False


class SeauJetons:
    """`capacite` jetons au plus, `debit` jetons ajoutés par seconde ; une
    requête prend un jeton."""

    def __init__(self, debit, capacite):
        self.debit, self.capacite = debit, capacite
        self.etat = {}                      # cle -> [jetons, heure du calcul]

    def autoriser(self, cle, t):
        e = self.etat.get(cle)
        if e is None:
            e = self.etat[cle] = [self.capacite, t]
        e[0] = min(self.capacite, e[0] + (t - e[1]) * self.debit)
        e[1] = t
        if e[0] >= 1:
            e[0] -= 1
            return True
        return False


class GCRA:
    """Seau percé utilisé comme compteur (GCRA) : une seule heure par clé,
    l'heure d'arrivée théorique (TAT) de la prochaine requête."""

    def __init__(self, debit, rafale):
        self.intervalle = 1 / debit
        self.tolerance = (rafale - 1) * self.intervalle
        self.etat = {}                      # cle -> TAT

    def autoriser(self, cle, t):
        tat = max(self.etat.get(cle, t), t)
        if tat - t <= self.tolerance:
            self.etat[cle] = tat + self.intervalle
            return True
        return False


class SeauPerceFile:
    """Seau percé utilisé comme file : les requêtes attendent et sortent au
    débit fixe ; au-delà de `taille` requêtes en attente, refus."""

    def __init__(self, debit, taille):
        self.intervalle, self.taille = 1 / debit, taille
        self.etat = {}                      # cle -> heure de sortie de la dernière

    def sortie(self, cle, t):
        """Heure à laquelle la requête sera servie, ou None si refusée."""
        derniere = self.etat.get(cle, -math.inf)
        depart = max(t, derniere + self.intervalle)
        if (depart - t) / self.intervalle > self.taille:
            return None
        self.etat[cle] = depart
        return depart


LIMITE, FENETRE = 100, 60.0


def limiteurs():
    return {
        "fenêtre fixe": FenetreFixe(LIMITE, FENETRE),
        "journal glissant": JournalGlissant(LIMITE, FENETRE),
        "compteur glissant": CompteurGlissant(LIMITE, FENETRE),
        "seau à jetons (b = 100)": SeauJetons(LIMITE / FENETRE, LIMITE),
        "seau à jetons (b = 10)": SeauJetons(LIMITE / FENETRE, 10),
        "GCRA (rafale 10)": GCRA(LIMITE / FENETRE, 10),
    }


def max_sur_fenetre(heures, duree):
    """Plus grand nombre d'heures dans un intervalle glissant de `duree`."""
    heures, m, i = sorted(heures), 0, 0
    for j, h in enumerate(heures):
        while heures[i] <= h - duree:
            i += 1
        m = max(m, j - i + 1)
    return m


def frontieres():
    print("1. rafales de part et d'autre des frontières de fenêtre")
    print(f"   limite {LIMITE} requêtes par {FENETRE:.0f} s ; 5 fois : 100 "
          "requêtes dans")
    print("   la dernière seconde d'une minute, 100 dans la première de la "
          "suivante")
    trace = []
    for k in range(1, 6):
        trace += [60 * k - 1 + i / 100 for i in range(100)]
        trace += [60 * k + i / 100 for i in range(100)]
    print(f"   {'':24} {'acceptées':>9} {'max/60 s':>9} {'max/2 s':>8}")
    for nom, lim in limiteurs().items():
        ok = [t for t in trace if lim.autoriser("a", t)]
        print(f"   {nom:24} {len(ok):9} {max_sur_fenetre(ok, 60):9} "
              f"{max_sur_fenetre(ok, 2):8}")


def regulier():
    print("\n2. client constant à 3 requêtes par seconde pendant 10 min")
    trace = [i / 3 for i in range(1800)]
    print(f"   {'':24} {'acceptées':>9} {'max/60 s':>9} {'max/2 s':>8}")
    for nom, lim in limiteurs().items():
        ok = [t for t in trace if lim.autoriser("a", t)]
        print(f"   {nom:24} {len(ok):9} {max_sur_fenetre(ok, 60):9} "
              f"{max_sur_fenetre(ok, 2):8}")
    f = SeauPerceFile(LIMITE / FENETRE, 10)
    attentes = []
    refus = 0
    for t in trace:
        s = f.sortie("a", t)
        if s is None:
            refus += 1
        else:
            attentes.append(s - t)
    print(f"   seau percé en file (10) : {len(attentes)} servies, {refus} "
          f"refusées, attente max {max(attentes):.1f} s")


def precision(graine):
    """2 000 sources poissonniennes de 0,5 à 2 fois la limite, 10 min.
    (a) Estimation du compteur glissant contre le compte exact des requêtes
    des 60 dernières secondes, calculée sur toutes les requêtes reçues, comme
    Cloudflare. (b) Garantie : pour chaque source, le plus grand nombre de
    requêtes acceptées dans un intervalle glissant de 60 s."""
    rng = random.Random(graine)
    par_source = []
    for s in range(2000):
        taux = LIMITE / FENETRE * rng.uniform(0.5, 2.0)
        t, h = rng.uniform(0, 60), []
        while t < 600:
            h.append(t)
            t += rng.expovariate(taux)
        par_source.append(h)
    total = sum(map(len, par_source))
    ecarts, desaccords = [], 0
    for h in par_source:
        fen, compte = deque(), {}
        for t in h:
            fen.append(t)
            while fen[0] <= t - FENETRE:
                fen.popleft()
            n = int(t // FENETRE)
            compte[n] = compte.get(n, 0) + 1
            approx = (compte.get(n - 1, 0) * (1 - (t - n * FENETRE) / FENETRE)
                      + compte[n])
            ecarts.append(abs(approx - len(fen)) / len(fen))
            desaccords += (approx > LIMITE) != (len(fen) > LIMITE)
    garanties = {}
    for nom, fab in [("fenêtre fixe", lambda: FenetreFixe(LIMITE, FENETRE)),
                     ("journal glissant",
                      lambda: JournalGlissant(LIMITE, FENETRE)),
                     ("compteur glissant",
                      lambda: CompteurGlissant(LIMITE, FENETRE)),
                     ("seau à jetons (b = 100)",
                      lambda: SeauJetons(LIMITE / FENETRE, LIMITE))]:
        lim, acc, pire, depasse = fab(), 0, 0, 0
        for s, h in enumerate(par_source):
            ok = [t for t in h if lim.autoriser(s, t)]
            acc += len(ok)
            m = max_sur_fenetre(ok, FENETRE)
            pire, depasse = max(pire, m), depasse + (m > LIMITE)
        garanties[nom] = (acc, depasse, pire)
    return total, sum(ecarts) / len(ecarts), desaccords, garanties


def memoire():
    print("\n4. mémoire par clé, 2 000 clés saturées, limite 1 000 par heure")
    n, limite = 2000, 1000
    fabriques = {
        "fenêtre fixe": lambda: FenetreFixe(limite, 3600),
        "compteur glissant": lambda: CompteurGlissant(limite, 3600),
        "seau à jetons": lambda: SeauJetons(limite / 3600, limite),
        "GCRA": lambda: GCRA(limite / 3600, limite),
        "journal (deque)": lambda: JournalGlissant(limite, 3600),
        "journal (array d)": lambda: JournalGlissant(limite, 3600, True),
    }
    for nom, fab in fabriques.items():
        tracemalloc.start()
        lim = fab()
        for i in range(limite):
            for c in range(n):
                lim.autoriser(c, 10 + i * 0.5 + c * 1e-6)
        octets = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()
        print(f"   {nom:20} {octets / n:9.0f} octets par clé")


def debit():
    print("\n5. décisions par seconde (un fil, 10 000 clés, 1 million de "
          "requêtes)")
    rng = random.Random(1)
    cles = [rng.randrange(10000) for _ in range(1_000_000)]
    for nom, lim in limiteurs().items():
        if "b = 10)" in nom:
            continue
        debut = time.perf_counter()
        t = 0.0
        for c in cles:
            lim.autoriser(c, t)
            t += 0.0006
        d = time.perf_counter() - debut
        print(f"   {nom:24} {1_000_000 / d / 1e6:5.2f} million(s) par s")


def main():
    frontieres()
    regulier()
    print("\n3. 2 000 sources de 0,5 à 2 fois la limite pendant 10 min")
    for graine in (1, 2):
        total, ecart, desacc, gar = precision(graine)
        print(f"   tirage {graine} : {total} requêtes ; compteur glissant : "
              f"écart moyen {100 * ecart:.1f} %,")
        print(f"      {desacc} décisions « au-dessus de 100 » différentes "
              f"du compte exact,")
        print(f"      soit {100 * desacc / total:.2f} % des requêtes")
        for nom, (acc, dep, pire) in gar.items():
            print(f"   {nom:23} {acc:8} acceptées, {dep:4} sources "
                  f"> 100, max {pire}")
    memoire()
    debit()


if __name__ == "__main__":
    main()
