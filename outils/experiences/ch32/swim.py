"""Chapitre 32 : le protocole d'appartenance SWIM, simulé.

Simulation par périodes de protocole (1 s), sur un réseau qui perd chaque
message avec une probabilité donnée. À chaque période, chaque membre vivant
sonde le suivant de sa liste (ping, ack) ; sans réponse, il demande à k
autres membres de sonder pour lui (ping-req). Sans le mécanisme de
suspicion, un échec fait déclarer le membre mort ; avec lui, le membre est
d'abord suspect, peut réfuter en augmentant son numéro d'incarnation, et
n'est déclaré mort qu'après 4 × log10(n) périodes (réglage par défaut de
memberlist). Les nouvelles (suspect, vivant, mort) voyagent greffées sur les
messages du protocole, chacune au plus 4 × ⌈log10(n + 1)⌉ fois par membre.
Un membre « lent » traite trop tard, avec une probabilité donnée, les
réponses et les nouvelles qu'il reçoit (surcharge du processeur).

    python3 swim.py
"""
import math
import random
import statistics
import sys

VIVANT, SUSPECT, MORT = 0, 1, 2


class Membre:
    def __init__(self, i, n, rng):
        self.i, self.incarnation = i, 0
        self.vue = {j: (VIVANT, 0) for j in range(n)}   # j -> (état, incarnation)
        self.a_diffuser = {}                # j -> transmissions restantes
        self.suspects = {}                  # j -> période de début
        self.liste, self.pos, self.rng = [], 0, rng

    def cible(self, n):
        for _ in range(2 * n):
            if self.pos >= len(self.liste):
                self.liste = [j for j in range(n) if j != self.i]
                self.rng.shuffle(self.liste)
                self.pos = 0
            j = self.liste[self.pos]
            self.pos += 1
            if self.vue[j][0] != MORT:
                return j
        return None


class Simulation:
    def __init__(self, n, perte, k, suspicion, lents=(), q_lent=0.0,
                 graine=1):
        self.n, self.perte, self.k, self.suspicion = n, perte, k, suspicion
        self.rng = random.Random(graine)
        self.membres = [Membre(i, n, random.Random(graine * 7919 + i))
                        for i in range(n)]
        self.vivants = set(range(n))
        self.lents, self.q_lent = set(lents), q_lent
        self.limite = 4 * math.ceil(math.log10(n + 1))
        self.delai_suspicion = 4 * max(1.0, math.log10(n))
        self.messages, self.periode = 0, 0
        self.fausses = []                   # (accusateur, accusé) déclarations
        self.premiere = {}                  # j -> période de 1re suspicion
        self.mort_connue = {}               # j -> période où tous le savent

    # --- réseau ---------------------------------------------------------
    def passe(self, src, dst):
        self.messages += 1
        return dst in self.vivants and self.rng.random() >= self.perte

    def traite(self, m):
        """Le membre m traite-t-il à temps ce qu'il reçoit ?"""
        return m not in self.lents or self.rng.random() >= self.q_lent

    # --- nouvelles -------------------------------------------------------
    def greffer(self, src, dst):
        """src envoie ses nouvelles à dst avec un message."""
        ms = self.membres[src]
        for j in list(ms.a_diffuser):
            ms.a_diffuser[j] -= 1
            if ms.a_diffuser[j] <= 0:
                del ms.a_diffuser[j]
            if self.traite(dst):
                self.recevoir(dst, j, *ms.vue[j])

    def recevoir(self, m, j, etat, inc):
        mm = self.membres[m]
        if j == m:                          # une nouvelle sur moi-même
            if etat == SUSPECT and inc >= mm.incarnation:
                mm.incarnation = inc + 1    # réfutation
                mm.vue[m] = (VIVANT, mm.incarnation)
                mm.a_diffuser[m] = self.limite
            return
        e0, i0 = mm.vue[j]
        if e0 == MORT:
            return
        plus_recente = (etat == MORT or (etat == SUSPECT and inc >= i0
                        and not (e0 == SUSPECT and inc == i0))
                        or (etat == VIVANT and inc > i0))
        if plus_recente:
            mm.vue[j] = (etat, inc)
            mm.a_diffuser[j] = self.limite
            if etat == SUSPECT:
                mm.suspects.setdefault(j, self.periode)
            else:
                mm.suspects.pop(j, None)

    def declarer(self, m, j):
        """m n'a pas eu de réponse de j."""
        mm = self.membres[m]
        etat, inc = mm.vue[j]
        if self.suspicion:
            if etat == VIVANT:
                mm.vue[j] = (SUSPECT, inc)
                mm.suspects[j] = self.periode
                mm.a_diffuser[j] = self.limite
                self.premiere.setdefault(j, self.periode)
        else:
            self.mourir(m, j)

    def mourir(self, m, j):
        mm = self.membres[m]
        if mm.vue[j][0] != MORT:
            mm.vue[j] = (MORT, mm.vue[j][1])
            mm.a_diffuser[j] = self.limite
            mm.suspects.pop(j, None)
            self.premiere.setdefault(j, self.periode)
            if j in self.vivants:
                self.fausses.append((m, j))

    # --- une période -----------------------------------------------------
    def sonder(self, i, j):
        """ping direct, puis ping-req par k intermédiaires."""
        if self.passe(i, j) and (self.greffer(i, j) or True) \
                and self.traite(j) and self.passe(j, i) \
                and (self.greffer(j, i) or True) and self.traite(i):
            return True
        autres = [m for m in self.vivants_vus(i) if m not in (i, j)]
        for m in self.rng.sample(autres, min(self.k, len(autres))):
            if self.passe(i, m) and self.traite(m) and self.passe(m, j) \
                    and (self.greffer(m, j) or True) and self.traite(j) \
                    and self.passe(j, m) and self.passe(m, i) \
                    and (self.greffer(m, i) or True) and self.traite(i):
                return True
        return False

    def vivants_vus(self, i):
        vue = self.membres[i].vue
        return [m for m in range(self.n) if vue[m][0] != MORT]

    def pas(self):
        self.periode += 1
        ordre = list(self.vivants)
        self.rng.shuffle(ordre)
        for i in ordre:
            mi = self.membres[i]
            j = mi.cible(self.n)
            if j is not None and not self.sonder(i, j):
                self.declarer(i, j)
            for s, debut in list(mi.suspects.items()):
                if self.periode - debut >= self.delai_suspicion:
                    self.mourir(i, s)
        for j in range(self.n):
            if j not in self.vivants and j not in self.mort_connue and all(
                    self.membres[m].vue[j][0] == MORT for m in self.vivants):
                self.mort_connue[j] = self.periode


def detection(n, graines=10, graine=1):
    premieres, toutes, charge = [], [], []
    for g in range(graine * 100, graine * 100 + graines):
        sim = Simulation(n, 0.0, 3, True, graine=g)
        for _ in range(5):
            sim.pas()
        victime = sim.rng.randrange(n)
        sim.vivants.discard(victime)
        debut, m0 = sim.periode, sim.messages
        while victime not in sim.mort_connue and sim.periode < debut + 200:
            sim.pas()
        premieres.append(sim.premiere[victime] - debut)
        toutes.append(sim.mort_connue[victime] - debut)
        charge.append((sim.messages - m0) / (sim.periode - debut) / (n - 1))
    return (statistics.mean(premieres), max(premieres),
            statistics.mean(toutes), max(toutes), statistics.mean(charge))


def fausses(n, perte, k, suspicion, periodes, lents=(), q=0.0, graine=1):
    sim = Simulation(n, perte, k, suspicion, lents, q, graine)
    for _ in range(periodes):
        sim.pas()
    sains = [(a, b) for a, b in sim.fausses if a not in sim.lents]
    exclus = sum(1 for j in sim.vivants if sum(
        sim.membres[m].vue[j][0] == MORT for m in sim.vivants) > n / 2)
    return len(sim.fausses), len(sains), exclus


def main():
    graine = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    print(f"simulation SWIM, graine {graine} ; période de protocole = 1 s")
    print("\n1. panne d'un membre, sans perte, k = 3, suspicion")
    print("   (10 pannes, 4 pour n = 1 000 ; moyenne et maximum en périodes)")
    print(f"   {'n':>5} {'1re suspicion':>14} {'tous savent':>12} "
          f"{'msg/membre/s':>13} {'tout-à-tout':>12}")
    for n in (10, 100, 1000):
        p1, p1m, pt, ptm, c = detection(n, 10 if n < 1000 else 4, graine)
        print(f"   {n:5} {p1:7.1f} (max {p1m:2}) {pt:6.1f} (max {ptm:2}) "
              f"{c:9.2f}     {n - 1:9}")
    print("\n2. fausses déclarations de mort, n = 100, 600 périodes")
    print("   (déclarations ; entre parenthèses, membres vivants que la")
    print("   majorité croit morts à la fin)")
    print(f"   {'réglage':36} {'perte 1 %':>14} {'perte 5 %':>14}")
    for nom, k, susp in [("ping direct seul", 0, False),
                         ("+ 3 sondes indirectes", 3, False),
                         ("+ 3 sondes indirectes + suspicion", 3, True)]:
        r = [fausses(100, p, k, susp, 600, graine=graine)
             for p in (0.01, 0.05)]
        print(f"   {nom:36} " + " ".join(f"{f'{d} ({x})':>14}"
                                          for d, _, x in r))
    print("\n3. membres lents (60 % de réponses traitées trop tard), n = 100,")
    print("   perte 1 %, k = 3, suspicion, 300 périodes")
    print(f"   {'membres lents':>14} {'fausses déclarations':>21} "
          f"{'dont par des sains':>19}")
    for nl in (0, 1, 4, 16):
        tot, sains, _ = fausses(100, 0.01, 3, True, 300, lents=range(nl),
                                q=0.6, graine=graine)
        print(f"   {nl:14} {tot:21} {sains:19}")


if __name__ == "__main__":
    main()
