"""Chapitre 34 : Paxos (décret unique) en simulation à événements discrets.

n accepteurs, plusieurs proposeurs qui veulent chacun faire choisir leur
propre valeur. Chaque message met 0,5 à 1,5 ms et se perd avec une
probabilité donnée. Une valeur est « choisie » dès qu'un quorum
d'accepteurs l'a acceptée avec le même numéro de proposition ; un
observateur extérieur vérifie qu'une seule valeur est jamais choisie.

Variantes : Paxos correct (quorums majoritaires) ; quorums de 2 sur 5, qui
ne se recoupent pas ; proposeur qui oublie la règle « reprendre la valeur
acceptée de plus grand numéro » ; accepteurs qui redémarrent sans avoir
gardé leur état sur disque.

    python3 paxos_sim.py [graine]
"""
import heapq
import random
import statistics
import sys


class Accepteur:
    def __init__(self):
        self.promis = -1
        self.accepte = None                 # (numéro, valeur)
        self.vivant = True


class Simulation:
    def __init__(self, n, quorum, rng, perte=0.0, oubli_regle=False,
                 amnesie=False, recul="aleatoire"):
        self.n, self.q, self.rng, self.perte = n, quorum, rng, perte
        self.oubli_regle, self.amnesie, self.recul = oubli_regle, amnesie, recul
        self.acc = [Accepteur() for _ in range(n)]
        self.file, self.t, self.messages = [], 0.0, 0
        self.acceptes = {}                  # (numéro, valeur) -> accepteurs
        self.choisies = set()
        self.premier_choix = None
        self.tours = 0

    # --- réseau -------------------------------------------------------
    def envoyer(self, quoi, dest, *args):
        self.messages += 1
        if self.rng.random() < self.perte:
            return
        heapq.heappush(self.file, (self.t + self.rng.uniform(5e-4, 1.5e-3),
                                   self.rng.random(), quoi, dest, args))

    def plus_tard(self, delai, quoi, dest, *args):
        heapq.heappush(self.file, (self.t + delai, self.rng.random(), quoi,
                                   dest, args))

    # --- accepteur -----------------------------------------------------
    def prepare(self, i, p, num):
        a = self.acc[i]
        if not a.vivant:
            return
        if num > a.promis:
            a.promis = num
            self.envoyer("promesse", p, num, i, a.accepte)
        else:
            self.envoyer("refus", p, num, a.promis)

    def accept(self, i, p, num, valeur):
        a = self.acc[i]
        if not a.vivant:
            return
        if num >= a.promis:
            a.promis, a.accepte = num, (num, valeur)
            ens = self.acceptes.setdefault((num, valeur), set())
            ens.add(i)
            if len(ens) >= self.q:
                self.choisies.add(valeur)
                if self.premier_choix is None:
                    self.premier_choix = self.t
            self.envoyer("accepte", p, num, i)
        else:
            self.envoyer("refus", p, num, a.promis)

    def panne(self, i, duree):
        self.acc[i].vivant = False
        self.plus_tard(duree, "reprise", i)

    def reprise(self, i):
        a = self.acc[i]
        a.vivant = True
        if self.amnesie:                    # l'état n'était pas sur disque
            a.promis, a.accepte = -1, None

    def lancer(self, proposeurs, duree_max=2.0, pannes=0, f_pannes=0.0):
        self.prop = {}
        for p, (debut, valeur) in enumerate(proposeurs):
            self.prop[p] = {"valeur": valeur, "tour": 0, "phase": 0,
                            "promesses": {}, "acceptes": set(), "num": -1}
            self.plus_tard(debut, "tenter", p)
        for _ in range(pannes):
            i = self.rng.randrange(self.n)
            self.plus_tard(self.rng.uniform(0, f_pannes), "panne", i,
                           self.rng.uniform(0.002, 0.02))
        while self.file:
            self.t, _, quoi, dest, args = heapq.heappop(self.file)
            if self.t > duree_max:
                break
            getattr(self, quoi)(dest, *args) if quoi in (
                "prepare", "accept", "panne", "reprise") else \
                getattr(self, "p_" + quoi)(dest, *args)
            if self.premier_choix is not None and not self.continuer:
                break
        return self

    continuer = True

    # --- proposeur -------------------------------------------------------
    def p_tenter(self, p):
        s = self.prop[p]
        s["tour"] += 1
        self.tours += 1
        s["num"] = s["tour"] * 100 + p      # numéros uniques par proposeur
        s["phase"], s["promesses"], s["acceptes"] = 1, {}, set()
        for i in range(self.n):
            self.envoyer("prepare", i, p, s["num"])
        self.plus_tard(0.010, "delai", p, s["num"])  # > 2 allers-retours

    def p_promesse(self, p, num, i, accepte):
        s = self.prop[p]
        if s["phase"] != 1 or num != s["num"]:
            return
        s["promesses"][i] = accepte
        if len(s["promesses"]) >= self.q:
            precedents = [a for a in s["promesses"].values() if a]
            v = s["valeur"]
            if precedents and not self.oubli_regle:
                v = max(precedents)[1]      # valeur de plus grand numéro
            s["phase"], s["v"] = 2, v
            for j in range(self.n):
                self.envoyer("accept", j, p, num, v)

    def p_accepte(self, p, num, i):
        s = self.prop[p]
        if s["phase"] != 2 or num != s["num"]:
            return
        s["acceptes"].add(i)
        if len(s["acceptes"]) >= self.q:
            s["phase"] = 3                  # le proposeur sait que c'est choisi

    def p_refus(self, p, num, promis):
        s = self.prop[p]
        if num == s["num"] and s["phase"] in (1, 2):
            s["tour"] = max(s["tour"], promis // 100)
            self.reessayer(p)

    def p_delai(self, p, num):
        s = self.prop[p]
        if num == s["num"] and s["phase"] in (1, 2):
            self.reessayer(p)

    def reessayer(self, p):
        s = self.prop[p]
        s["phase"] = 0
        attente = 0.0 if self.recul == "aucun" else \
            self.rng.uniform(0, 0.002 * min(2 ** s["tour"], 64))
        self.plus_tard(attente, "tenter", p)


def surete(rng, essais=10_000):
    print("1. sûreté : 10 000 exécutions, 3 proposeurs concurrents, 5 accepteurs,")
    print("   5 % de messages perdus, 2 pannes d'accepteur de 2 à 20 ms")
    print(f"   {'variante':44} {'2 valeurs choisies':>18}")
    for nom, q, oubli, amn in [
            ("Paxos, quorums de 3 sur 5", 3, False, False),
            ("quorums de 2 sur 5 (ne se recoupent pas)", 2, False, False),
            ("oubli de la valeur acceptée", 3, True, False),
            ("accepteurs sans état sur disque", 3, False, True)]:
        mauvaises = 0
        for _ in range(essais):
            sim = Simulation(5, q, rng, perte=0.05, oubli_regle=oubli,
                             amnesie=amn)
            sim.continuer = True            # laisser tourner jusqu'au bout
            debuts = [rng.uniform(0, 0.002) for _ in range(3)]
            sim.lancer([(d, f"v{k}") for k, d in enumerate(debuts)],
                       duree_max=0.1, pannes=2, f_pannes=0.01)
            mauvaises += len(sim.choisies) > 1
        print(f"   {nom:44} {mauvaises:12} ({100 * mauvaises / essais:.2f} %)")


def vivacite(rng, essais=2_000):
    print("\n2. vivacité : 2 proposeurs qui démarrent en même temps, 5 accepteurs")
    print(f"   {'recul après un échec':30} {'décidé':>7} {'temps méd.':>11} "
          f"{'99e c.':>8} {'tours méd.':>10}")
    for nom, recul, n_prop in [("aucun", "aucun", 2),
                               ("aléatoire exponentiel", "aleatoire", 2),
                               ("un seul proposeur (leader)", "aleatoire", 1)]:
        temps, tours, decides = [], [], 0
        for _ in range(essais):
            sim = Simulation(5, 3, rng, recul=recul)
            sim.continuer = False
            sim.lancer([(0.0, f"v{k}") for k in range(n_prop)], duree_max=1.0)
            if sim.premier_choix is not None:
                decides += 1
                temps.append(sim.premier_choix * 1000)
                tours.append(sim.tours)
        temps.sort()
        med = statistics.median(temps) if temps else float("nan")
        p99 = temps[int(0.99 * len(temps))] if temps else float("nan")
        print(f"   {nom:30} {100 * decides / essais:6.1f}% {med:8.1f} ms "
              f"{p99:6.1f} ms {statistics.median(tours) if tours else 0:8.0f}")


def pannes(rng, essais=2_000):
    print("\n3. accepteurs arrêtés dès le départ, un proposeur, 1 s au plus")
    print(f"   {'accepteurs':>11} {'arrêtés':>8} {'décidé':>8} {'temps méd.':>11}")
    for n in (3, 5):
        for k in range(0, (n + 1) // 2 + 1):
            temps, decides = [], 0
            for _ in range(essais):
                sim = Simulation(n, n // 2 + 1, rng)
                sim.continuer = False
                for i in rng.sample(range(n), k):
                    sim.acc[i].vivant = False
                sim.lancer([(0.0, "v")], duree_max=1.0)
                if sim.premier_choix is not None:
                    decides += 1
                    temps.append(sim.premier_choix * 1000)
            med = f"{statistics.median(temps):8.1f} ms" if temps else "     jamais"
            print(f"   {n:11} {k:8} {100 * decides / essais:7.1f}% {med}")


def cout(rng):
    print("\n4. coût d'une décision sans concurrence ni perte, 5 accepteurs")
    sim = Simulation(5, 3, rng)
    sim.continuer = True
    sim.lancer([(0.0, "v")], duree_max=0.05)
    print(f"   {sim.messages} messages (prepare, promesse, accept, acceptée, "
          f"× 5 accepteurs)")
    print(f"   valeur choisie en {sim.premier_choix * 1000:.1f} ms : deux "
          f"allers-retours, 0,5 à 1,5 ms par sens")


def main():
    graine = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    rng = random.Random(graine)
    print(f"simulation de Paxos, graine {graine}\n")
    surete(rng)
    vivacite(rng)
    pannes(rng)
    cout(rng)


if __name__ == "__main__":
    main()
