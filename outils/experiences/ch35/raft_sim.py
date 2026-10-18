"""Chapitre 35 : Raft en simulation à événements discrets.

Cinq serveurs, des messages qui mettent 1 à 5 ms, des pannes et des
partitions. Chaque serveur suit l'algorithme de l'article d'Ongaro et
Ousterhout (élection, AppendEntries, règle de validation) ; un observateur
vérifie qu'aucune position du journal n'est jamais validée avec deux entrées
différentes (sûreté de la machine à états).

1. durée d'une élection après la panne du leader, selon la plage des délais
   d'élection (le leader envoie un battement juste avant de tomber) ;
2. sûreté sur des exécutions avec pannes et partitions, pour Raft et deux
   variantes fautives : vote sans la restriction d'élection, et validation
   d'entrées d'un terme précédent par simple comptage (figure 8 de
   l'article).

    python3 raft_sim.py [graine]
"""
import heapq
import random
import statistics
import sys

SUIVEUR, CANDIDAT, LEADER = "suiveur", "candidat", "leader"


class Serveur:
    def __init__(self, i):
        self.i = i
        self.terme, self.vote, self.journal = 0, None, []   # durable
        self.etat, self.valide, self.vivant = SUIVEUR, 0, True
        self.minuteur = 0                   # numéro du minuteur d'élection actif
        self.voix = set()
        self.suivant, self.accord = {}, {}


class Raft:
    def __init__(self, rng, n=5, delai_min=0.150, delai_max=0.300,
                 latence=(0.001, 0.005), perte=0.0, sans_restriction=False,
                 validation_naive=False, clients=True):
        self.rng, self.n = rng, n
        self.delai_min, self.delai_max = delai_min, delai_max
        self.latence, self.perte = latence, perte
        self.sans_restriction = sans_restriction
        self.validation_naive = validation_naive
        self.clients = clients
        self.s = [Serveur(i) for i in range(n)]
        self.file, self.t, self.seq = [], 0.0, 0
        self.coupes = set()                 # paires (a, b) qui ne communiquent pas
        self.valides = {}                   # position -> (terme, commande)
        self.violations = 0
        self.premier_leader = None
        self.commande = 0
        for srv in self.s:
            self.armer(srv)

    # --- mécanique ------------------------------------------------------
    def plus_tard(self, d, quoi, *args):
        self.seq += 1
        heapq.heappush(self.file, (self.t + d, self.seq, quoi, args))

    def envoyer(self, a, b, msg):
        if (a, b) in self.coupes or self.rng.random() < self.perte:
            return
        self.plus_tard(self.rng.uniform(*self.latence), "recevoir", b, a, msg)

    def armer(self, srv):
        srv.minuteur += 1
        self.plus_tard(self.rng.uniform(self.delai_min, self.delai_max),
                       "expiration", srv.i, srv.minuteur)

    def tourner(self, jusqu_a):
        while self.file and self.file[0][0] <= jusqu_a:
            self.t, _, quoi, args = heapq.heappop(self.file)
            getattr(self, quoi)(*args)
        self.t = jusqu_a

    def noter(self, srv, ancien):
        for k in range(ancien + 1, srv.valide + 1):
            e = srv.journal[k - 1]
            vu = self.valides.setdefault(k, e)
            if vu != e:
                self.violations += 1
                self.valides[k] = e

    def dernier(self, srv):
        return (srv.journal[-1][0] if srv.journal else 0, len(srv.journal))

    def devenir_suiveur(self, srv, terme):
        if terme > srv.terme:
            srv.terme, srv.vote = terme, None
        srv.etat = SUIVEUR

    # --- minuteurs ------------------------------------------------------
    def expiration(self, i, numero):
        srv = self.s[i]
        if not srv.vivant or numero != srv.minuteur or srv.etat == LEADER:
            return
        srv.terme += 1
        srv.etat, srv.vote, srv.voix = CANDIDAT, i, {i}
        self.armer(srv)
        lt, li = self.dernier(srv)
        for j in range(self.n):
            if j != i:
                self.envoyer(i, j, ("vote?", srv.terme, lt, li))

    def battement(self, i, terme):
        srv = self.s[i]
        if not srv.vivant or srv.etat != LEADER or srv.terme != terme:
            return
        if self.clients and self.rng.random() < 0.3:   # commande d'un client
            self.commande += 1
            srv.journal.append((srv.terme, self.commande))
            srv.accord[i] = len(srv.journal)
        for j in range(self.n):
            if j != i:
                self.repliquer(srv, j)
        self.plus_tard(0.050, "battement", i, terme)

    def repliquer(self, srv, j):
        prec = srv.suivant[j] - 1
        terme_prec = srv.journal[prec - 1][0] if prec > 0 else 0
        self.envoyer(srv.i, j, ("ajout", srv.terme, prec, terme_prec,
                                srv.journal[prec:prec + 50], srv.valide))

    # --- messages -------------------------------------------------------
    def recevoir(self, b, a, msg):
        srv = self.s[b]
        if not srv.vivant:
            return
        sorte, terme = msg[0], msg[1]
        if terme > srv.terme:
            self.devenir_suiveur(srv, terme)
        getattr(self, "m_" + sorte.rstrip("?!").replace("-", "_"))(srv, a, msg)

    def m_vote(self, srv, a, msg):
        if msg[0] == "vote!":
            return self.m_reponse_vote(srv, a, msg)
        _, terme, lt, li = msg
        a_jour = (lt, li) >= self.dernier(srv)
        ok = (terme == srv.terme and srv.vote in (None, a)
              and (a_jour or self.sans_restriction))
        if ok:
            srv.vote = a
            self.armer(srv)
        self.envoyer(srv.i, a, ("vote!", srv.terme, ok))

    def m_reponse_vote(self, srv, a, msg):
        _, terme, ok = msg
        if srv.etat != CANDIDAT or terme != srv.terme or not ok:
            return
        srv.voix.add(a)
        if len(srv.voix) > self.n // 2:
            srv.etat = LEADER
            if self.premier_leader is None:
                self.premier_leader = self.t
            srv.suivant = {j: len(srv.journal) + 1 for j in range(self.n)}
            srv.accord = {j: 0 for j in range(self.n)}
            srv.accord[srv.i] = len(srv.journal)
            self.battement(srv.i, srv.terme)

    def m_ajout(self, srv, a, msg):
        if msg[0] == "ajout!":
            return self.m_reponse_ajout(srv, a, msg)
        _, terme, prec, terme_prec, entrees, valide_leader = msg
        if terme < srv.terme:
            return self.envoyer(srv.i, a, ("ajout!", srv.terme, False, 0))
        srv.etat = SUIVEUR
        self.armer(srv)
        if prec > len(srv.journal) or (
                prec > 0 and srv.journal[prec - 1][0] != terme_prec):
            return self.envoyer(srv.i, a, ("ajout!", srv.terme, False, 0))
        for k, e in enumerate(entrees):
            pos = prec + k
            if pos < len(srv.journal) and srv.journal[pos] != e:
                del srv.journal[pos:]       # conflit : on tronque
            if pos >= len(srv.journal):
                srv.journal.append(e)
        fin = prec + len(entrees)
        if valide_leader > srv.valide:
            ancien = srv.valide
            srv.valide = min(valide_leader, fin)
            if srv.valide > ancien:
                self.noter(srv, ancien)
        self.envoyer(srv.i, a, ("ajout!", srv.terme, True, fin))

    def m_reponse_ajout(self, srv, a, msg):
        _, terme, ok, fin = msg
        if srv.etat != LEADER or terme != srv.terme:
            return
        if not ok:
            srv.suivant[a] = max(1, srv.suivant[a] - 1)
            return self.repliquer(srv, a)
        srv.accord[a] = max(srv.accord[a], fin)
        srv.suivant[a] = srv.accord[a] + 1
        for pos in range(len(srv.journal), srv.valide, -1):
            copies = sum(1 for v in srv.accord.values() if v >= pos)
            if copies > self.n // 2 and (self.validation_naive
                                         or srv.journal[pos - 1][0] == srv.terme):
                ancien = srv.valide
                srv.valide = pos
                self.noter(srv, ancien)
                break

    # --- pannes ---------------------------------------------------------
    def tomber(self, i):
        self.s[i].vivant = False

    def relever(self, i):
        srv = self.s[i]
        srv.vivant, srv.etat, srv.valide = True, SUIVEUR, 0   # le reste est durable
        self.armer(srv)

    def leader(self):
        chefs = [s for s in self.s if s.vivant and s.etat == LEADER]
        return max(chefs, key=lambda s: s.terme) if chefs else None


def elections(rng, essais=1000):
    print("1. durée d'une élection après la panne du leader (5 serveurs,")
    print("   le leader envoie un battement à tous puis tombe)")
    print(f"   {'délai d’élection':>17} {'latence':>12} {'médiane':>9} "
          f"{'99e c.':>9} {'max':>9}")
    for (dmin, dmax), lat in [((0.150, 0.150), (0.002, 0.002)),
                              ((0.150, 0.155), (0.001, 0.005)),
                              ((0.150, 0.200), (0.001, 0.005)),
                              ((0.150, 0.300), (0.001, 0.005)),
                              ((0.012, 0.024), (0.001, 0.005))]:
        durees = []
        for _ in range(essais):
            # premier leader élu avec des délais aléatoires, puis la
            # configuration étudiée s'applique aux battements suivants
            r = Raft(rng, clients=False)
            r.tourner(1.0)
            r.delai_min, r.delai_max, r.latence = dmin, dmax, lat
            r.tourner(1.5)
            chef = r.leader()
            if chef is None:
                continue
            # battement à tous, puis panne
            for j in range(r.n):
                if j != chef.i:
                    r.repliquer(chef, j)
            r.tomber(chef.i)
            debut = r.t
            fin = debut + 10.0
            while r.t < fin:
                r.tourner(min(fin, r.t + 0.005))
                nouveau = r.leader()
                if nouveau and nouveau is not chef:
                    break
            durees.append(min(r.t - debut, 10.0) * 1000)
        durees.sort()
        lat_txt = (f"{lat[0] * 1000:.0f} ms" if lat[0] == lat[1]
                   else f"{lat[0] * 1000:.0f}-{lat[1] * 1000:.0f} ms")
        print(f"   {dmin * 1000:6.0f}-{dmax * 1000:3.0f} ms {lat_txt:>12} "
              f"{statistics.median(durees):7.0f}ms "
              f"{durees[int(0.99 * len(durees))]:7.0f}ms {durees[-1]:7.0f}ms")


def perturber(r, rng, duree):
    """Pannes et partitions aléatoires pendant `duree` secondes."""
    t = 0.0
    while t < duree:
        t += rng.uniform(0.05, 0.3)
        r.tourner(t)
        x = rng.random()
        if x < 0.4:                         # le leader (ou un autre) tombe
            chef = r.leader()
            i = chef.i if chef and rng.random() < 0.7 else rng.randrange(r.n)
            r.tomber(i)
            r.plus_tard(rng.uniform(0.02, 0.4), "relever", i)
        elif x < 0.7:                       # partition aléatoire
            groupe = set(rng.sample(range(r.n), rng.randint(1, 2)))
            r.coupes = {(a, b) for a in range(r.n) for b in range(r.n)
                        if (a in groupe) != (b in groupe)}
        else:
            r.coupes = set()
    r.coupes = set()
    r.tourner(duree + 1.0)


def surete(rng, essais=2000):
    print(f"\n2. sûreté : {essais} exécutions de 4 s par variante, pannes et "
          f"partitions")
    print("   toutes les 50 à 300 ms ; une commande pour 3 battements sur 10")
    print(f"   {'variante':36} {'exécutions fautives':>20} {'positions':>10}")
    for nom, sr, vn in [("Raft", False, False),
                        ("vote sans restriction d'élection", True, False),
                        ("validation naïve (figure 8)", False, True),
                        ("les deux", True, True)]:
        fautives = positions = 0
        for _ in range(essais):
            r = Raft(rng, sans_restriction=sr, validation_naive=vn)
            perturber(r, rng, 4.0)
            fautives += r.violations > 0
            positions += r.violations
        print(f"   {nom:36} {fautives:12} ({100 * fautives / essais:4.1f} %)"
              f" {positions:10}")


def main():
    graine = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    rng = random.Random(graine)
    print(f"simulation de Raft, graine {graine}\n")
    elections(rng)
    surete(rng)


if __name__ == "__main__":
    main()
