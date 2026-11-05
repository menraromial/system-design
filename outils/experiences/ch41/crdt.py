"""Chapitre 41 : des répliques qui convergent sans se coordonner (CRDT).

Cinq répliques reçoivent des opérations au hasard et se transmettent leur état
par un réseau qui perd 20 % des messages, en duplique 20 % et les livre dans
le désordre (anti-entropie par paires tirées au hasard). Après un dernier
échange complet, on compare les répliques entre elles et à la vérité.

1. compteurs : 10 000 incréments ; entier « dernier écrivain gagne », entier
   fusionné par addition, G-Counter.
2. PN-Counter et invariant : un stock de 100 articles, des ventes acceptées
   tant que la réplique locale voit du stock, des échanges plus ou moins rares.
3. ensembles : scénarios précis (ajout et retrait concurrents, nouvel ajout
   après un retrait, horloge en avance), puis 20 000 opérations au hasard ;
   ensemble naïf (opérations rejouées), ensemble LWW, 2P-Set, OR-Set.
4. taille des métadonnées : OR-Set après 100 000 opérations, G-Counter selon
   le nombre de répliques.
5. un CRDT de texte réel, pycrdt (liaison de y-crdt, l'implémentation Rust de
   Yjs) : deux répliques éditent hors ligne puis échangent leurs mises à jour.

    python3 crdt.py
"""
import json
import random
import uuid

REPLIQUES = 5
alea = random.Random(41)


# ---- types ---------------------------------------------------------------------
class EntierLWW:
    """Un entier et l'horodatage de sa dernière écriture ; fusion : le plus récent."""
    def __init__(self, i):
        self.i, self.v, self.ts = i, 0, (0, 0)

    def incr(self, t):
        self.v, self.ts = self.v + 1, (t, self.i)

    def etat(self):
        return (self.v, self.ts)

    def fusion(self, e):
        if e[1] > self.ts:
            self.v, self.ts = e

    def valeur(self):
        return self.v


class EntierSomme:
    """Fusion par addition des incréments reçus : ni idempotente, ni sûre."""
    def __init__(self, i):
        self.v, self.local = 0, 0

    def incr(self, t):
        self.v += 1
        self.local += 1

    def etat(self):
        n, self.local = self.local, 0
        return n

    def fusion(self, e):
        self.v += e

    def valeur(self):
        return self.v


class GCounter:
    """Un compteur par réplique ; fusion : maximum composante par composante."""
    def __init__(self, i, n=REPLIQUES):
        self.i, self.c = i, [0] * n

    def incr(self, t, k=1):
        self.c[self.i] += k

    def etat(self):
        return list(self.c)

    def fusion(self, e):
        self.c = [max(a, b) for a, b in zip(self.c, e)]

    def valeur(self):
        return sum(self.c)


class PNCounter:
    def __init__(self, i):
        self.p, self.n = GCounter(i), GCounter(i)

    def ajouter(self, k):
        (self.p if k >= 0 else self.n).incr(0, abs(k))

    def etat(self):
        return (self.p.etat(), self.n.etat())

    def fusion(self, e):
        self.p.fusion(e[0])
        self.n.fusion(e[1])

    def valeur(self):
        return self.p.valeur() - self.n.valeur()


class EnsembleNaif:
    """Chaque réplique rejoue les opérations reçues, dans l'ordre d'arrivée."""
    def __init__(self, i):
        self.s, self.journal = set(), []

    def ajouter(self, x, t):
        self.s.add(x)
        self.journal.append(("+", x))

    def retirer(self, x, t):
        self.s.discard(x)
        self.journal.append(("-", x))

    def etat(self):
        return list(self.journal)

    def fusion(self, ops):
        for op, x in ops:
            (self.s.add if op == "+" else self.s.discard)(x)

    def valeur(self):
        return frozenset(self.s)


class EnsembleLWW:
    """Par élément, l'opération de plus grand horodatage gagne (comme Roshi)."""
    def __init__(self, i):
        self.d = {}                     # élément -> (horodatage, présent)

    def _poser(self, x, t, present):
        if x not in self.d or t > self.d[x][0]:
            self.d[x] = (t, present)

    def ajouter(self, x, t):
        self._poser(x, t, True)

    def retirer(self, x, t):
        self._poser(x, t, False)

    def etat(self):
        return dict(self.d)

    def fusion(self, e):
        for x, (t, p) in e.items():
            self._poser(x, t, p)

    def valeur(self):
        return frozenset(x for x, (t, p) in self.d.items() if p)


class DeuxPhases:
    """2P-Set : un ensemble des ajouts, un des retraits ; un retrait est définitif."""
    def __init__(self, i):
        self.a, self.r = set(), set()

    def ajouter(self, x, t):
        self.a.add(x)

    def retirer(self, x, t):
        if x in self.a:
            self.r.add(x)

    def etat(self):
        return (set(self.a), set(self.r))

    def fusion(self, e):
        self.a |= e[0]
        self.r |= e[1]

    def valeur(self):
        return frozenset(self.a - self.r)


class ORSet:
    """Ensemble observé-supprimé : chaque ajout porte une étiquette unique ; un
    retrait supprime les étiquettes qu'il a vues (pierres tombales)."""
    def __init__(self, i):
        self.ajouts, self.retraits = {}, set()   # étiquette -> élément ; étiquettes retirées

    def ajouter(self, x, t):
        self.ajouts[uuid.UUID(int=alea.getrandbits(128)).hex] = x

    def retirer(self, x, t):
        self.retraits |= {e for e, y in self.ajouts.items() if y == x}

    def etat(self):
        return (dict(self.ajouts), set(self.retraits))

    def fusion(self, e):
        self.ajouts.update(e[0])
        self.retraits |= e[1]

    def valeur(self):
        return frozenset(y for e, y in self.ajouts.items() if e not in self.retraits)


# ---- réseau ----------------------------------------------------------------------
def anti_entropie(reps, en_vol):
    """Une réplique envoie son état à une autre ; perte, duplication, désordre."""
    a, b = alea.sample(range(len(reps)), 2)
    msg = reps[a].etat()
    if alea.random() < 0.2:
        return                                         # perdu
    en_vol.append((b, msg))
    if alea.random() < 0.2:
        en_vol.append((b, msg))                        # dupliqué
    alea.shuffle(en_vol)
    while en_vol and alea.random() < 0.5:
        dest, m = en_vol.pop()
        reps[dest].fusion(m)


def synchroniser(reps, en_vol):
    for dest, m in en_vol:
        reps[dest].fusion(m)
    en_vol.clear()
    for _ in range(2):                                 # deux tours complets, sans perte
        for a in range(len(reps)):
            for b in range(len(reps)):
                if a != b:
                    reps[b].fusion(reps[a].etat())


# ---- 1. compteurs ----------------------------------------------------------------
def compteurs():
    print("1. 10 000 incréments répartis sur 5 répliques ; anti-entropie toutes les")
    print("   10 opérations, 20 % de pertes, 20 % de doublons, désordre")
    print(f"   {'type':<26}valeurs des 5 répliques")
    for cls, nom in ((EntierLWW, "entier, dernier écrivain"), (EntierSomme, "entier, fusion par somme"),
                     (GCounter, "G-Counter")):
        reps, en_vol = [cls(i) for i in range(REPLIQUES)], []
        for t in range(1, 10_001):
            reps[alea.randrange(REPLIQUES)].incr(t)
            if t % 10 == 0:
                anti_entropie(reps, en_vol)
        synchroniser(reps, en_vol)
        print(f"   {nom:<26}{', '.join(str(r.valeur()) for r in reps)}")


# ---- 2. invariant ----------------------------------------------------------------
def stock():
    print("\n2. stock de 100 articles, PN-Counter ; 1 000 clients achètent sur une")
    print("   réplique au hasard si elle voit encore du stock")
    print(f"   {'échange tous les':<20}{'ventes':>8}{'stock final':>13}{'survendus':>11}")
    for periode in (1, 10, 50, 200, 1000):
        reps = [PNCounter(i) for i in range(REPLIQUES)]
        reps[0].ajouter(100)
        synchroniser(reps, [])
        ventes = 0
        for t in range(1, 1001):
            r = reps[alea.randrange(REPLIQUES)]
            if r.valeur() > 0:
                r.ajouter(-1)
                ventes += 1
            if t % periode == 0:
                synchroniser(reps, [])
        synchroniser(reps, [])
        v = reps[0].valeur()
        print(f"   {str(periode) + ' achats':<20}{ventes:>8}{v:>13}{max(0, -v):>11}")


# ---- 3. ensembles ----------------------------------------------------------------
TYPES = ((EnsembleLWW, "LWW"), (DeuxPhases, "2P-Set"), (ORSet, "OR-Set"))


def scenario(nom, actions):
    res = []
    for cls, _ in TYPES:
        reps = [cls(i) for i in range(2)]
        for act in actions:
            if act == "sync":
                synchroniser(reps, [])
            else:
                r, op, x, t = act
                (reps[r].ajouter if op == "+" else reps[r].retirer)(x, t)
        synchroniser(reps, [])
        v = reps[0].valeur()
        res.append("{" + ", ".join(sorted(v)) + "}" if v else "{}")
    print(f"   {nom:<41}" + "".join(f"{x:<9}" for x in res))


def ensembles():
    print("\n3. ensembles, deux répliques R0 et R1 (horodatages entre parenthèses)")
    print(f"   {'scénario':<41}" + "".join(f"{n:<9}" for _, n in TYPES))
    scenario("R0 +x(1) ; sync ; R0 −x(2) ∥ R1 +x(3)",
             [(0, "+", "x", 1), "sync", (0, "-", "x", 2), (1, "+", "x", 3)])
    scenario("R0 +x(1) ; sync ; R0 −x(3) ∥ R1 +x(2)",
             [(0, "+", "x", 1), "sync", (0, "-", "x", 3), (1, "+", "x", 2)])
    scenario("R0 +x(1) ; R0 −x(2) ; sync ; R1 +x(3)",
             [(0, "+", "x", 1), (0, "-", "x", 2), "sync", (1, "+", "x", 3)])
    scenario("R0 −x(9), horloge en avance ; R1 +x(5)",
             [(0, "+", "x", 1), "sync", (0, "-", "x", 9), "sync", (1, "+", "x", 5)])

    print("\n   20 000 opérations au hasard sur 100 éléments, 5 répliques, même réseau")
    print(f"   {'type':<14}{'convergent ?':<14}{'éléments':>9}")
    for cls, nom in ((EnsembleNaif, "naïf"),) + TYPES:
        reps, en_vol = [cls(i) for i in range(REPLIQUES)], []
        for t in range(1, 20_001):
            r = reps[alea.randrange(REPLIQUES)]
            x = f"e{alea.randrange(100)}"
            (r.ajouter if alea.random() < 0.6 else r.retirer)(x, (t, alea.random()))
            if t % 10 == 0:
                anti_entropie(reps, en_vol)
        synchroniser(reps, en_vol)
        vals = {r.valeur() for r in reps}
        print(f"   {nom:<14}{'oui' if len(vals) == 1 else f'non ({len(vals)} états)':<14}"
              f"{len(reps[0].valeur()):>9}")


# ---- 4. métadonnées ----------------------------------------------------------------
def metadonnees():
    print("\n4. taille des métadonnées")
    s = ORSet(0)
    print(f"   {'OR-Set, opérations':<24}{'éléments':>9}{'étiquettes':>11}{'retirées':>10}{'JSON (o)':>11}")
    for n in range(1, 100_001):
        x = f"e{alea.randrange(50)}"
        (s.ajouter if alea.random() < 0.5 else s.retirer)(x, 0)
        if n in (1_000, 10_000, 100_000):
            taille = len(json.dumps([s.ajouts, sorted(s.retraits)]))
            print(f"   {n:<24}{len(s.valeur()):>9}{len(s.ajouts):>11}{len(s.retraits):>10}{taille:>11}")
    simple = len(json.dumps(sorted(s.valeur())))
    print(f"   le même ensemble sans métadonnées : {simple} o")
    print("\n   G-Counter, un identifiant de 36 caractères et un compteur par réplique")
    print(f"   {'répliques':<24}{'JSON (o)':>11}")
    for n in (3, 100, 10_000, 1_000_000):
        etat = {str(uuid.UUID(int=i)): 1 for i in range(min(n, 10_000))}
        t = len(json.dumps(etat)) * (n / min(n, 10_000))
        print(f"   {n:<24}" + f"{t:,.0f}".replace(",", " ").rjust(11))


# ---- 5. texte ------------------------------------------------------------------------
def editer(octets):
    """Deux répliques éditent hors ligne, puis échangent leurs mises à jour.
    octets : positions converties en octets UTF-8 (l'unité de pycrdt) ou non."""
    from pycrdt import Doc, Text
    al = random.Random(5)
    base = Doc()
    base["t"] = Text("Le texte initial de la note.")
    docs = []
    for client in (1, 2):
        d = Doc(client_id=client)
        d["t"] = Text()
        d.apply_update(base.get_update())
        docs.append(d)

    def pos(s, i):
        return len(s[:i].encode()) if octets else i

    for d in docs:
        t = d["t"]
        for _ in range(5_000):
            s = str(t)
            n = len(s)
            if n > 20 and al.random() < 0.3:
                i = al.randrange(n - 5)
                k = al.randint(1, 5)
                del t[pos(s, i):pos(s, i + k)]
            else:
                i = al.randrange(n + 1)
                t.insert(pos(s, i), al.choice(["a", "le ", "x", "mot ", "é"]))
    etats = [d.get_state() for d in docs]
    maj = [docs[0].get_update(etats[1]), docs[1].get_update(etats[0])]
    docs[1].apply_update(maj[0])
    docs[0].apply_update(maj[1])
    return docs, maj


def texte():
    print("\n5. pycrdt (Yjs) : deux répliques font chacune 5 000 modifications hors ligne")
    docs, _ = editer(False)
    print(f"   positions en caractères : textes identiques après échange :"
          f" {str(docs[0]['t']) == str(docs[1]['t'])}")
    docs, maj = editer(True)
    t0, t1 = str(docs[0]["t"]), str(docs[1]["t"])
    print(f"   positions en octets UTF-8 : textes identiques après échange : {t0 == t1}")
    print(f"   longueur du texte : {len(t0)} caractères ({len(t0.encode())} o en UTF-8)")
    print(f"   mises à jour échangées : {len(maj[0])} o et {len(maj[1])} o")
    print(f"   document complet encodé : {len(docs[0].get_update())} o")
    i = 10
    for d, m in zip(docs, ("[A]", "[B]")):
        s = str(d["t"])
        d["t"].insert(len(s[:i].encode()), m)
    u0, u1 = docs[0].get_update(docs[1].get_state()), docs[1].get_update(docs[0].get_state())
    docs[1].apply_update(u0)
    docs[0].apply_update(u1)
    print(f"   insertions concurrentes au même endroit : {str(docs[0]['t'])[i:i + 6]!r}"
          f" et {str(docs[1]['t'])[i:i + 6]!r}")


if __name__ == "__main__":
    compteurs()
    stock()
    ensembles()
    metadonnees()
    texte()
