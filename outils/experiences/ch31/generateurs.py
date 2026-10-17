"""Chapitre 31 : générateurs d'identifiants uniques, en Python seul.

1. vitesse de génération et taille de chaque format ;
2. ordre : part des identifiants consécutifs croissants ;
3. Snowflake : épuisement de la séquence, horloge qui recule, décalage
   d'horloge entre deux machines (horloge simulée) ;
4. ce que révèle un identifiant : date de création, volume (problème des
   chars allemands).

    python3 generateurs.py
"""
import os
import random
import secrets
import time
import uuid

CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
EPOCH_TWITTER = 1288834974657           # ms, l'époque de Snowflake chez Twitter


def ms():
    return time.time_ns() // 1_000_000


def ulid_texte(n):
    return "".join(CROCKFORD[(n >> (5 * i)) & 31] for i in range(25, -1, -1))


class ULID:
    """48 bits de date en ms, 80 bits aléatoires ; en mode monotone, le
    même milliseconde incrémente la partie aléatoire."""

    def __init__(self, monotone):
        self.monotone, self.dernier_ms, self.alea = monotone, -1, 0

    def suivant(self):
        t = ms()
        if self.monotone and t == self.dernier_ms:
            self.alea += 1
            if self.alea >> 80:
                raise OverflowError("ULID : partie aléatoire épuisée")
        else:
            self.alea = int.from_bytes(os.urandom(10))
        self.dernier_ms = t
        return (t << 80) | self.alea


class Snowflake:
    """41 bits de ms depuis l'époque, 10 bits de machine, 12 de séquence."""

    def __init__(self, machine, horloge=ms, protege=True):
        self.machine, self.horloge, self.protege = machine, horloge, protege
        self.dernier, self.seq = -1, 0
        self.attentes = 0                   # millisecondes attendues

    def suivant(self):
        t = self.horloge()
        if t < self.dernier and self.protege:
            while t < self.dernier:         # l'horloge a reculé : attendre
                self.attentes += 1
                t = self.horloge()
        if t == self.dernier:
            self.seq = (self.seq + 1) & 4095
            if self.seq == 0:               # 4 096 dans la même ms : attendre
                while t <= self.dernier:
                    self.attentes += 1
                    t = self.horloge()
        else:
            self.seq = 0
        self.dernier = t
        return ((t - EPOCH_TWITTER) << 22) | (self.machine << 12) | self.seq


def vitesse():
    print("1. génération (un fil, 300 000 identifiants)")
    u_mono, sf = ULID(True), Snowflake(1)
    formats = [
        ("compteur en mémoire", None, "8 o", "19 car."),
        ("UUID v4", uuid.uuid4, "16 o", "36 car."),
        ("UUID v7", uuid.uuid7, "16 o", "36 car."),
        ("ULID monotone", u_mono.suivant, "16 o", "26 car."),
        ("Snowflake", sf.suivant, "8 o", "19 car."),
        ("128 bits de secrets", lambda: secrets.token_bytes(16), "16 o",
         "22 car."),
    ]
    compteur = iter(range(10**9))
    for nom, f, binaire, texte in formats:
        f = f or compteur.__next__
        debut = time.perf_counter()
        for _ in range(300_000):
            f()
        d = time.perf_counter() - debut
        print(f"   {nom:22} {300_000 / d / 1e6:5.2f} M/s  "
              f"{d / 300_000 * 1e9:5.0f} ns  {binaire:>5} {texte:>8}")
    print(f"   exemples : v4 {uuid.uuid4()}")
    print(f"              v7 {uuid.uuid7()}")
    print(f"              ULID {ulid_texte(u_mono.suivant())}, "
          f"Snowflake {sf.suivant()}")


def ordre():
    print("\n2. ordre : part des paires consécutives croissantes (1 million)")
    u_alea, u_mono, sf = ULID(False), ULID(True), Snowflake(1)
    for nom, f in [("UUID v4", lambda: uuid.uuid4().int),
                   ("UUID v7", lambda: uuid.uuid7().int),
                   ("ULID non monotone", u_alea.suivant),
                   ("ULID monotone", u_mono.suivant),
                   ("Snowflake", sf.suivant)]:
        prec, croissants, n = f(), 0, 1_000_000
        for _ in range(n):
            v = f()
            croissants += v > prec
            prec = v
        print(f"   {nom:22} {100 * croissants / n:7.3f} %")


class Horloge:
    """Horloge simulée, en millisecondes, que le test fait avancer."""

    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def snowflake_limites():
    print("\n3. Snowflake, horloge simulée")
    # séquence épuisée : 10 000 identifiants demandés d'un coup ; chaque
    # lecture de l'horloge fait passer 0,1 µs de temps réel
    reel = [0.0]

    def horloge():
        reel[0] += 0.0001
        return 1_800_000_000_000 + int(reel[0])
    sf = Snowflake(7, horloge)
    ids = [sf.suivant() for _ in range(10_000)]
    par_ms = {}
    for i in ids:
        par_ms[i >> 22] = par_ms.get(i >> 22, 0) + 1
    print(f"   10 000 identifiants demandés d'un coup : {len(set(ids))} "
          f"distincts, sur {len(par_ms)} ms")
    print(f"      par ms : {sorted(par_ms.values(), reverse=True)} ; "
          f"durée {reel[0]:.2f} ms (1.00 sans attente)")
    # horloge qui recule de 5 ms au bout de 20 ms ; chaque lecture de
    # l'horloge fait passer 0,1 ms de temps réel (10 identifiants par ms)
    for protege in (False, True):
        reel = [0.0]

        def horloge():
            reel[0] += 0.1
            recul = 5 if reel[0] >= 20 else 0
            return 1_800_000_000_000 + int(reel[0]) - recul
        sf = Snowflake(7, horloge, protege)
        ids = [sf.suivant() for _ in range(400)]
        doublons = len(ids) - len(set(ids))
        nom = "protégé (attend)" if protege else "naïf"
        print(f"   horloge reculée de 5 ms, {nom:16} : "
              f"{doublons:3} doublons sur {len(ids)}")
    # décalage d'horloge entre deux machines : 100 000 événements en 10 s,
    # chacun sur une machine tirée au hasard ; la machine 1 avance de d ms
    from bisect import bisect_left
    rng = random.Random(1)
    vrai = sorted(rng.uniform(0, 10_000) for _ in range(100_000))
    machines = [rng.randrange(2) for _ in vrai]
    for decalage in (0, 1, 10, 50):
        evts = [(int(t + (decalage if m else 0)) << 22) | (m << 12)
                for t, m in zip(vrai, machines)]
        # minimum des identifiants à partir de chaque position
        mins = evts[:]
        for i in range(len(mins) - 2, -1, -1):
            mins[i] = min(mins[i], mins[i + 1])
        pire, inverses = 0.0, 0
        for i, e in enumerate(evts):
            # dernier j > i dont l'identifiant est plus petit que e
            j = bisect_left(mins, e, lo=i + 1) - 1
            if j > i:
                inverses += 1
                pire = max(pire, vrai[j] - vrai[i])
        print(f"   machine 1 en avance de {decalage:2} ms : {100 * inverses / len(evts):5.1f} % "
              f"des événements")
        print(f"      classés après un plus récent ; écart réel max "
              f"{pire:5.1f} ms")


def revelations():
    print("\n4. ce que révèle un identifiant")
    u = uuid.uuid7()
    t = u.int >> 80
    print(f"   UUID v7 {u}")
    print(f"      créé le "
          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t / 1000))}"
          f".{t % 1000:03d} UTC")
    s = 175928847299117063                  # exemple de la documentation Discord
    t = (s >> 22) + 1420070400000
    print(f"   Snowflake de Discord {s}")
    print(f"      créé le "
          f"{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t / 1000))}"
          f".{t % 1000:03d} UTC, processus {(s >> 12) & 31}, "
          f"incrément {s & 4095}")
    # problème des chars allemands : estimer le nombre de commandes
    print("   184 000 commandes numérotées ; estimation m + m/k - 1 sur 10 000"
          " tirages")
    rng = random.Random(1)
    vrai = 184_000
    for k in (5, 20, 100):
        erreurs = []
        for _ in range(10_000):
            echantillon = rng.sample(range(1, vrai + 1), k)
            m = max(echantillon)
            estime = m + m / k - 1
            erreurs.append(100 * (estime - vrai) / vrai)
        erreurs.sort()
        print(f"   {k:3} numéros vus : erreur de {erreurs[500]:+5.1f} % à "
              f"{erreurs[9500]:+5.1f} % (5e-95e centiles)")


if __name__ == "__main__":
    vitesse()
    ordre()
    snowflake_limites()
    revelations()
