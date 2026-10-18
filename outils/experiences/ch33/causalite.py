"""Chapitre 33 : horodatages physiques, Lamport, vectoriels, HLC et TrueTime.

Simulation à événements discrets. n processus ont chacun une horloge
physique décalée d'au plus ± ε et qui dérive de ± 50 ppm. Chacun produit des
événements à des instants aléatoires : un événement local, ou l'envoi d'un
message à un autre processus, reçu après un délai réseau de 0,1 à 1 ms.
Chaque événement reçoit quatre horodatages : l'horloge physique locale, une
horloge de Lamport, une horloge vectorielle et une horloge hybride (HLC,
Kulkarni et al., 2014). Les horloges vectorielles donnent la relation
« s'est produit avant » exacte, qui sert de référence.

La dernière partie simule l'attribution d'horodatages de validation par deux
serveurs dont les horloges sont fausses d'au plus ε, sans et avec l'attente
de validation de TrueTime (Spanner).

    python3 causalite.py [graine]
"""
import bisect
import heapq
import random
import statistics
import sys


def simuler(n, epsilon, duree, rng):
    decalage = [rng.uniform(-epsilon, epsilon) for _ in range(n)]
    derive = [rng.uniform(-50e-6, 50e-6) for _ in range(n)]

    def physique(i, t):
        return t + decalage[i] + derive[i] * t

    lamport = [0] * n
    vecteur = [[0] * n for _ in range(n)]
    hlc = [(0.0, 0) for _ in range(n)]      # (l, c)
    evenements = []                         # (i, vrai t, phys, lamport, vect, hlc)
    messages = inverses = 0                 # reçus « avant » leur envoi
    file = []
    for i in range(n):
        heapq.heappush(file, (rng.expovariate(1000), "local", i, None))
    while file:
        t, sorte, i, msg = heapq.heappop(file)
        if t > duree:
            break
        pt = physique(i, t)
        vecteur[i][i] += 1
        if sorte == "recu":
            l_m, v_m, h_m, pt_m = msg
            messages += 1
            inverses += pt < pt_m
            lamport[i] = max(lamport[i], l_m) + 1
            vecteur[i] = [max(a, b) for a, b in zip(vecteur[i], v_m)]
            l0, c0 = hlc[i]
            lm, cm = h_m
            l = max(l0, lm, pt)
            if l == l0 == lm:
                c = max(c0, cm) + 1
            elif l == l0:
                c = c0 + 1
            elif l == lm:
                c = cm + 1
            else:
                c = 0
            hlc[i] = (l, c)
        else:
            lamport[i] += 1
            l0, c0 = hlc[i]
            l = max(l0, pt)
            hlc[i] = (l, c0 + 1 if l == l0 else 0)
        evenements.append((i, t, pt, lamport[i], tuple(vecteur[i]), hlc[i]))
        if sorte != "recu":
            # le prochain événement local de ce processus
            heapq.heappush(file, (t + rng.expovariate(1000), "local", i, None))
            if rng.random() < 0.5:          # cet événement envoie un message
                j = rng.choice([x for x in range(n) if x != i])
                heapq.heappush(file, (t + rng.uniform(1e-4, 1e-3), "recu", j,
                                      (lamport[i], tuple(vecteur[i]), hlc[i],
                                       pt)))
    return evenements, messages, inverses


def avant(va, vb):
    return all(a <= b for a, b in zip(va, vb)) and va != vb


def comparer(n, epsilon, rng, paires=200_000, fenetre=0.005):
    """Paires d'événements séparés de moins de `fenetre` en temps réel."""
    ev, messages, inverses = simuler(n, epsilon, 2.0, rng)
    temps = [e[1] for e in ev]
    causales = concurrentes = 0
    faux_phys = faux_lamport = faux_hlc = 0
    for _ in range(paires):
        k = rng.randrange(len(ev))
        lo = bisect.bisect_left(temps, temps[k] - fenetre)
        hi = bisect.bisect_right(temps, temps[k] + fenetre)
        j = rng.randrange(lo, hi)
        if j == k:
            continue
        a, b = ev[k], ev[j]
        if avant(b[4], a[4]):
            a, b = b, a
        if avant(a[4], b[4]):
            causales += 1
            faux_phys += a[2] >= b[2]
            faux_lamport += a[3] >= b[3]
            faux_hlc += a[5] >= b[5]
        else:
            concurrentes += 1
    avance = max(h[0] - pt for _, _, pt, _, _, h in ev)
    cmax = max(h[1] for *_, h in ev)
    return (messages, inverses, causales, concurrentes, faux_phys,
            faux_lamport, faux_hlc, avance, cmax)


def truetime(epsilon, avec_attente, rng, essais=20_000):
    """T1 valide sur le serveur A ; dès que le client a la réponse, il lance
    T2 sur le serveur B. L'ordre réel est T1 puis T2 ; on compte les cas où
    s2 < s1 (cohérence externe violée) et l'attente ajoutée."""
    violations, attentes = 0, []
    for _ in range(essais):
        erreur_a, erreur_b = (rng.uniform(-epsilon, epsilon) for _ in range(2))
        t = 0.0                             # temps réel du début de T1
        s1 = t + erreur_a + (epsilon if avec_attente else 0)
        if avec_attente:                    # attendre TT.after(s1)
            # TT.now().earliest = horloge - ε dépasse s1
            fin1 = s1 + epsilon - erreur_a
            attentes.append(fin1 - t)
        else:
            fin1 = t
        t2 = fin1 + 0.0005                  # le client lance T2 0,5 ms après
        s2 = t2 + erreur_b + (epsilon if avec_attente else 0)
        violations += s2 < s1
    return violations, (statistics.mean(attentes) if attentes else 0.0)


def main():
    graine = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    rng = random.Random(graine)
    print(f"simulation, graine {graine} ; 5 processus, 1 000 événements/s "
          f"chacun pendant 2 s ;")
    print("délai réseau 0,1 à 1 ms ; un événement sur deux envoie un message")
    print("\n1. ordre causal et horodatages (paires séparées de moins de 5 ms)")
    print(f"   {'ε':>7} {'msg inversés':>13} {'causales':>9} {'concur.':>8} "
          f"{'physique':>9} {'Lamport':>8} {'HLC':>4}")
    resultats = []
    for eps in (0.0, 1e-4, 1e-3, 1e-2, 1e-1):
        r = comparer(5, eps, rng)
        resultats.append((eps, r))
        msg, inv, ca, co, fp, fl, fh, av, cm = r
        print(f"   {eps * 1000:5.1f}ms {100 * inv / msg:11.2f} % {ca:9} {co:8} "
              f"{100 * fp / ca:7.2f} % {fl:8} {fh:4}")
    print("\n   horloge hybride (l, c) : avance de l sur l'horloge physique")
    print(f"   {'ε':>7} {'avance max de l':>16} {'c max':>6}")
    for eps, r in resultats:
        print(f"   {eps * 1000:5.1f}ms {r[7] * 1000:13.2f} ms {r[8]:6}")
    print("\n2. taille d'un horodatage transmis avec chaque message")
    for n in (5, 100, 1000):
        print(f"   {n:5} processus : Lamport 8 o, HLC 12 o, vecteur "
              f"{8 * n} o")
    print("\n3. horodatages de validation sur deux serveurs (20 000 paires)")
    print(f"   {'ε':>6} {'sans attente':>14} {'avec attente':>14} "
          f"{'attente moyenne':>16}")
    for eps in (0.001, 0.004, 0.007):
        v0, _ = truetime(eps, False, rng)
        v1, att = truetime(eps, True, rng)
        print(f"   {eps * 1000:4.0f}ms {v0:9} viol. {v1:9} viol. "
              f"{att * 1000:12.1f} ms")


if __name__ == "__main__":
    main()
