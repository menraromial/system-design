"""Chapitre 9 : latence de queue amplifiée par un appel en éventail.

Une requête interroge N serveurs en parallèle et attend la plus lente des N
réponses. La latence d'un serveur est tirée au hasard dans la distribution
réellement mesurée par file_attente.py (un serveur à 50 % de charge, service
moyen de 2 ms). On compare aussi les requêtes doublées (hedged requests) :
si un serveur n'a pas répondu au bout de son 95e centile, on envoie la même
demande à un second serveur et l'on garde la première réponse.
Usage : python3 eventail.py
"""
import random
import statistics

from file_attente import mesurer

random.seed(11)
print("mesure de la distribution d'un serveur à 50 % de charge (20 000 requêtes)…")
sejours = [x * 1000 for x in mesurer(0.5)[-1]]  # en ms
q = statistics.quantiles(sejours, n=100)
p50, p95, p99 = q[49], q[94], q[98]
print(f"un serveur : p50 {p50:.1f} ms, p95 {p95:.1f} ms, p99 {p99:.1f} ms")


def tirage():
    return random.choice(sejours)


def doublee():
    """renvoie (latence, nombre de requêtes envoyées)"""
    a = tirage()
    if a <= p95:
        return a, 1
    return min(a, p95 + tirage()), 2


ESSAIS = 20_000
print("> p99 : part des requêtes complètes qui dépassent le p99 d'un serveur seul")
print(f"{'serveurs':>8} {'p50':>6} {'p99':>6} {'> p99':>7} {'p99 doublé':>10} {'en plus':>8}")
for n in (1, 10, 100):
    simples, doubles, envois = [], [], 0
    for _ in range(ESSAIS):
        simples.append(max(tirage() for _ in range(n)))
        r = [doublee() for _ in range(n)]
        doubles.append(max(x for x, _ in r))
        envois += sum(k for _, k in r)
    qs, qd = statistics.quantiles(simples, n=100), statistics.quantiles(doubles, n=100)
    lents = sum(x > p99 for x in simples) / ESSAIS
    print(f"{n:8d} {qs[49]:6.1f} {qs[98]:6.1f} {100 * lents:5.1f} % {qd[98]:10.1f} "
          f"{100 * (envois / (ESSAIS * n) - 1):6.1f} %")
