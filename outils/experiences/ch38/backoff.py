"""Chapitre 38 : attentes entre les tentatives, comparées par simulation.

Calcul pur, sans réseau (une dizaine de secondes).

1. Contention optimiste, sur le modèle de l'article de Marc Brooker (AWS, 2015).
   N clients veulent modifier la même ligne au même instant. Une tentative lit
   la version courante, puis son écriture arrive au serveur après un délai
   réseau (moyenne 10 ms, écart type 2 ms) ; elle réussit si aucune autre
   écriture n'a réussi depuis sa lecture. La réponse revient après le même
   délai. Un client qui échoue attend selon la politique, puis recommence.
   On compte les tentatives (le travail du serveur) et la durée jusqu'au
   succès du dernier client, en moyenne sur 200 tirages.

2. Clients synchronisés par une coupure. 1 000 clients échouent ensemble à
   t = 0 ; le service est indisponible jusqu'à t = 2 s, puis il accepte au plus
   100 requêtes par tranche de 50 ms (2 000 par seconde) et refuse aussitôt
   les autres. On compte les tentatives par tranche de 50 ms.

    python3 backoff.py [--sortie ../../../figures/donnees/ch38-backoff.tex]
"""
import argparse
import heapq
import random
import statistics
from pathlib import Path

BASE, PLAFOND = 10.0, 1000.0          # ms


def attente(politique, n, precedente, alea):
    """Attente avant la tentative n + 1, après n échecs (n >= 1)."""
    expo = min(PLAFOND, BASE * 2 ** n)
    if politique == "immédiat":
        return 0.0
    if politique == "sans gigue":
        return expo
    if politique == "gigue complète":
        return alea.uniform(0, expo)
    if politique == "gigue égale":
        return expo / 2 + alea.uniform(0, expo / 2)
    if politique == "gigue décorrélée":
        return min(PLAFOND, alea.uniform(BASE, precedente * 3))
    raise ValueError(politique)


POLITIQUES = ["immédiat", "sans gigue", "gigue égale", "gigue complète", "gigue décorrélée"]


def contention(politique, n_clients, graine):
    alea = random.Random(graine)
    delai = lambda: max(1.0, alea.gauss(10, 2))
    evenements = []          # (instant d'arrivée de l'écriture, client, instant de lecture)
    for c in range(n_clients):
        heapq.heappush(evenements, (delai(), c, 0.0))
    echecs = [0] * n_clients
    precedente = [BASE] * n_clients
    derniere_reussite = -1.0
    tentatives, fin = 0, 0.0
    while evenements:
        t, c, lecture = heapq.heappop(evenements)
        tentatives += 1
        retour = t + delai()
        if derniere_reussite < lecture:
            derniere_reussite = t
            fin = max(fin, retour)
            continue
        echecs[c] += 1
        a = attente(politique, echecs[c], precedente[c], alea)
        precedente[c] = max(a, BASE)
        depart = retour + a
        heapq.heappush(evenements, (depart + delai(), c, depart))
    return tentatives, fin


def synchronises(politique, graine, n=1000, panne=2000.0, tranche=50.0, capacite=100):
    alea = random.Random(graine)
    file = [(0.0, c) for c in range(n)]      # tentatives (instant, client)
    echecs = [0] * n
    precedente = [BASE] * n
    acceptes = {}
    par_tranche = {}
    fin = 0.0
    heapq.heapify(file)
    while file:
        t, c = heapq.heappop(file)
        k = int(t // tranche)
        par_tranche[k] = par_tranche.get(k, 0) + 1
        if t >= panne and acceptes.get(k, 0) < capacite:
            acceptes[k] = acceptes.get(k, 0) + 1
            fin = max(fin, t)
            continue
        echecs[c] += 1
        a = attente(politique, echecs[c], precedente[c], alea)
        precedente[c] = max(a, BASE)
        heapq.heappush(file, (t + max(a, 1.0), c))
    return sum(par_tranche.values()), fin, par_tranche


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sortie")
    args = p.parse_args()

    print("1. contention optimiste : tentatives et durée jusqu'au dernier succès")
    print(f"   (base {BASE:g} ms, plafond {PLAFOND:g} ms, moyenne sur 200 tirages)")
    for n in (10, 50, 100):
        print(f"\n   {n} clients")
        print(f"   {'politique':<18}{'tentatives':>11}{'par client':>11}{'durée':>11}")
        for pol in POLITIQUES:
            res = [contention(pol, n, g) for g in range(200)]
            tent = statistics.mean(r[0] for r in res)
            duree = statistics.mean(r[1] for r in res)
            print(f"   {pol:<18}{tent:>11.0f}{tent / n:>11.1f}{duree:>9.0f}ms")

    print("\n2. 1 000 clients synchronisés, service absent pendant 2 s,")
    print("   puis 100 requêtes acceptées par tranche de 50 ms")
    print(f"   {'politique':<18}{'tentatives':>11}{'pic/50 ms':>10}{'dernier servi':>14}")
    courbes = {}
    for pol in POLITIQUES[1:]:
        tent, fin, tr = synchronises(pol, 1)
        courbes[pol] = tr
        pic = max(v for k, v in tr.items() if k * 50 >= 2000)
        print(f"   {pol:<18}{tent:>11}{pic:>10}{fin / 1000:>12.2f} s")

    if args.sortie:
        lignes = ["% Fichier généré par outils/experiences/ch38/backoff.py : ne pas modifier.",
                  "% tentatives par tranche de 50 ms, 1 000 clients synchronisés"]
        for pol, nom in (("sans gigue", "SansGigue"), ("gigue complète", "Complete")):
            tr = courbes[pol]
            pts = ",".join(f"{k}/{tr.get(k, 0)}" for k in range(0, 160))
            lignes.append(f"\\def\\tranches{nom}{{{pts}}}")
        Path(args.sortie).write_text("\n".join(lignes) + "\n")


if __name__ == "__main__":
    main()
