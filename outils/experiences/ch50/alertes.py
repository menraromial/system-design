"""Chapitre 50 : alertes sur un seuil de taux d'erreur et alertes sur la vitesse de
consommation du budget d'erreur (SRE workbook, « Alerting on SLOs »).

Simulation minute par minute de 30 jours (plus 3 jours d'historique) d'un
service à 1 000 requêtes par seconde en moyenne, avec un cycle jour-nuit
(0,4 à 1,6 fois la moyenne), un SLO de 99,9 % sur 30 jours et :
  - un bruit de fond de 0,005 % d'erreurs ;
  - 4 bouffées par jour en moyenne, de 1 à 3 minutes, à 0,5 à 2 % d'erreurs
    (redémarrage d'une instance, déploiement) : elles ne méritent pas de page ;
  - quatre incidents : A, panne totale de 10 minutes ; B, 5 % d'erreurs pendant
    1 heure ; C, 1 % pendant 6 heures ; D, 0,3 % pendant 3 jours.

Pour chaque règle : épisodes d'alerte (minutes en alerte consécutives, à
5 minutes près), fausses alertes (épisode qui ne recouvre aucun incident),
délai de détection de chaque incident, délai de réarmement après sa fin.

    python3 alertes.py [graine]   (quelques secondes)
"""
import sys

import numpy as np

SLO = 0.999
BUDGET = 1 - SLO
JOUR = 1440
HIST = 3 * JOUR
N = 30 * JOUR


def simuler(graine):
    r = np.random.default_rng(graine)
    m = np.arange(HIST + N)
    debit = 60_000 * (1 + 0.6 * np.sin(2 * np.pi * (m % JOUR) / JOUR - np.pi / 2))   # requêtes par minute
    taux = np.full(HIST + N, 0.00005)
    bouffees = 0
    for jour in range(33):
        for _ in range(r.poisson(4)):
            debut = jour * JOUR + r.integers(JOUR)
            taux[debut:debut + r.integers(1, 4)] = r.uniform(0.005, 0.02)
            bouffees += jour >= 3
    incidents = {}
    for nom, jour, duree, t in (("A", 4, 10, 1.0), ("B", 9, 60, 0.05), ("C", 15, 360, 0.01),
                                ("D", 20, 3 * JOUR, 0.003)):
        debut = HIST + jour * JOUR + int(r.integers(JOUR))
        taux[debut:debut + duree] = t
        incidents[nom] = (debut, debut + duree, t)
    requetes = r.poisson(debit)
    erreurs = r.binomial(requetes, np.minimum(taux, 1.0))
    return requetes, erreurs, incidents, bouffees


def fenetre(x, w):
    """Somme glissante sur les w dernières minutes (minute courante comprise)."""
    c = np.concatenate([[0], np.cumsum(x)])
    i = np.arange(len(x))
    return c[i + 1] - c[np.maximum(0, i + 1 - w)]


def conso(req, err, w):
    """Vitesse de consommation sur w minutes : taux d'erreur / budget."""
    return fenetre(err, w) / np.maximum(1, fenetre(req, w)) / BUDGET


def episodes(alerte):
    idx = np.flatnonzero(alerte)
    eps = []
    for i in idx:
        if eps and i - eps[-1][1] <= 5:
            eps[-1][1] = i
        else:
            eps.append([i, i])
    return eps


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def duree(m):
    if m is None:
        return "-"
    return f"{m} min" if m < 120 else f"{m / 60:.1f} h"


def main():
    graine = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    req, err, inc, bouffees = simuler(graine)
    total_budget = BUDGET * req[HIST:].sum()
    print(f"1. 30 jours, {nb(req[HIST:].sum() / 1e6)} millions de requêtes, SLO 99,9 %,")
    print(f"   budget de {nb(total_budget)} erreurs (graine {graine})")
    print(f"   {'source':<38}{'erreurs':>11}{'part du budget':>16}")
    masque = np.zeros(len(req), bool)
    for nom, (d, f, t) in inc.items():
        masque[d:f] = True
        e = err[d:f].sum()
        desc = {"A": "A, panne totale, 10 min", "B": "B, 5 % pendant 1 h", "C": "C, 1 % pendant 6 h",
                "D": "D, 0,3 % pendant 3 jours"}[nom]
        print(f"   {desc:<38}{nb(e):>11}{100 * e / total_budget:>14.1f} %")
    hors = err[HIST:][~masque[HIST:]].sum()
    print(f"   {f'bruit de fond et {bouffees} bouffées':<38}{nb(hors):>11}{100 * hors / total_budget:>14.1f} %")
    tot = err[HIST:].sum()
    print(f"   {'total':<38}{nb(tot):>11}{100 * tot / total_budget:>14.1f} %")

    taux = lambda w: fenetre(err, w) / np.maximum(1, fenetre(req, w))
    regles = {
        "seuil 0,1 % sur 10 min": taux(10) >= BUDGET,
        "seuil 1 % sur 5 min": taux(5) >= 0.01,
        "conso. ≥ 14,4 sur 1 h": conso(req, err, 60) >= 14.4,
        "multifenêtre, pages": ((conso(req, err, 60) >= 14.4) & (conso(req, err, 5) >= 14.4))
                               | ((conso(req, err, 360) >= 6) & (conso(req, err, 30) >= 6)),
        "multifenêtre, tickets": (conso(req, err, 3 * JOUR) >= 1) & (conso(req, err, 360) >= 1),
    }
    print("\n2. délai de détection de chaque incident, réarmement médian après la fin")
    print(f"   {'règle':<22}{'alertes':>7}{'fausses':>8}{'A':>7}{'B':>7}{'C':>7}{'D':>7}{'réarm.':>7}")
    for nom, a in regles.items():
        eps = [e for e in episodes(a) if e[1] >= HIST]
        faux = sum(1 for d, f in eps if not any(d <= fi + 60 and f >= di for di, fi, _ in inc.values()))
        det, rearm = [], []
        for di, fi, _ in inc.values():
            premiers = [d for d, f in eps if d <= fi and f >= di]
            if premiers:
                det.append(max(0, min(premiers) - di))
                fin_ep = max(f for d, f in eps if d <= fi + 60 and f >= di)
                rearm.append(max(0, fin_ep - fi))
            else:
                det.append(None)
        rearm.sort()
        r_med = duree(rearm[len(rearm) // 2]) if rearm else "-"
        print(f"   {nom:<22}{len(eps):>7}{faux:>8}" + "".join(f"{duree(x):>7}" for x in det) + f"{r_med:>7}")

    if len(sys.argv) > 2:                      # données de la figure : budget consommé, heure par heure
        cumul = np.cumsum(err[HIST:]) / total_budget * 100
        lignes = ["% produit par outils/experiences/ch50/alertes.py"]
        pts = " ".join(f"({h / 24:.3f},{cumul[min(N - 1, h * 60 + 59)]:.2f})" for h in range(0, 30 * 24, 2))
        lignes.append(f"\\def\\courbebudget{{{pts}}}")
        for nom, cle in (("seuil 0,1 % sur 10 min", "seuil"), ("multifenêtre, pages", "pages"),
                         ("multifenêtre, tickets", "tickets")):
            eps = [e for e in episodes(regles[nom]) if e[1] >= HIST]
            lignes.append(f"\\def\\nombre{cle}{{{len(eps)}}}")
            lignes.append(f"\\def\\alertes{cle}{{" + ",".join(
                f"{max(0, d - HIST) / JOUR:.3f}/{(f + 1 - HIST) / JOUR:.3f}" for d, f in eps) + "}")
        lignes.append("\\def\\incidents{" + ",".join(
            f"{n}/{(d - HIST) / JOUR:.3f}/{(f - HIST) / JOUR:.3f}" for n, (d, f, _) in inc.items()) + "}")
        open(sys.argv[2], "w").write("\n".join(lignes) + "\n")


if __name__ == "__main__":
    main()
