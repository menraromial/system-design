"""Chapitre 48 : fenêtres en temps de traitement et en temps d'événement, et
compromis du filigrane (watermark), sur un flux de clics dans le désordre
(evenements.py : 10 minutes à 1 000 événements par seconde, fenêtres de 10 s).

Les événements sont traités dans leur ordre d'arrivée, comme par un moteur de
flux. On compare :
  1. des fenêtres découpées selon l'instant d'arrivée (temps de traitement) et
     selon l'instant de l'événement (temps d'événement) : écart aux vrais comptes ;
  2. un filigrane égal au plus grand temps d'événement vu moins un délai : une
     fenêtre est émise quand le filigrane dépasse sa fin, les événements arrivés
     après sont perdus ; part perdue et attente du résultat selon le délai ;
  3. un délai court suivi de corrections (allowed lateness) : la fenêtre est
     émise tôt, puis réémise à chaque événement tardif accepté.

    python3 fenetres.py        (une demi-minute ; --figure écrit les points de la
                                figure 48.1)
"""
import statistics
import sys
from pathlib import Path

from evenements import generer, verite

FENETRE = 10


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def temps_de_traitement(evts, vrai):
    comptes = {}
    for arr, _, page in evts:
        cle = (page, int(arr // FENETRE) * FENETRE)
        comptes[cle] = comptes.get(cle, 0) + 1
    ecarts = [abs(comptes.get(k, 0) - n) / n for k, n in vrai.items()]
    faux = sum(1 for k, n in vrai.items() if comptes.get(k, 0) != n)
    return faux, statistics.mean(ecarts)


def filigrane(evts, vrai, delai, tolerance=0.0):
    """Émet chaque fenêtre quand le filigrane passe sa fin ; accepte ensuite les
    retardataires pendant « tolerance » secondes de filigrane (réémission)."""
    max_ev = float("-inf")
    ouvertes, emises = {}, {}
    perdus = reemissions = 0
    attentes = []
    for arr, t, page in evts:
        debut = int(t // FENETRE) * FENETRE
        cle = (page, debut)
        wm = max_ev - delai
        if debut + FENETRE <= wm:                         # fenêtre déjà émise
            if debut + FENETRE + tolerance > wm:
                emises[cle] = emises.get(cle, 0) + 1
                reemissions += 1
            else:
                perdus += 1
        else:
            ouvertes[cle] = ouvertes.get(cle, 0) + 1
        max_ev = max(max_ev, t)
        wm = max_ev - delai
        for k in [k for k in ouvertes if k[1] + FENETRE <= wm]:
            emises[k] = emises.get(k, 0) + ouvertes.pop(k)
            attentes.append(arr - (k[1] + FENETRE))          # attente après la fin de la fenêtre
    for k, n in ouvertes.items():
        emises[k] = emises.get(k, 0) + n
    exactes = sum(1 for k, n in vrai.items() if emises.get(k, 0) == n)
    return perdus, exactes, statistics.median(attentes), reemissions


def main():
    evts = generer(600, 1000)
    vrai = verite(evts, FENETRE)
    print(f"{nb(len(evts))} événements, {nb(len(vrai))} fenêtres (page, 10 s)")
    faux, ecart = temps_de_traitement(evts, vrai)
    print("\n1. fenêtres en temps de traitement (selon l'arrivée)")
    print(f"   comptes faux : {nb(faux)} fenêtres sur {nb(len(vrai))} ;"
          f" écart moyen {100 * ecart:.1f} %")
    print("\n2. temps d'événement, filigrane = plus grand temps vu - délai")
    print(f"   {'délai':>7}{'perdus':>9}{'part':>8}{'fenêtres exactes':>18}{'attente méd.':>14}")
    for delai in (0, 0.5, 1, 2, 5, 10, 30, 60):
        perdus, exactes, attente, _ = filigrane(evts, vrai, delai)
        print(f"   {delai:>6}s{nb(perdus):>9}{100 * perdus / len(evts):>7.2f}%"
              f"{100 * exactes / len(vrai):>17.1f}%{attente:>12.1f} s")
    print("\n3. délai de 2 s, puis corrections pendant une tolérance")
    print(f"   {'tolérance':>9}{'perdus':>9}{'réémissions':>13}{'fenêtres exactes':>18}")
    for tol in (0, 10, 60):
        perdus, exactes, _, re = filigrane(evts, vrai, 2, tol)
        print(f"   {tol:>8}s{nb(perdus):>9}{nb(re):>13}{100 * exactes / len(vrai):>17.1f}%")


def figure():
    """Points (arrivée, temps d'événement) de 40 s de flux à 15 événements par
    seconde, filigrane à 5 s, et événements arrivés derrière le filigrane."""
    evts = generer(40, 15, graine=7)
    max_ev, fil, normaux, tard = float("-inf"), [], [], []
    for arr, t, _ in evts:
        wm = max_ev - 5
        (tard if t < wm else normaux).append((arr, t))
        max_ev = max(max_ev, t)
        fil.append((arr, max(max_ev - 5, 0)))
    f = lambda pts: " ".join(f"({a:.2f},{b:.2f})" for a, b in pts if a <= 44)
    sortie = Path(__file__).resolve().parents[3] / "figures" / "donnees" / "ch48-points.tex"
    sortie.write_text("% produit par outils/experiences/ch48/fenetres.py --figure\n"
                      f"\\def\\normaux{{{f(normaux)}}}\n\\def\\tardifs{{{f(tard)}}}\n"
                      f"\\def\\filigrane{{{f(fil[::3])}}}\n")
    print(len(normaux), "normaux,", len(tard), "tardifs")


if __name__ == "__main__":
    if "--figure" in sys.argv:
        figure()
    else:
        main()
