"""Chapitre 9 : histogramme des temps de séjour mesurés à 50 % de charge
(file_attente.py), écrit sous forme de coordonnées TikZ pour la figure 9.1.
Usage : python3 histogramme.py > ../../../figures/donnees/ch09-histogramme.tex
"""
import statistics
import sys

from file_attente import mesurer

sejours = [x * 1000 for x in mesurer(0.5)[-1]]
q = statistics.quantiles(sejours, n=1000)
largeur, bornes = 1.0, 40
compte = [0] * bornes
for x in sejours:
    compte[min(int(x // largeur), bornes - 1)] += 1
part = [100 * c / len(sejours) for c in compte]
print("% Fichier généré par outils/experiences/ch09/histogramme.py : ne pas modifier.")
print(f"% {len(sejours)} requêtes ; part des requêtes (%) par tranche de {largeur:g} ms")
print("\\def\\histo{" + ",".join(f"{i*largeur:g}/{p:.3f}" for i, p in enumerate(part)) + "}")
print(f"\\def\\moyenne{{{statistics.mean(sejours):.2f}}}")
print(f"\\def\\mediane{{{q[499]:.2f}}}")
print(f"\\def\\pquatrevingtdixneuf{{{q[989]:.2f}}}")
print(f"\\def\\pmaxpart{{{max(part):.3f}}}")
for nom, v in (("moyenne", statistics.mean(sejours)), ("mediane", q[499]), ("pquatrevingtdixneuf", q[989])):
    print(f"\\def\\{nom}texte{{{v:.1f}}}".replace(".", "{,}"))
print(f"mesure : moyenne {statistics.mean(sejours):.2f} ms, p50 {q[499]:.2f}, p99 {q[989]:.2f}, "
      f"p99,9 {q[998]:.2f}, max {max(sejours):.2f}", file=sys.stderr)
