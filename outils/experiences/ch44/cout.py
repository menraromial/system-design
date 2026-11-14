"""Chapitre 44 : point de bascule entre fonctions à la demande et machines
permanentes. Calcul pur, aux tarifs publiés par AWS pour us-east-1 (page
aws.amazon.com/lambda/pricing, relevée le 2026-11-14) :

  Lambda x86          0,0000166667 $ par Go-seconde, 0,20 $ par million d'appels
  m7g.xlarge          0,1632 $ de l'heure à la demande (4 vCPU, 16 Go)

Lambda facture la durée murale de l'appel, attente comprise, et n'accorde du
processeur qu'en proportion de la mémoire (un vCPU à 1 769 Mo, voir froid.py) :
durée = attente + processeur / min(1, mémoire / 1 769). Une machine ne « paie »
que le processeur consommé, car elle traite d'autres requêtes pendant qu'une
attend. On suppose une flotte d'au moins deux machines (une seule ne
survit pas à sa propre panne) remplies à 60 % au plus. Ni la passerelle HTTP,
ni le transfert réseau, ni l'offre gratuite ne sont comptés.

    python3 cout.py              (python3 cout.py --figure écrit aussi les
                                  coordonnées de la figure 44.3)
"""
import math
import sys
from pathlib import Path

PRIX_GO_S = 0.0000166667
PRIX_APPEL = 0.20 / 1e6
PRIX_MACHINE_H = 0.1632
VCPU_MACHINE = 4
REMPLISSAGE = 0.60
MIN_MACHINES = 2
HEURES_MOIS = 730
S_MOIS = HEURES_MOIS * 3600

# nom, processeur consommé (s), attente d'entrées-sorties (s), mémoire (Mo)
PROFILS = [("routage", 0.002, 0.010, 128),
           ("API", 0.020, 0.080, 512),
           ("calcul", 1.000, 0.000, 1769)]
DEBITS = [1, 10, 100, 1000]


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def duree_lambda(cpu, attente, memoire):
    return attente + cpu / min(1.0, memoire / 1769)


def par_appel_lambda(cpu, attente, memoire):
    return duree_lambda(cpu, attente, memoire) * memoire / 1024 * PRIX_GO_S + PRIX_APPEL


def machines(debit, cpu):
    par_machine = VCPU_MACHINE * REMPLISSAGE / cpu         # req/s par machine
    return max(MIN_MACHINES, math.ceil(debit / par_machine)), par_machine


def flotte_mois(debit, cpu):
    return machines(debit, cpu)[0] * PRIX_MACHINE_H * HEURES_MOIS


def bascule(cpu, attente, memoire):
    """Premier débit moyen (req/s) à partir duquel la flotte coûte moins cher."""
    debit = 0.01
    while debit < 1e5:
        if debit * S_MOIS * par_appel_lambda(cpu, attente, memoire) > flotte_mois(debit, cpu):
            return debit
        debit *= 1.001
    return None


def main():
    print("coût mensuel ($) à débit moyen constant ; Lambda x86 ; flotte d'au")
    print("moins 2 m7g.xlarge remplies à 60 % ; us-east-1\n")
    for nom, cpu, attente, memoire in PROFILS:
        n, par_machine = machines(1, cpu)
        d = duree_lambda(cpu, attente, memoire)
        lam = 1e6 * par_appel_lambda(cpu, attente, memoire)
        mac = 1e6 * PRIX_MACHINE_H / 3600 / par_machine
        print(f"{nom} : {nb(cpu * 1000)} ms de processeur, {nb(attente * 1000)} ms"
              f" d'attente, {nb(memoire)} Mo")
        print(f"   durée facturée {nb(d * 1000)} ms ; le million d'appels : Lambda"
              f" {lam:.2f} $,")
        print(f"   machine pleine {mac:.2f} $ ({nb(par_machine)} req/s par machine)")
        print(f"   {'req/s':>7}{'appels/mois':>14}{'Lambda':>11}{'machines':>10}{'flotte':>10}")
        for debit in DEBITS:
            print(f"   {nb(debit):>7}{nb(debit * S_MOIS / 1e6) + ' M':>14}"
                  f"{nb(debit * S_MOIS * par_appel_lambda(cpu, attente, memoire)):>11}"
                  f"{machines(debit, cpu)[0]:>10}{nb(flotte_mois(debit, cpu)):>10}")
        b = bascule(cpu, attente, memoire)
        print(f"   bascule à {nb(b, 1)} req/s en moyenne, {nb(b * S_MOIS / 1e6, 1)}"
              f" millions d'appels par mois\n" if b else "   pas de bascule\n")


def figure():
    """Coordonnées log-log (décades) de la figure 44.3, pour les profils API et calcul."""
    lignes = []
    for nom, cpu, attente, memoire in PROFILS[1:]:
        cle = {"API": "Api", "calcul": "Calcul"}[nom]
        lam = [(x, math.log10(10 ** x * S_MOIS * par_appel_lambda(cpu, attente, memoire)))
               for x in (0, 3)]
        marches, x = [], 0.0
        while x <= 3:
            debit = 10 ** x
            marches.append((x, math.log10(flotte_mois(debit, cpu))))
            x += 0.005
        pts = [marches[0]] + [p for i, p in enumerate(marches[1:], 1)
                              if abs(p[1] - marches[i - 1][1]) > 1e-9 or i == len(marches) - 1]
        coords = []
        for i, (x, y) in enumerate(pts):            # marches : verticale puis palier
            if i:
                coords.append((x, coords[-1][1]))
            coords.append((x, y))
        b = bascule(cpu, attente, memoire)
        f = lambda c: " ".join(f"({x:.3f},{y:.3f})" for x, y in c)
        lignes += [f"\\def\\lambda{cle}{{{f(lam)}}}", f"\\def\\flotte{cle}{{{f(coords)}}}",
                   f"\\def\\bascule{cle}{{{math.log10(b):.3f}}}"]
    sortie = Path(__file__).resolve().parents[3] / "figures" / "donnees" / "ch44-bascule.tex"
    sortie.write_text("% produit par outils/experiences/ch44/cout.py --figure\n"
                      + "\n".join(lignes) + "\n")


if __name__ == "__main__":
    main()
    if "--figure" in sys.argv:
        figure()
