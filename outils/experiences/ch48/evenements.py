"""Chapitre 48 : flux d'événements de clics dans le désordre, commun aux scripts.

Chaque événement naît à un instant (son temps d'événement) et arrive au système
plus tard (son temps d'arrivée). Le retard d'arrivée suit un mélange : 90 % des
événements arrivent en moins de 0,5 s, 9 % entre 0,5 et 4 s (réseau mobile
lent), 1 % entre 4 et 40 s (téléphone hors ligne qui renvoie ses clics plus
tard). Les pages suivent une loi de Zipf sur 50 pages.
"""
import random


def generer(duree_s, debit, graine=48):
    """Liste de (arrivée_s, temps_événement_s, page), triée par arrivée."""
    alea = random.Random(graine)
    poids = [1 / (k + 1) for k in range(50)]
    pages = [f"/page/{k}" for k in range(50)]
    evts = []
    n = int(duree_s * debit)
    for i in range(n):
        t = i / debit
        u = alea.random()
        if u < 0.90:
            retard = alea.uniform(0, 0.5)
        elif u < 0.99:
            retard = alea.uniform(0.5, 4)
        else:
            retard = alea.uniform(4, 40)
        evts.append((t + retard, t, alea.choices(pages, poids)[0]))
    evts.sort()
    return evts


def verite(evts, fenetre_s):
    """Comptes exacts par (page, début de fenêtre en temps d'événement)."""
    v = {}
    for _, t, page in evts:
        cle = (page, int(t // fenetre_s) * fenetre_s)
        v[cle] = v.get(cle, 0) + 1
    return v
