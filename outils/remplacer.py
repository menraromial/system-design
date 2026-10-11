"""Remplacements exacts dans une page déjà passée par typo.py.

Le texte cherché est écrit avec des espaces et des apostrophes ordinaires :
la recherche accepte à leur place les espaces insécables (U+00A0, U+202F) et
l'apostrophe typographique. Chaque motif doit apparaître exactement une fois.

Usage (depuis Python) : from remplacer import remplacer ; s = remplacer(s, a, b)
"""
import re


def motif(texte):
    m = re.escape(texte)
    m = m.replace("\\ ", "[   ]").replace("'", "['’]")
    return m


def remplacer(s, ancien, nouveau, fois=1):
    s2, n = re.subn(motif(ancien), lambda _: nouveau, s)
    if n != fois:
        raise SystemExit(f"{n} occurrence(s) au lieu de {fois} : {ancien[:70]}")
    return s2
