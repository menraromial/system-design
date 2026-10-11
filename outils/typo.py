"""Typographie française des pages du cours, à lancer après chaque rédaction.

- espace fine insécable (U+202F) entre les groupes de chiffres : 200 000 ;
- espace insécable (U+00A0) avant « : », à l'intérieur des guillemets « »,
  entre un nombre et son unité (300 ms, 8 h 01) ;
- espace fine insécable avant « ; », « ? », « ! » ;
- apostrophe typographique (’) entre deux lettres.
Le front matter, les lignes d'import, les blocs de code et le code en ligne
ne sont pas touchés.

Usage : python3 outils/typo.py docs/partie-0/demarche.mdx [...]
"""
import re
import sys
from pathlib import Path

FINE, INSEC = " ", " "


def corriger_texte(t):
    t = re.sub(r"(?<=\d) (?=\d{3}(?!\d))", FINE, t)
    t = re.sub(r"(?<=\d) (?=\d{3}(?!\d))", FINE, t)  # 1 000 000 : deux passes
    t = re.sub(r"(?<=\d) (?=(?:h|min|ms|µs|ns|s|%|o|Ko|Mo|Go|To|Po|bit|Mbit|Gbit|m²|€|\$)(?!\w))", INSEC, t)
    t = re.sub(r"(?<=\d\u00a0h) (?=\d\d(?!\d))", INSEC, t)
    t = re.sub(r" :(?=\s|$)", INSEC + ":", t)
    t = re.sub(r" ([;?!])", FINE + r"\1", t)
    t = re.sub(r"(?<=\w)'(?=\w)", "\u2019", t)
    t = re.sub(r"« ", "«" + INSEC, t)
    t = re.sub(r" »", INSEC + "»", t)
    return t


def corriger_ligne(ligne):
    morceaux = re.split(r"(`[^`]*`|\]\([^)]*\))", ligne)
    return "".join(m if m.startswith("`") or m.startswith("](") else corriger_texte(m)
                   for m in morceaux)


def corriger(chemin):
    lignes = Path(chemin).read_text().split("\n")
    sortie, dans_code, front = [], False, 0
    for i, ligne in enumerate(lignes):
        if i == 0 and ligne == "---":
            front = 1
            sortie.append(ligne)
            continue
        if front == 1:
            if ligne == "---":
                front = 2
            sortie.append(ligne)
            continue
        if ligne.lstrip().startswith("```"):
            dans_code = not dans_code
            sortie.append(ligne)
            continue
        if dans_code or ligne.startswith("import "):
            sortie.append(ligne)
            continue
        sortie.append(corriger_ligne(ligne))
    Path(chemin).write_text("\n".join(sortie))


if __name__ == "__main__":
    for c in sys.argv[1:]:
        corriger(c)
