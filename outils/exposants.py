"""Convertit en formules KaTeX les puissances écrites en exposants Unicode
dans la prose (10⁵, 3,65 × 10⁸, 2¹⁰). Les tableaux, le code et les formules
existantes ne sont pas touchés.

Usage : python3 outils/exposants.py docs/.../page.mdx [...]
"""
import re
import sys
from pathlib import Path

SUP = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹⁻", "0123456789-")
E = "[⁰¹²³⁴⁵⁶⁷⁸⁹⁻]+"
ESP = "[   ]"
PUISSANCE = re.compile(rf"(?:(\d+(?:,\d+)?){ESP}×{ESP})?(\d+)({E})")


def katex(m):
    mantisse, base, exp = m.group(1), m.group(2), m.group(3).translate(SUP)
    corps = f"{base}^{{{exp}}}" if len(exp) > 1 else f"{base}^{exp}"
    if mantisse:
        corps = mantisse.replace(",", "{,}") + r" \times " + corps
    return f"${corps}$"


def convertir_ligne(ligne):
    morceaux = re.split(r"(`[^`]*`|\$[^$]+\$|\]\([^)]*\))", ligne)
    return "".join(m if m[:1] in "`$" or m.startswith("](") else PUISSANCE.sub(katex, m)
                   for m in morceaux)


def convertir(chemin):
    sortie, dans_code, front = [], False, 0
    for i, ligne in enumerate(Path(chemin).read_text().split("\n")):
        if i == 0 and ligne == "---":
            front = 1
        elif front == 1 and ligne == "---":
            front = 2
        elif ligne.lstrip().startswith("```") or ligne.strip() == "$$":
            dans_code = not dans_code
        elif front != 1 and not dans_code and not ligne.startswith(("|", "import ", "[^")):
            ligne = convertir_ligne(ligne)
        sortie.append(ligne)
    Path(chemin).write_text("\n".join(sortie))


if __name__ == "__main__":
    for c in sys.argv[1:]:
        convertir(c)
