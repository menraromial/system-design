"""Génère les couleurs des figures à partir de palette.tsv.

Une seule source pour trois usages :
  - lib/sd-couleurs.tex : \\definecolor pour TikZ (valeurs du thème clair) ;
  - ../src/css/figures.css : variables CSS des deux thèmes ;
  - la table hex -> variable utilisée par svgpost.py.

svgpost.py reconnaît une couleur à 2 unités près par canal : deux teintes de
la palette doivent donc différer d'au moins 6 sur un canal, sinon le script
s'arrête.
"""
from itertools import combinations
from pathlib import Path

ICI = Path(__file__).parent


def lire_palette():
    lignes = []
    for ligne in (ICI / "palette.tsv").read_text().splitlines():
        if not ligne.strip() or ligne.startswith("#"):
            continue
        nom, clair, var, sombre = ligne.split("\t")
        lignes.append((nom, clair.upper(), var, sombre.upper()))
    return lignes


def ecart(a, b):
    return max(abs(int(a[i:i + 2], 16) - int(b[i:i + 2], 16)) for i in (0, 2, 4))


def verifier(pal):
    # le blanc et le noir de TikZ sont convertis à part par svgpost.py
    teintes = [(n, c) for n, c, _, _ in pal] + [("blanc", "FFFFFF"), ("noir", "000000")]
    trop_proches = [(a, b) for (a, ca), (b, cb) in combinations(teintes, 2) if ecart(ca, cb) < 6]
    if trop_proches:
        raise SystemExit(f"couleurs trop proches dans palette.tsv : {trop_proches}")


def main():
    pal = lire_palette()
    verifier(pal)
    tex = ["% Fichier généré par palette.py : ne pas modifier à la main."]
    tex += [f"\\definecolor{{{n}}}{{HTML}}{{{c}}}" for n, c, _, _ in pal]
    (ICI / "lib" / "sd-couleurs.tex").write_text("\n".join(tex) + "\n")

    css = ["/* Fichier généré par figures/palette.py : ne pas modifier à la main. */", ":root {"]
    css += [f"  {v}: #{c};" for _, c, v, _ in pal]
    css += ["}", "[data-theme='dark'] {"]
    css += [f"  {v}: #{s};" for _, _, v, s in pal]
    css += ["}"]
    (ICI.parent / "src" / "css" / "figures.css").write_text("\n".join(css) + "\n")


if __name__ == "__main__":
    main()
