"""Relève les phrases de prose d'une page qui contiennent un deux-points.

Sert à la relecture (règles de ponctuation : un deux-points seulement pour une
citation, une énumération, une explication ou une conséquence). Ignore le front
matter, les blocs de code, les tableaux, les notes, les titres, les imports, les
balises, les lignes d'encadré, et les étiquettes « Hypothèses : » et
« Conclusion : » quand elles sont le seul deux-points de la phrase.
Usage : python3 outils/deux_points.py docs/.../page.mdx [...]
"""
import re
import sys

DP = r'[   ]:(?=\s)'


def phrases(chemin):
    s = open(chemin).read()
    s = re.sub(r'^---.*?---', '', s, flags=re.S)
    s = re.sub(r'```.*?```', '', s, flags=re.S)
    for ligne in s.splitlines():
        if ligne.startswith(('import', '[^', '|', '<', '#', ':::', '</')) or not ligne.strip():
            continue
        for ph in re.split(r'(?<=[.!?»)])\s+(?=[A-ZÀ-ÝÉ«`*\[])', ligne):
            k = len(re.findall(DP, ph))
            if k and not (k == 1 and re.match(r'(Hypothèses|Conclusion)[   ]:', ph)):
                yield k, ph


if __name__ == '__main__':
    for chemin in sys.argv[1:]:
        n = 0
        for n, (k, ph) in enumerate(phrases(chemin), 1):
            print(f"[{n}]{'*' if k > 1 else ''} {ph}")
        print(f"# {chemin} : {n} phrases avec deux-points", file=sys.stderr)
