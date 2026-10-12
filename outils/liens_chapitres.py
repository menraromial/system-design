"""Transforme les renvois « ch. N » des pages du cours en liens vers les chapitres.

La liste des chapitres rédigés vient de src/components/parties.ts (champ doc).
Un renvoi vers un chapitre pas encore écrit reste en texte ; il deviendra un lien
quand le script sera relancé après la rédaction de ce chapitre. Le script défait
d'abord les liens qu'il a posés, puis relie tout : on peut le relancer sans risque.

Formes reconnues : « ch. 12 », « ch. 24 et 25 », « ch. 24 à 26 », « ch. 8 ou 9 »
(espace simple ou insécable après « ch. »). Laissés tels quels : l'en-tête YAML,
les blocs de code, le code en ligne, les formules, les titres, les lignes JSX
(attributs alt des figures), les lignes d'import et d'encadré, et les renvois
d'un chapitre vers lui-même.
Usage : python3 outils/liens_chapitres.py   (depuis la racine du dépôt)
"""
import os
import pathlib
import re

RACINE = pathlib.Path(__file__).resolve().parent.parent
DOCS = RACINE / "docs"
ESP = "[   ]"


def chapitres():
    texte = (RACINE / "src/components/parties.ts").read_text()
    table = {}
    for num, doc in re.findall(r"\{num: (\d+), titre: '(?:[^'\\]|\\.)*', doc: '([^']+)'\}", texte):
        for ext in (".mdx", ".md"):
            f = DOCS / f"{doc}{ext}"
            if f.exists():
                table[int(num)] = f
    return table


# un lien posé par ce script : [ch. 12](…) ou [25](…), vers un fichier .md(x)
LIEN = re.compile(rf"\[((?:ch\.{ESP})?\d+)\]\(([^)\s]+\.mdx?)\)")
RENVOI = re.compile(rf"\bch\.({ESP})(\d+)((?:{ESP}(?:et|à|ou){ESP}\d+)*)")
SUITE = re.compile(rf"({ESP}(?:et|à|ou){ESP})(\d+)")
PROTEGE = re.compile(r"(`[^`]*`|\$\$?[^$]+\$\$?|\]\([^)]*\))")


def relier_texte(texte, table, fichier):
    def cible(n):
        f = table.get(n)
        if f is None or f == fichier:
            return None
        return os.path.relpath(f, fichier.parent)

    def remplacer(m):
        esp, n, suite = m.group(1), int(m.group(2)), m.group(3)
        c = cible(n)
        tete = f"[ch.{esp}{n}]({c})" if c else f"ch.{esp}{n}"

        def numero(s):
            c2 = cible(int(s.group(2)))
            return s.group(1) + (f"[{s.group(2)}]({c2})" if c2 else s.group(2))
        return tete + SUITE.sub(numero, suite)

    morceaux = PROTEGE.split(texte)
    for i in range(0, len(morceaux), 2):    # indices pairs : hors code, formules, cibles de liens
        morceaux[i] = RENVOI.sub(remplacer, morceaux[i])
    return "".join(morceaux)


def traiter(fichier, table):
    cibles = {f.resolve() for f in table.values()}
    lignes = fichier.read_text().split("\n")
    sortie, dans_code, entete = [], False, lignes[:1] == ["---"]
    for k, ligne in enumerate(lignes):
        if entete:
            sortie.append(ligne)
            if k > 0 and ligne == "---":
                entete = False
            continue
        if ligne.lstrip().startswith("```"):
            dans_code = not dans_code
            sortie.append(ligne)
            continue
        brut = ligne.lstrip()
        if dans_code or brut.startswith(("#", "<", "import ", ":::")):
            sortie.append(ligne)
            continue
        # défaire les liens déjà posés vers un chapitre, avant de tout relier
        ligne = LIEN.sub(lambda m: m.group(1) if (fichier.parent / m.group(2)).resolve() in cibles
                         else m.group(0), ligne)
        sortie.append(relier_texte(ligne, table, fichier))
    nouveau = "\n".join(sortie)
    if nouveau != fichier.read_text():
        fichier.write_text(nouveau)
        return True
    return False


def main():
    table = chapitres()
    modifies = [f for f in sorted(DOCS.rglob("*.md*")) if traiter(f, table)]
    print(f"{len(table)} chapitres rédigés ; {len(modifies)} fichiers modifiés")
    for f in modifies:
        print("  ", f.relative_to(RACINE))


if __name__ == "__main__":
    main()
