"""Chapitre 47 : génère des journaux d'accès web sur disque.

16 fichiers de 1 million de lignes (environ 0,6 Go en tout) dans
~/.cache/cours-sd-ch47/journaux/ (pas dans /tmp, qui est en mémoire).
Une ligne : horodatage, page, utilisateur, statut HTTP, octets envoyés.
Les pages suivent une loi de Zipf (exposant 1,1 sur 100 000 pages), comme
la popularité réelle des pages : quelques pages concentrent une grande part
des accès, ce qui déséquilibre les partitions.

    python3 donnees.py
"""
from pathlib import Path

import numpy as np

DOSSIER = Path.home() / ".cache" / "cours-sd-ch47" / "journaux"
N_FICHIERS, LIGNES, PAGES = 16, 1_000_000, 100_000


def main():
    DOSSIER.mkdir(parents=True, exist_ok=True)
    alea = np.random.default_rng(47)
    rangs = np.arange(1, PAGES + 1)
    p = rangs ** -1.1
    p /= p.sum()
    for f in range(N_FICHIERS):
        pages = alea.choice(PAGES, size=LIGNES, p=p)
        users = alea.integers(0, 2_000_000, size=LIGNES)
        statuts = alea.choice([200, 200, 200, 200, 200, 200, 304, 404, 500], size=LIGNES)
        octets = alea.integers(200, 60_000, size=LIGNES)
        t0 = 1_767_225_600 + f * 3600
        ts = t0 + np.sort(alea.integers(0, 3600, size=LIGNES))
        with open(DOSSIER / f"part-{f:02d}.txt", "w") as sortie:
            sortie.writelines(f"{a} /page/{b} u{c} {d} {e}\n" for a, b, c, d, e in
                              zip(ts.tolist(), pages.tolist(), users.tolist(), statuts.tolist(),
                                  octets.tolist()))
    total = sum(x.stat().st_size for x in DOSSIER.iterdir())
    print(f"{N_FICHIERS} fichiers, {N_FICHIERS * LIGNES} lignes, {total / 1e9:.2f} Go")
    print(f"part de la page la plus demandée : {100 * p[0]:.1f} %")


if __name__ == "__main__":
    main()
