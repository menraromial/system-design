"""Chapitre 20 : dossier de travail et mesure des octets écrits sur le disque.

Les données vont dans ~/.cache/cours-sd-ch20, sur le disque de la machine (et
non dans /tmp, qui est souvent un système de fichiers en mémoire) ; le dossier
est supprimé par son nom à la fin de chaque script.
"""
import os
import shutil

DOSSIER = os.path.expanduser("~/.cache/cours-sd-ch20")


def preparer():
    nettoyer()
    os.makedirs(DOSSIER)
    return DOSSIER


def nettoyer():
    shutil.rmtree(DOSSIER, ignore_errors=True)


def supprimer(chemin):
    shutil.rmtree(chemin, ignore_errors=True)


def octets_ecrits():
    """Octets écrits sur le volume qui contient le dossier de travail, d'après le
    compteur du noyau (/sys/dev/block/.../stat, en secteurs de 512 octets).
    Le compteur du processus (/proc/self/io) n'est pas utilisé : il surestime les
    écritures d'un moteur qui écrit dans un fichier projeté en mémoire, comme LMDB.
    Les écritures des autres programmes de la machine sur ce volume sont comptées
    aussi ; elles sont faibles devant les volumes mesurés ici."""
    st = os.stat(DOSSIER)
    stat = open(f"/sys/dev/block/{os.major(st.st_dev)}:{os.minor(st.st_dev)}/stat").read().split()
    return int(stat[6]) * 512


def taille(chemin):
    """Place réellement occupée sur le disque (blocs alloués), en octets."""
    total = 0
    for racine, _, fichiers in os.walk(chemin):
        for f in fichiers:
            try:
                total += os.stat(os.path.join(racine, f)).st_blocks * 512
            except FileNotFoundError:            # fichier supprimé entre-temps (compaction)
                pass
    return total
