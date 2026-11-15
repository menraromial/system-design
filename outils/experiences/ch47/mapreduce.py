"""Chapitre 47 : un MapReduce minimal sur un poste, pour mesurer ses mécanismes.

Le travail compte, pour chaque page, le nombre d'accès et les octets envoyés,
à partir des 16 fichiers de journaux produits par donnees.py (une tâche map par
fichier, M = 16 ; R = 16 partitions). Chaque tâche est un processus ; le maître
en lance au plus W à la fois, relance une tâche dont le processus meurt, et peut
lancer une copie de secours d'une tâche trop lente (première arrivée gagne).
Les sorties sont écrites dans un fichier temporaire puis renommées, si bien
qu'une tâche exécutée deux fois ne produit qu'un résultat.

  1. le travail avec 16 processus, sans puis avec combineur ;
  2. passage à l'échelle de 1 à 16 processus ;
  3. un processus map tué au milieu de sa tâche ;
  4. une tâche map cinq fois plus lente, sans puis avec tâches de secours ;
  5. déséquilibre des partitions sans combineur, puis avec salage des clés.

    python3 donnees.py ; python3 mapreduce.py      (environ 4 minutes)
"""
import collections
import multiprocessing as mp
import os
import shutil
import signal
import statistics
import time
import zlib
from pathlib import Path

RACINE = Path.home() / ".cache" / "cours-sd-ch47"
ENTREES = sorted((RACINE / "journaux").glob("part-*.txt"))
R = 16
CTX = mp.get_context("fork")


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


# ---- les tâches -------------------------------------------------------------------------
def partition(cle):
    return zlib.crc32(cle.encode()) % R          # même résultat dans tous les processus


def ecrire_atomique(chemin, lignes):
    tmp = chemin.with_suffix(f".tmp{os.getpid()}")
    with open(tmp, "w") as f:
        f.writelines(lignes)
    os.replace(tmp, chemin)


def tache_map(i, rep, combineur, sel, lent):
    t0 = time.perf_counter()
    if combineur:
        parts = [collections.defaultdict(lambda: [0, 0]) for _ in range(R)]
    else:
        parts = [[] for _ in range(R)]
    k = 0
    with open(ENTREES[i]) as f:
        for ligne in f:
            _, page, _, _, octets = ligne.split()
            cle = f"{page}#{k % 16}" if sel else page      # salage : 16 sous-clés par page
            k += 1
            p = partition(cle)
            if combineur:
                a = parts[p][cle]
                a[0] += 1
                a[1] += int(octets)
            else:
                parts[p].append(f"{cle}\t1\t{octets}\n")
    if lent:
        time.sleep(lent * (time.perf_counter() - t0))      # tâche ralentie : lent fois sa durée
    for p in range(R):
        lignes = (parts[p] if not combineur else
                  (f"{c}\t{n}\t{o}\n" for c, (n, o) in parts[p].items()))
        ecrire_atomique(rep / f"map-{i:02d}-{p:02d}", lignes)


def tache_reduce(p, rep):
    total = collections.defaultdict(lambda: [0, 0])
    for i in range(len(ENTREES)):
        with open(rep / f"map-{i:02d}-{p:02d}") as f:
            for ligne in f:
                c, n, o = ligne.split("\t")
                a = total[c]
                a[0] += int(n)
                a[1] += int(o)
    ecrire_atomique(rep / f"sortie-{p:02d}", (f"{c}\t{n}\t{o}\n" for c, (n, o) in total.items()))


# ---- le maître -------------------------------------------------------------------------------
def executer(taches, w, tuer=None, secours=False):
    """taches : {identifiant: (fonction, args[, args d'une copie de secours])}.
    Renvoie les durées et les compteurs."""
    attente = collections.deque(taches)
    en_cours = {}                         # processus -> (id, début)
    faites, durees = set(), {}
    relances = copies = 0
    tue = False
    while len(faites) < len(taches):
        while attente and len(en_cours) < w:
            t = attente.popleft()
            if t in faites:
                continue
            f, args = taches[t][:2]
            p = CTX.Process(target=f, args=args)
            p.start()
            en_cours[p] = (t, time.monotonic())
        time.sleep(0.005)
        maintenant = time.monotonic()
        for p in list(en_cours):
            t, debut = en_cours[p]
            if tuer and not tue and t == tuer[0] and maintenant - debut > tuer[1]:
                os.kill(p.pid, signal.SIGKILL)               # panne d'un travailleur
                tue = True
            if not p.is_alive():
                p.join()
                del en_cours[p]
                if p.exitcode == 0:
                    if t not in faites:
                        faites.add(t)
                        durees[t] = maintenant - debut
                    for q, (t2, _) in list(en_cours.items()):   # copie devenue inutile
                        if t2 == t:
                            q.kill()
                            q.join()
                            del en_cours[q]
                elif t not in faites:
                    relances += 1
                    attente.appendleft(t)                     # réexécution
        if secours and not attente and len(durees) >= len(taches) // 2:
            mediane = statistics.median(durees.values())
            lances = collections.Counter(t for t, _ in en_cours.values())
            for p, (t, debut) in list(en_cours.items()):
                if lances[t] == 1 and maintenant - debut > 2 * mediane and len(en_cours) < w + 4:
                    f, args = taches[t][0], taches[t][-1]     # la lenteur tient à la machine
                    q = CTX.Process(target=f, args=args)
                    q.start()
                    en_cours[q] = (t, maintenant)
                    lances[t] += 1
                    copies += 1
    return durees, relances, copies


def travail(w=16, combineur=True, sel=False, tuer=None, lent_tache=None, secours=False):
    rep = RACINE / "inter"
    shutil.rmtree(rep, ignore_errors=True)
    rep.mkdir(parents=True)
    t0 = time.monotonic()
    maps = {i: (tache_map, (i, rep, combineur, sel, 4 if i == lent_tache else 0),
                (i, rep, combineur, sel, 0)) for i in range(len(ENTREES))}
    dm, relances, copies = executer(maps, w, tuer=tuer, secours=secours)
    t1 = time.monotonic()
    inter = sum(f.stat().st_size for f in rep.glob("map-*") if ".tmp" not in f.name)
    taille_part = [sum((rep / f"map-{i:02d}-{p:02d}").stat().st_size for i in range(len(ENTREES)))
                   for p in range(R)]
    reds = {p: (tache_reduce, (p, rep)) for p in range(R)}
    dr, _, _ = executer(reds, w)
    t2 = time.monotonic()
    # contrôle : la page la plus demandée (après regroupement des sous-clés salées)
    tot = collections.Counter()
    for p in range(R):
        with open(rep / f"sortie-{p:02d}") as f:
            for ligne in f:
                c, n, _ = ligne.split("\t")
                tot[c.split("#")[0]] += int(n)
    return {"map": t1 - t0, "reduce": t2 - t1, "total": t2 - t0, "inter": inter, "parts": taille_part,
            "dr": dr, "dm": dm, "relances": relances, "copies": copies, "pages": len(tot),
            "top": tot.most_common(1)[0]}


def main():
    entree = sum(f.stat().st_size for f in ENTREES)
    print(f"entrée : {len(ENTREES)} fichiers, {entree / 1e6:.0f} Mo ; M = {len(ENTREES)}, R = {R}")
    print("\n1. travail complet, 16 processus")
    print(f"   {'':<16}{'map s':>7}{'reduce s':>10}{'total s':>9}{'intermédiaire':>15}")
    for comb in (False, True):
        r = travail(16, combineur=comb)
        print(f"   {'avec combineur' if comb else 'sans combineur':<16}{r['map']:>7.1f}{r['reduce']:>10.1f}"
              f"{r['total']:>9.1f}{r['inter'] / 1e6:>12.0f} Mo")
    print(f"   {nb(r['pages'])} pages ; la plus demandée : {r['top'][0]}, {nb(r['top'][1])} accès")
    print("\n2. passage à l'échelle (avec combineur)")
    print(f"   {'processus':>9}{'total s':>9}{'accélération':>14}")
    base = None
    for w in (1, 2, 4, 8, 16):
        r = travail(w)
        base = base or r["total"]
        print(f"   {w:>9}{r['total']:>9.1f}{base / r['total']:>13.1f}x")
    ref = travail(16)
    dmap = statistics.median(ref["dm"].values())
    print(f"\n3. panne : processus de la tâche map 3 tué à mi-parcours ({dmap / 2:.1f} s)")
    r = travail(16, tuer=(3, dmap / 2))
    print(f"   sans panne : {ref['total']:.1f} s ; avec panne : {r['total']:.1f} s ;"
          f" tâches relancées : {r['relances']}")
    print(f"   résultat identique : {r['top'] == ref['top'] and r['pages'] == ref['pages']}")
    print("\n4. retardataire : tâche map 5 cinq fois plus lente")
    for sec in (False, True):
        r = travail(16, lent_tache=5, secours=sec)
        print(f"   {'avec' if sec else 'sans'} tâches de secours : map {r['map']:.1f} s, total {r['total']:.1f} s,"
              f" copies lancées : {r['copies']}")
    print("\n5. déséquilibre des partitions (sans combineur)")
    print(f"   {'':<14}{'part. max':>10}{'médiane':>9}{'rapport':>9}{'reduce max':>12}{'méd.':>7}")
    for sel in (False, True):
        r = travail(16, combineur=False, sel=sel)
        pm, pmed = max(r["parts"]), statistics.median(r["parts"])
        dmax, dmed = max(r["dr"].values()), statistics.median(r["dr"].values())
        print(f"   {'salage' if sel else 'clé = page':<14}{pm / 1e6:>7.0f} Mo{pmed / 1e6:>6.0f} Mo{pm / pmed:>9.1f}"
              f"{dmax:>10.2f} s{dmed:>6.2f} s")
    shutil.rmtree(RACINE / "inter", ignore_errors=True)


if __name__ == "__main__":
    main()
