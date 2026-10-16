"""Chapitre 26 : répartir un million de clés sur des nœuds, et en ajouter un.

Un million de clés (« client:0 » à « client:999999 ») sont réparties sur 10
nœuds par six méthodes, puis un onzième nœud est ajouté. Pour chaque méthode :
  - déséquilibre : nombre de clés du nœud le plus chargé divisé par la moyenne ;
  - clés déplacées par l'ajout du onzième nœud (l'idéal est 1/11, soit 9,1 %).
Méthodes : hachage modulo N ; hachage cohérent avec 1, 16 et 256 jetons par
nœud (Karger et al., 1997) ; hachage « jump » (Lamping et Veach, 2014) ; 1 024
partitions fixes réparties entre les nœuds, dont certaines changent de nœud.
Puis deux charges déséquilibrées :
  - des clés croissantes (une date) réparties par plages ou par hachage :
    part des écritures de la dernière minute reçue par le nœud le plus chargé ;
  - une popularité de Zipf (exposant 1) sur le million de clés : part des
    requêtes reçue par le nœud le plus chargé, avant et après avoir découpé les
    100 clés les plus demandées en 10 sous-clés chacune (salage).
Pour l'anneau, les positions des jetons dépendent du tirage, et l'on en fait cinq.
Calcul pur, sans réseau ni base. Usage : python3 hachage.py
"""
import bisect
import hashlib
import random
import statistics

CLES = [f"client:{i}" for i in range(1_000_000)]


def h(texte):
    return int.from_bytes(hashlib.blake2b(texte.encode(), digest_size=8).digest(), "big")


HACHES = [h(c) for c in CLES]


def modulo(n):
    return [x % n for x in HACHES]


def anneau(n, jetons, tirage=0):
    points = sorted((h(f"tirage{tirage}-nœud{k}-jeton{j}"), k) for k in range(n) for j in range(jetons))
    positions = [p for p, _ in points]
    def place(x):
        i = bisect.bisect_right(positions, x) % len(points)
        return points[i][1]
    return [place(x) for x in HACHES]


def jump(cle, n):
    """Hachage « jump » de Lamping et Veach (arXiv:1406.2294), sur une clé de 64 bits."""
    b, j = -1, 0
    while j < n:
        b = j
        cle = (cle * 2862933555777941757 + 1) % 2**64
        j = int((b + 1) * (2**31 / ((cle >> 33) + 1)))
    return b


def partitions_fixes(n, total=1024, avant=None):
    """1 024 partitions ; la partition d'une clé ne change jamais. Ajouter un nœud
    lui donne des partitions prises aux autres, à peu près autant à chacun."""
    if avant is None:
        affectation = [p % n for p in range(total)]
    else:
        affectation = list(avant)
        voulu = total // n
        prises = 0
        par_noeud = {k: [p for p in range(total) if affectation[p] == k] for k in range(n - 1)}
        while prises < voulu:
            plus = max(par_noeud, key=lambda k: len(par_noeud[k]))
            affectation[par_noeud[plus].pop()] = n - 1
            prises += 1
    return [affectation[x % total] for x in HACHES], affectation


def mesure(avant, apres, n=10):
    charges = [0] * n
    for k in avant:
        charges[k] += 1
    return max(charges) / statistics.mean(charges), sum(a != b for a, b in zip(avant, apres)) / len(avant)


def bilan(nom, avant, apres):
    deseq, deplacees = mesure(avant, apres)
    print(f"   {nom:<28}{deseq:>9.2f}{100 * deplacees:>13.1f} %")


def main():
    print("1 000 000 de clés, 10 nœuds, puis 11")
    print("\n1. répartition et ajout d'un onzième nœud ; anneau : 5 tirages des jetons")
    print(f"   {'méthode':<28}{'max/moy.':>9}{'déplacées':>15}")
    bilan("modulo N", modulo(10), modulo(11))
    for jetons in (1, 16, 256):
        res = [mesure(anneau(10, jetons, t), anneau(11, jetons, t)) for t in range(5)]
        d = sorted(r[0] for r in res)
        m = sorted(100 * r[1] for r in res)
        print(f"   {'anneau, %d jeton%s/nœud' % (jetons, 's' if jetons > 1 else ''):<28}"
              f"{d[0]:>5.2f}-{d[-1]:.2f}{m[0]:>8.1f}-{m[-1]:.1f} %")
    bilan("hachage jump", [jump(x, 10) for x in HACHES], [jump(x, 11) for x in HACHES])
    a10, aff = partitions_fixes(10)
    a11, _ = partitions_fixes(11, avant=aff)
    bilan("1 024 partitions fixes", a10, a11)

    print("\n2. clés croissantes : 60 000 écritures pendant la dernière minute,")
    print("   part reçue par le nœud le plus chargé")
    # dates en millisecondes sur une journée ; 10 plages égales sur la journée
    alea = random.Random(26)
    journee = 86_400_000
    recentes = list(range(journee - 60_000, journee))      # une écriture par milliseconde
    plage = [min(d * 10 // journee, 9) for d in recentes]
    hache = [h(str(d)) % 10 for d in recentes]
    for nom, aff in (("par plages de dates", plage), ("par hachage de la date", hache)):
        charges = [aff.count(k) for k in range(10)]
        print(f"   {nom:<28}{100 * max(charges) / len(aff):>6.1f} %")

    print("\n3. popularité de Zipf (exposant 1) : 1 000 000 de requêtes")
    poids = [1 / (r + 1) for r in range(len(CLES))]
    total = sum(poids)
    requetes = alea.choices(range(len(CLES)), cum_weights=list(_cumul(poids)), k=1_000_000)
    noeud = [jump(HACHES[r], 10) for r in requetes]
    charges = [noeud.count(k) for k in range(10)]
    print(f"   la clé la plus demandée : {100 * poids[0] / total:.1f} % des requêtes")
    print(f"   nœud le plus chargé : {100 * max(charges) / len(noeud):.1f} % des requêtes"
          f" (moyenne 10,0 %)")
    sel = random.Random(1)
    noeud_sale = []
    for r in requetes:
        if r < 100:                       # clé chaude : une des 10 sous-clés, au hasard
            noeud_sale.append(jump(h(f"{CLES[r]}#{sel.randrange(10)}"), 10))
        else:
            noeud_sale.append(jump(HACHES[r], 10))
    charges = [noeud_sale.count(k) for k in range(10)]
    print(f"   après salage des 100 clés les plus demandées : {100 * max(charges) / len(noeud_sale):.1f} %")
    print(f"   (chaque lecture d'une clé salée doit interroger ses 10 sous-clés)")


def _cumul(poids):
    s = 0.0
    for p in poids:
        s += p
        yield s


if __name__ == "__main__":
    main()
