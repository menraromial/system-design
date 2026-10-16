"""Chapitre 21 : ce qu'un magasin clé-valeur sait faire, et ce qu'il ne sait pas.

1 million d'utilisateurs sont rangés dans Redis, chacun dans un hachage
utilisateur:<id> (nom, ville, âge), par lots de 1 000 commandes (pipeline).
  1. lire un utilisateur par sa clé (HGETALL), 1 000 fois : durée médiane ;
  2. trouver les utilisateurs d'une ville sans index : il faut parcourir toutes
     les clés (SCAN) et lire la ville de chacune ;
  3. même question avec un index tenu par l'application : à l'écriture, chaque
     identifiant est aussi ajouté à l'ensemble ville:<nom> (SADD), et la
     question devient une lecture de cet ensemble (SMEMBERS).
On mesure aussi ce que l'index coûte en mémoire (INFO memory) et à l'écriture.
Prérequis : docker, image redis:8.8-alpine, pip install -r requirements.txt.
Usage : python3 cle_valeur.py
"""
import random
import time

import redis

import commun

N, VILLES = 1_000_000, ["Paris", "Lyon", "Marseille", "Lille", "Nantes", "Rennes", "Brest", "Nice",
                        "Toulouse", "Bordeaux"] + [f"Ville{i}" for i in range(90)]


def nombre(x):
    return f"{x:,}".replace(",", " ")


def memoire(r):
    return r.info("memory")["used_memory"] / 2**20


def charger(r, index):
    r.flushall()
    alea = random.Random(21)
    t = time.perf_counter()
    p = r.pipeline(transaction=False)
    for i in range(N):
        ville = alea.choice(VILLES)
        p.hset(f"utilisateur:{i}", mapping={"nom": f"nom{i}", "ville": ville,
                                            "age": alea.randint(18, 90)})
        if index:
            p.sadd(f"ville:{ville}", i)
        if i % 1000 == 999:
            p.execute()
    p.execute()
    return time.perf_counter() - t


def main():
    r = commun.lancer("redis", lambda: redis.Redis(port=10179, decode_responses=True).ping()
                      and redis.Redis(port=10179, decode_responses=True))
    try:
        print(f"{nombre(N)} utilisateurs dans des hachages Redis")
        print(f"   {'':<36}{'durée':>12}{'mémoire':>10}")
        duree = charger(r, index=False)
        print(f"   {'chargement sans index':<36}{duree:>10.1f} s{memoire(r):>7.0f} Mo")

        alea = random.Random(5)
        cles = [f"utilisateur:{alea.randrange(N)}" for _ in range(1000)]
        med = commun.chrono(lambda: r.hgetall(cles.pop()), n=1000)
        print(f"   {'1. par la clé (HGETALL), médiane':<36}{med:>9.3f} ms")

        t = time.perf_counter()
        trouves = commandes = 0
        curseur = 0
        while True:
            curseur, cles = r.scan(curseur, match="utilisateur:*", count=1000)
            commandes += 1
            p = r.pipeline(transaction=False)
            for c in cles:
                p.hget(c, "ville")
            villes = p.execute()
            commandes += len(cles)
            trouves += sum(1 for v in villes if v == "Lyon")
            if curseur == 0:
                break
        duree = time.perf_counter() - t
        print(f"   {'2. Lyon, sans index (SCAN + HGET)':<36}{1000 * duree:>9.0f} ms")
        print(f"      {nombre(trouves)} trouvés, {nombre(commandes)} commandes")

        duree = charger(r, index=True)
        print(f"   {'chargement avec index ville:<nom>':<36}{duree:>10.1f} s{memoire(r):>7.0f} Mo")
        med = commun.chrono(lambda: r.smembers("ville:Lyon"), n=20)
        n = r.scard("ville:Lyon")
        print(f"   {'3. Lyon, avec index (SMEMBERS)':<36}{med:>9.1f} ms")
        print(f"      {nombre(n)} trouvés, 1 commande")
    finally:
        commun.arreter("redis")


if __name__ == "__main__":
    main()
