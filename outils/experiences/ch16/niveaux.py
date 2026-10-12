"""Chapitre 16 : ce que coûte une lecture à chaque niveau où l'on peut placer un cache.

  1. un dictionnaire dans la mémoire du processus ;
  2. Redis, sur la même machine (un aller-retour TCP local) ;
  3. PostgreSQL : un produit par sa clé primaire ;
  4. PostgreSQL : la « page produit », un produit et l'agrégat de ses 20 avis ;
  5. PostgreSQL : les 10 produits les mieux notés d'une catégorie (10 000 produits,
     200 000 avis), une requête qu'on ne veut pas refaire à chaque affichage.
Données : 100 000 produits et 2 millions d'avis (commun.py). Chaque requête est
répétée, sur des clés tirées au hasard ; on publie la médiane et le 99e centile.
Prérequis : docker, images postgres:18-alpine et redis:8.8-alpine.
Usage : python3 niveaux.py   (pip install -r requirements.txt)
"""
import json
import random
import statistics
import time

import commun


def mesurer(f, n):
    durees = []
    for _ in range(n):
        t = time.perf_counter_ns()
        f()
        durees.append((time.perf_counter_ns() - t) / 1000)       # µs
    durees.sort()
    return statistics.median(durees), durees[int(0.99 * n) - 1]


def main():
    cx, r = commun.lancer()
    try:
        commun.catalogue(cx)
        alea = random.Random(16)
        valeur = json.dumps({"nom": "produit 42", "prix": 42.99, "note": 3.8, "avis": 20}).encode()
        local = {f"produit:{i}": valeur for i in range(100_000)}
        for i in range(100_000):
            r.set(f"produit:{i}", valeur)
        cur = cx.cursor()
        q_cle = "SELECT nom, prix FROM produits WHERE id = %s"
        q_page = """SELECT p.nom, p.prix, count(*), avg(a.note) FROM produits p
                    JOIN avis a ON a.produit_id = p.id WHERE p.id = %s GROUP BY p.id"""
        q_top = """SELECT p.id, avg(a.note) AS n FROM produits p JOIN avis a ON a.produit_id = p.id
                   WHERE p.categorie = %s GROUP BY p.id ORDER BY n DESC, p.id LIMIT 10"""
        for q, arg in ((q_cle, 1), (q_page, 1), (q_top, 1)):       # échauffement, plans préparés
            for _ in range(20):
                cur.execute(q, (arg,), prepare=True)
                cur.fetchall()

        def sql(q, haut):
            def f():
                cur.execute(q, (alea.randint(1, haut),), prepare=True)
                cur.fetchall()
            return f

        essais = (
            ("dictionnaire du processus", lambda: local.get(f"produit:{alea.randrange(100_000)}"), 20_000),
            ("Redis local (GET)", lambda: r.get(f"produit:{alea.randrange(100_000)}"), 5_000),
            ("PostgreSQL, clé primaire", sql(q_cle, 100_000), 5_000),
            ("PostgreSQL, page produit", sql(q_page, 100_000), 5_000),
            ("PostgreSQL, top 10 catégorie", sql(q_top, 9), 100),
        )
        print("une lecture, sur la machine du cours ; 100 000 produits, 2 millions d'avis")
        print(f"{'niveau':<32}{'médiane':>12}{'99e centile':>14}{'rapport':>10}")
        base = None
        for nom, f, n in essais:
            med, p99 = mesurer(f, n)
            base = base or med
            print(f"{nom:<32}{med:>9.2f} µs{p99:>11.2f} µs{med / base:>9.0f}x")
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
