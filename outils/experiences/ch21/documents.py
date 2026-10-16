"""Chapitre 21 : documents imbriqués, références et tables.

Les mêmes 200 000 commandes de 3 lignes, sur 50 000 produits dont la popularité
suit une loi de Zipf, sont rangées :
  - dans PostgreSQL, en trois tables (produits, commandes, lignes) ;
  - dans MongoDB, en documents « commande » qui contiennent leurs lignes, avec
    une copie du nom du produit dans chaque ligne (imbrication) ;
  - dans MongoDB, en documents « commande » dont les lignes ne contiennent que
    l'identifiant du produit (référence), les produits étant dans leur collection.
Mesures :
  1. lire une commande complète (lignes et noms des produits) par son
     identifiant : médiane sur 1 000 lectures ;
  2. renommer le produit le plus vendu : une ligne dans PostgreSQL et dans la
     collection des produits, mais toutes ses copies dans les commandes imbriquées ;
  3. un document qui grandit sans borne : on ajoute à un même document des
     commentaires de 1 Kio jusqu'au refus de MongoDB, en relevant la durée d'un
     ajout et la taille du document en chemin.
Prérequis : docker, images postgres:18-alpine et mongo:7 (MongoDB 8 refuse de démarrer
sur un noyau Linux 6.19 ou plus récent, ticket SERVER-121912), pip install -r
requirements.txt. Usage : python3 documents.py
"""
import random
import time

import psycopg
import pymongo
from pymongo.errors import PyMongoError

import commun

COMMANDES, PRODUITS, LIGNES = 200_000, 50_000, 3


def nombre(x):
    return f"{x:,}".replace(",", " ")


def donnees():
    alea = random.Random(21)
    cumul, total = [], 0.0
    for k in range(PRODUITS):
        total += 1 / (k + 1)
        cumul.append(total)
    produits = [(p, f"produit {p}", round(alea.uniform(1, 200), 2)) for p in range(PRODUITS)]
    commandes = []
    for c in range(COMMANDES):
        choisis = alea.choices(range(PRODUITS), cum_weights=cumul, k=LIGNES)
        commandes.append((c, alea.randrange(20_000), [(p, alea.randint(1, 5)) for p in choisis]))
    return produits, commandes


def charger_pg(produits, commandes):
    cx = commun.lancer("pg", lambda: psycopg.connect(commun.PG, autocommit=True))
    cx.execute("""CREATE TABLE produits (id int PRIMARY KEY, nom text, prix numeric(8,2));
                  CREATE TABLE commandes (id int PRIMARY KEY, client_id int);
                  CREATE TABLE lignes (commande_id int REFERENCES commandes, rang int,
                                       produit_id int REFERENCES produits, quantite int,
                                       PRIMARY KEY (commande_id, rang));""")
    with cx.cursor() as cur:
        with cur.copy("COPY produits FROM STDIN") as cp:
            for p in produits:
                cp.write_row(p)
        with cur.copy("COPY commandes FROM STDIN") as cp:
            for c, client, _ in commandes:
                cp.write_row((c, client))
        with cur.copy("COPY lignes FROM STDIN") as cp:
            for c, _, lignes in commandes:
                for rang, (p, q) in enumerate(lignes):
                    cp.write_row((c, rang, p, q))
    cx.execute("CREATE INDEX ON lignes (produit_id)")
    cx.execute("VACUUM ANALYZE")
    return cx


def charger_mongo(produits, commandes):
    client = commun.lancer("mongo", lambda: (lambda c: c.admin.command("ping") and c)(
        pymongo.MongoClient("mongodb://127.0.0.1:10117", serverSelectionTimeoutMS=2000)))
    db = client.cours
    noms = {p: (n, prix) for p, n, prix in produits}
    db.produits.insert_many([{"_id": p, "nom": n, "prix": prix} for p, n, prix in produits])
    lot_imb, lot_ref = [], []
    for c, client_id, lignes in commandes:
        lot_imb.append({"_id": c, "client_id": client_id, "lignes": [
            {"produit_id": p, "nom": noms[p][0], "prix": noms[p][1], "quantite": q} for p, q in lignes]})
        lot_ref.append({"_id": c, "client_id": client_id, "lignes": [
            {"produit_id": p, "quantite": q} for p, q in lignes]})
        if len(lot_imb) == 5000:
            db.commandes_imbriquees.insert_many(lot_imb)
            db.commandes_references.insert_many(lot_ref)
            lot_imb, lot_ref = [], []
    db.commandes_imbriquees.create_index("lignes.produit_id")
    return client, db


def main():
    produits, commandes = donnees()
    alea = random.Random(7)
    ids = [alea.randrange(COMMANDES) for _ in range(1000)]
    try:
        cx = charger_pg(produits, commandes)
        client, db = charger_mongo(produits, commandes)
        print(f"{nombre(COMMANDES)} commandes de {LIGNES} lignes, {nombre(PRODUITS)} produits")

        print("\n1. lire une commande complète par son identifiant (médiane sur 1 000)")
        q = ("SELECT c.id, c.client_id, l.rang, p.nom, p.prix, l.quantite FROM commandes c "
             "JOIN lignes l ON l.commande_id = c.id JOIN produits p ON p.id = l.produit_id "
             "WHERE c.id = %s")
        it = iter(ids)
        print(f"   {'PostgreSQL, jointure de 3 tables':<38}"
              f"{commun.chrono(lambda: cx.execute(q, (next(it),)).fetchall(), 1000):>7.3f} ms")
        it = iter(ids)
        print(f"   {'MongoDB, document imbriqué':<38}"
              f"{commun.chrono(lambda: db.commandes_imbriquees.find_one({'_id': next(it)}), 1000):>7.3f} ms")
        pipeline = lambda c: [{"$match": {"_id": c}},
                              {"$lookup": {"from": "produits", "localField": "lignes.produit_id",
                                           "foreignField": "_id", "as": "produits"}}]
        it = iter(ids)
        print(f"   {'MongoDB, références + $lookup':<38}"
              f"{commun.chrono(lambda: list(db.commandes_references.aggregate(pipeline(next(it)))), 1000):>7.3f} ms")

        print("\n2. renommer le produit le plus vendu, médiane sur 5 renommages")
        n_lignes, = cx.execute("SELECT count(*) FROM lignes WHERE produit_id = 0").fetchone()
        noms = iter(f"produit 0, nom {i}" for i in range(100))
        d = commun.chrono(lambda: cx.execute("UPDATE produits SET nom = %s WHERE id = 0", (next(noms),)), 5)
        print(f"   {'PostgreSQL, table produits':<38}{d:>7.1f} ms   1 ligne")
        print(f"      ({nombre(n_lignes)} lignes de commande citent ce produit)")
        d = commun.chrono(lambda: db.produits.update_one({"_id": 0}, {"$set": {"nom": next(noms)}}), 5)
        print(f"   {'MongoDB, document produit':<38}{d:>7.1f} ms   1 document")
        modifies = []
        d = commun.chrono(lambda: modifies.append(db.commandes_imbriquees.update_many(
            {"lignes.produit_id": 0}, {"$set": {"lignes.$[l].nom": next(noms)}},
            array_filters=[{"l.produit_id": 0}]).modified_count), 5)
        print(f"   {'MongoDB, copies imbriquées':<38}{d:>7.1f} ms   {nombre(modifies[-1])} documents")

        print("\n3. un document qui grandit : commentaires de 1 Kio ajoutés par $push")
        db.articles.insert_one({"_id": 1, "titre": "article", "commentaires": []})
        texte = "x" * 1024
        n, mesures = 0, []
        while True:
            t = time.perf_counter()
            try:
                db.articles.update_one({"_id": 1}, {"$push": {"commentaires": {"n": n, "texte": texte}}})
            except PyMongoError as e:
                print(f"   refus au commentaire {nombre(n + 1)} : {type(e).__name__}")
                print(f"   « {str(e).split(', full error')[0]} »")
                break
            d = time.perf_counter() - t
            n += 1
            if n in (1, 1000, 5000, 10000, 15000):
                taille = next(db.articles.aggregate([{"$project": {"t": {"$bsonSize": "$$ROOT"}}}]))["t"] / 2**20
                mesures.append((n, 1000 * d, taille))
        for n_, d, taille in mesures:
            print(f"   {nombre(n_):>6} commentaires : ajout en {d:6.2f} ms, document de {taille:5.1f} Mo")
    finally:
        commun.arreter("pg", "mongo")


if __name__ == "__main__":
    main()
