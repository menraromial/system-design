"""Chapitre 7 : le problème N+1 en GraphQL, mesuré en comptant les requêtes SQL.

Un schéma (auteurs, leurs livres) servi par graphql-core au-dessus de SQLite,
résolu de deux façons : chaque champ « livres » fait sa propre requête, ou
bien les livres de tous les auteurs de la réponse sont chargés en une seule
requête groupée (le principe des DataLoader).
Chaque requête SQL peut être retardée de LATENCE_MS millisecondes, pour
simuler l'aller-retour réseau vers un serveur de base de données (SQLite, en
mémoire, n'en a pas).
Usage : python3 graphql_n1.py   (pip install graphql-core)
"""
import asyncio
import sqlite3
import time

from graphql import (GraphQLArgument, GraphQLField, GraphQLInt, GraphQLList, GraphQLObjectType,
                     GraphQLSchema, GraphQLString, graphql)

db = sqlite3.connect(":memory:")
db.execute("CREATE TABLE auteur (id INTEGER PRIMARY KEY, nom TEXT)")
db.execute("CREATE TABLE livre (id INTEGER PRIMARY KEY, auteur_id INTEGER, titre TEXT)")
db.execute("CREATE INDEX livre_auteur ON livre (auteur_id)")
db.executemany("INSERT INTO auteur VALUES (?, ?)", ((i, f"Auteur {i}") for i in range(1, 201)))
db.executemany("INSERT INTO livre VALUES (?, ?, ?)",
               ((i, (i - 1) // 5 + 1, f"Livre {i}") for i in range(1, 1001)))
requetes = []
LATENCE_MS = 0.0


def trace(sql):  # chaque instruction SQL exécutée est comptée, et retardée si demandé
    requetes.append(sql)
    if LATENCE_MS:
        time.sleep(LATENCE_MS / 1000)


db.set_trace_callback(trace)


class Chargeur:
    """Regroupe les demandes faites pendant un même tour de la boucle d'événements."""

    def __init__(self, charger_plusieurs):
        self.charger_plusieurs, self.attente = charger_plusieurs, {}

    def charger(self, cle):
        boucle = asyncio.get_running_loop()
        if not self.attente:
            boucle.call_soon(self._expedier)
        return self.attente.setdefault(cle, boucle.create_future())

    def _expedier(self):
        attente, self.attente = self.attente, {}
        for cle, valeur in self.charger_plusieurs(list(attente)).items():
            attente[cle].set_result(valeur)


def livres_de(ids):
    marques = ",".join("?" * len(ids))
    resultat = {i: [] for i in ids}
    for auteur_id, titre in db.execute(
            f"SELECT auteur_id, titre FROM livre WHERE auteur_id IN ({marques})", ids):
        resultat[auteur_id].append({"titre": titre})
    return resultat


def schema(groupe):
    livre = GraphQLObjectType("Livre", {"titre": GraphQLField(GraphQLString)})

    def resoudre_livres(auteur, info):
        if groupe:
            return info.context["chargeur"].charger(auteur["id"])
        return [{"titre": t} for (t,) in db.execute(
            "SELECT titre FROM livre WHERE auteur_id = ?", (auteur["id"],))]

    auteur = GraphQLObjectType("Auteur", {
        "nom": GraphQLField(GraphQLString),
        "livres": GraphQLField(GraphQLList(livre), resolve=resoudre_livres)})

    def resoudre_auteurs(_, info, premiers):
        return [{"id": i, "nom": n} for i, n in db.execute(
            "SELECT id, nom FROM auteur ORDER BY id LIMIT ?", (premiers,))]

    return GraphQLSchema(GraphQLObjectType("Query", {
        "auteurs": GraphQLField(GraphQLList(auteur), resolve=resoudre_auteurs,
                                args={"premiers": GraphQLArgument(GraphQLInt)})}))


REQUETE = "{ auteurs(premiers: 100) { nom livres { titre } } }"


async def executer(groupe):
    requetes.clear()
    debut = time.perf_counter()
    resultat = await graphql(schema(groupe), REQUETE, context_value={"chargeur": Chargeur(livres_de)})
    duree = (time.perf_counter() - debut) * 1000
    assert not resultat.errors, resultat.errors
    nb_livres = sum(len(a["livres"]) for a in resultat.data["auteurs"])
    return len(requetes), duree, nb_livres


async def main():
    global LATENCE_MS
    print(f"requête GraphQL : {REQUETE}")
    for LATENCE_MS in (0.0, 0.5):
        print(f"-- latence simulée par requête SQL : {LATENCE_MS} ms")
        for groupe, nom in ((False, "un chargement par auteur"), (True, "chargement groupé")):
            n, duree, livres = await executer(groupe)
            print(f"{nom:26} : {n:3d} requêtes SQL, {duree:6.1f} ms, {livres} livres renvoyés")


if __name__ == "__main__":
    asyncio.run(main())
