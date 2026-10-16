"""Chapitre 21 : Cassandra, ou une table par requête.

200 000 messages, répartis dans 2 000 conversations et écrits par 1 000 auteurs,
sont rangés dans une table dont la clé de partition est la conversation et la
clé de tri la date d'envoi (la plus récente d'abord).
  1. lire les 20 derniers messages d'une conversation : la requête pour
     laquelle la table a été conçue (médiane sur 200) ;
  2. lire les messages d'un auteur : Cassandra refuse la requête ;
  3. la même avec ALLOW FILTERING : elle passe, en parcourant toute la table ;
  4. la réponse de la modélisation guidée par les requêtes : une seconde table,
     partitionnée par auteur, écrite en même temps que la première (lot
     journalisé) ; on mesure la lecture et le coût du chargement doublé.
Prérequis : docker, image cassandra:5, pip install -r requirements.txt.
Usage : python3 colonnes.py
"""
import random
import textwrap
import statistics
import time
import uuid

from cassandra import InvalidRequest
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.query import BatchStatement

import commun

MESSAGES, CONVERSATIONS, AUTEURS = 200_000, 2_000, 1_000


def connecter():
    cluster = Cluster(["127.0.0.1"], port=10142, connection_class=AsyncioConnection)
    return cluster, cluster.connect()


def mediane(f, args):
    durees = []
    for a in args:
        t = time.perf_counter()
        r = f(a)
        durees.append(1000 * (time.perf_counter() - t))
    return statistics.median(durees), r


def nombre(x):
    return f"{x:,}".replace(",", " ")


def main():
    alea = random.Random(21)
    t0 = 1_760_000_000
    messages = [(alea.randrange(CONVERSATIONS), uuid.uuid1(node=0, clock_seq=i),
                 alea.randrange(AUTEURS), f"message {i}") for i in range(MESSAGES)]
    try:
        cluster, s = commun.lancer("cassandra", connecter, delai=300)
        s.execute("CREATE KEYSPACE cours WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        s.set_keyspace("cours")
        s.execute("""CREATE TABLE messages_par_conversation (
                       conversation_id int, envoye_le timeuuid, auteur int, texte text,
                       PRIMARY KEY ((conversation_id), envoye_le))
                     WITH CLUSTERING ORDER BY (envoye_le DESC)""")
        s.execute("""CREATE TABLE messages_par_auteur (
                       auteur int, envoye_le timeuuid, conversation_id int, texte text,
                       PRIMARY KEY ((auteur), envoye_le))
                     WITH CLUSTERING ORDER BY (envoye_le DESC)""")
        ins_c = s.prepare("INSERT INTO messages_par_conversation (conversation_id, envoye_le, auteur, texte) "
                          "VALUES (?, ?, ?, ?)")
        ins_a = s.prepare("INSERT INTO messages_par_auteur (auteur, envoye_le, conversation_id, texte) "
                          "VALUES (?, ?, ?, ?)")
        print(f"{nombre(MESSAGES)} messages, {nombre(CONVERSATIONS)} conversations, {nombre(AUTEURS)} auteurs")

        t = time.perf_counter()
        execute_concurrent_with_args(s, ins_c, messages, concurrency=64)
        une = time.perf_counter() - t
        print(f"   chargement d'une table : {une:5.1f} s")

        alea = random.Random(3)
        convs = [alea.randrange(CONVERSATIONS) for _ in range(200)]
        q1 = s.prepare("SELECT envoye_le, auteur, texte FROM messages_par_conversation "
                       "WHERE conversation_id = ? LIMIT 20")
        med, r = mediane(lambda c: list(s.execute(q1, (c,))), convs)
        print(f"\n1. 20 derniers messages d'une conversation : {med:.2f} ms")
        print(f"   (médiane sur 200, {len(r)} lignes)")

        print("\n2. messages d'un auteur, sur la table par conversation")
        try:
            s.execute("SELECT * FROM messages_par_conversation WHERE auteur = 42")
            print("   accepté")
        except InvalidRequest as e:
            msg = str(e)
            debut = msg.find("Cannot execute")
            texte = msg[debut:].split('"')[0]
            print("   refusé :")
            print(textwrap.fill(texte, 66, initial_indent="   « ", subsequent_indent="     ") + " »")

        t = time.perf_counter()
        r = list(s.execute("SELECT * FROM messages_par_conversation WHERE auteur = 42 ALLOW FILTERING",
                           timeout=120))
        print(f"\n3. la même avec ALLOW FILTERING : {1000 * (time.perf_counter() - t):.0f} ms")
        print(f"   {len(r)} lignes retenues sur {nombre(MESSAGES)} lues")

        s.execute("TRUNCATE messages_par_conversation")
        t = time.perf_counter()

        def lot(m):
            b = BatchStatement()
            b.add(ins_c, m)
            b.add(ins_a, (m[2], m[1], m[0], m[3]))
            return b

        from cassandra.concurrent import execute_concurrent
        execute_concurrent(s, [(lot(m), ()) for m in messages], concurrency=64)
        deux = time.perf_counter() - t
        print(f"\n4. deux tables écrites ensemble (lots journalisés) : {deux:.1f} s")
        print(f"   ({deux / une:.1f} fois le chargement d'une table)")
        auteurs = [alea.randrange(AUTEURS) for _ in range(200)]
        q2 = s.prepare("SELECT envoye_le, conversation_id, texte FROM messages_par_auteur WHERE auteur = ?")
        med, r = mediane(lambda a: list(s.execute(q2, (a,))), auteurs)
        print(f"   messages d'un auteur, table par auteur : {med:.2f} ms")
        print(f"   (médiane sur 200, {len(r)} lignes)")
        cluster.shutdown()
    finally:
        commun.arreter("cassandra")


if __name__ == "__main__":
    main()
