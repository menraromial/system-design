"""Chapitre 28 : ce que perd ou double un consommateur Kafka qui plante.

10 000 paiements (identifiants 0 à 9 999) sont écrits dans un sujet Kafka de 3
partitions. Un consommateur les lit et, pour chacun, écrit une ligne dans une
base SQLite (l'« effet » du traitement). Il plante cinq fois, brutalement
(os._exit), après un nombre aléatoire de messages ; un nouveau processus
reprend chaque fois là où le groupe en était. À la fin, on compte les
paiements perdus (jamais écrits) et les doublons (écrits deux fois).
  1. quatre façons de faire :
     - au plus une fois : la position est validée (commit) avant le traitement ;
     - au moins une fois : le traitement, puis la validation de la position ;
     - réglages par défaut : validation automatique toutes les 5 s ;
     - consommateur idempotent : l'effet (clé unique) et la position sont
       écrits dans la même transaction SQLite, et la lecture reprend à la
       position enregistrée dans SQLite.
  2. lire, transformer, écrire dans un autre sujet : avec un producteur
     ordinaire, ou avec une transaction Kafka qui écrit les messages et valide
     la position ensemble ; on relit le sujet de sortie en read_committed et
     en read_uncommitted. Puis le débit de chacune, sans plantage.
Prérequis : docker, image apache/kafka, pip install -r requirements.txt.
Usage : python3 kafka_garanties.py
"""
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

import commun

N, ENTREE = 10_000, "paiements"
DOSSIER = Path.home() / ".cache" / "cours-sd-ch28"
VIES = 5


def base(chemin):
    sq = sqlite3.connect(chemin, isolation_level=None)
    sq.execute("PRAGMA journal_mode=WAL")
    sq.execute("PRAGMA synchronous=FULL")
    sq.execute("CREATE TABLE IF NOT EXISTS effets (id INTEGER)")
    sq.execute("CREATE TABLE IF NOT EXISTS effets_uniques (id INTEGER PRIMARY KEY)")
    sq.execute("CREATE TABLE IF NOT EXISTS positions (partition INTEGER PRIMARY KEY, suivant INTEGER)")
    return sq


# ---- une vie de consommateur (sous-processus) ------------------------------------
def tout_lu(c, delai_fin, enregistrees=None):
    """Vrai si le consommateur a les 3 partitions et les a lues jusqu'au bout. La position
    d'une partition dont il n'a rien reçu est inconnue (-1001) : on prend alors la position
    validée dans le groupe, ou celle enregistrée dans SQLite."""
    parts = c.assignment()
    if len(parts) < 3:
        return time.monotonic() > delai_fin
    positions = c.position(parts)
    validees = {p.partition: p.offset for p in c.committed(parts, timeout=5)}
    for p in positions:
        lu = p.offset
        if lu < 0:
            lu = validees.get(p.partition, -1)
        if lu < 0 and enregistrees:
            lu = enregistrees().get(p.partition, -1)
        if lu < c.get_watermark_offsets(p, cached=False)[1]:
            return time.monotonic() > delai_fin
    return True


def vie(mode, crash, chemin):
    sq = base(chemin)
    conf = {"bootstrap.servers": commun.KAFKA, "group.id": mode, "auto.offset.reset": "earliest",
            "enable.auto.commit": mode == "defaut", "session.timeout.ms": 6000}
    c = Consumer(conf)

    def reprendre(cons, parts):
        if mode == "idempotent":                 # la position fait foi dans SQLite
            enregistrees = dict(sq.execute("SELECT partition, suivant FROM positions").fetchall())
            for p in parts:
                if p.partition in enregistrees:
                    p.offset = enregistrees[p.partition]
            cons.assign(parts)

    c.subscribe([ENTREE], on_assign=reprendre)
    n, fin = 0, time.monotonic() + 120

    def compter():
        nonlocal n
        n += 1
        if crash and n >= crash:
            os._exit(1)                          # panne brutale : rien n'est fermé ni validé

    while True:
        lot = [m for m in c.consume(100, timeout=1) if not m.error()]
        if not lot:
            if tout_lu(c, fin, lambda: dict(sq.execute("SELECT partition, suivant FROM positions").fetchall())):
                break
            continue
        if mode == "au_plus_une_fois":
            c.commit(asynchronous=False)
            for m in lot:
                compter()
                sq.execute("INSERT INTO effets VALUES (?)", (int(m.value()),))
        elif mode in ("au_moins_une_fois", "defaut"):
            for m in lot:
                sq.execute("INSERT INTO effets VALUES (?)", (int(m.value()),))
                compter()
            if mode == "au_moins_une_fois":
                c.commit(asynchronous=False)
        else:
            sq.execute("BEGIN")
            suivants = {}
            for m in lot:
                sq.execute("INSERT OR IGNORE INTO effets_uniques VALUES (?)", (int(m.value()),))
                suivants[m.partition()] = m.offset() + 1
                compter()
            for p, s in suivants.items():
                sq.execute("INSERT OR REPLACE INTO positions VALUES (?, ?)", (p, s))
            sq.execute("COMMIT")
    c.close()


def vie_transformation(mode, crash, sortie, prefixe="transfo", taille_lot=100):
    c = Consumer({"bootstrap.servers": commun.KAFKA, "group.id": f"{prefixe}-{mode}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False, "session.timeout.ms": 6000})
    c.subscribe([ENTREE])
    if mode == "transaction":
        p = Producer({"bootstrap.servers": commun.KAFKA, "transactional.id": f"{prefixe}-transformateur"})
        p.init_transactions()
    else:
        p = Producer({"bootstrap.servers": commun.KAFKA, "enable.idempotence": True})
    n, fin, debut = 0, time.monotonic() + 120, None
    while True:
        lot = [m for m in c.consume(taille_lot, timeout=1) if not m.error()]
        if not lot:
            if tout_lu(c, fin):
                break
            continue
        debut = debut or time.perf_counter()
        if mode == "transaction":
            p.begin_transaction()
        for m in lot:
            p.produce(sortie, key=m.key(), value=m.value())
            n += 1
            if crash and n >= crash:
                p.flush()                        # les messages déjà produits partent au courtier
                os._exit(1)
        if mode == "transaction":
            p.send_offsets_to_transaction(c.position(c.assignment()), c.consumer_group_metadata())
            p.commit_transaction()
        else:
            p.flush()
            c.commit(asynchronous=False)
    if debut:
        print(f"{n} {time.perf_counter() - debut:.3f}")
    c.close()


# ---- le processus principal ------------------------------------------------------------
def lancer_vies(args, graine):
    alea = random.Random(graine)
    plantages = 0
    for k in range(VIES + 1):
        crash = alea.randint(700, 1500) if k < VIES else 0
        r = subprocess.run([sys.executable, __file__, *args, str(crash)], capture_output=True, text=True)
        plantages += r.returncode != 0
        if r.returncode not in (0, 1):
            print(r.stderr[-800:])
    return plantages


def compter_sortie(sujet, isolation):
    c = Consumer({"bootstrap.servers": commun.KAFKA, "group.id": f"lecture-{sujet}-{isolation}",
                  "auto.offset.reset": "earliest", "isolation.level": isolation})
    c.subscribe([sujet])
    vus, total, vides = set(), 0, 0
    while vides < 3:
        lot = [m for m in c.consume(500, timeout=1) if not m.error()]
        if not lot:
            vides += 1
            continue
        vides = 0
        for m in lot:
            total += 1
            vus.add(int(m.value()))
    c.close()
    return total, len(vus)


def main():
    shutil.rmtree(DOSSIER, ignore_errors=True)
    DOSSIER.mkdir(parents=True)
    try:
        version = commun.lancer_kafka()
        admin = AdminClient({"bootstrap.servers": commun.KAFKA})
        sujets = [NewTopic(s, num_partitions=3, replication_factor=1)
                  for s in (ENTREE, "sortie-simple", "sortie-transaction", "debit-simple", "debit-transaction")]
        for f in admin.create_topics(sujets).values():
            f.result()
        p = Producer({"bootstrap.servers": commun.KAFKA, "enable.idempotence": True})
        for i in range(N):
            p.produce(ENTREE, key=f"c{i % 100}", value=str(i))
        p.flush()
        print(f"Kafka {version}, {commun.nombre(N)} paiements dans 3 partitions ;"
              f" {VIES} plantages par essai")

        print("\n1. effets écrits dans SQLite")
        print(f"   {'méthode':<28}{'lignes':>8}{'perdus':>8}{'doublons':>10}")
        for graine, mode in enumerate(("au_plus_une_fois", "au_moins_une_fois", "defaut", "idempotent")):
            chemin = str(DOSSIER / f"{mode}.db")
            plantages = lancer_vies(["--vie", mode, chemin], graine)
            sq = base(chemin)
            table = "effets_uniques" if mode == "idempotent" else "effets"
            lignes, distincts = sq.execute(f"SELECT count(*), count(DISTINCT id) FROM {table}").fetchone()
            nom = {"au_plus_une_fois": "au plus une fois", "au_moins_une_fois": "au moins une fois",
                   "defaut": "validation automatique", "idempotent": "consommateur idempotent"}[mode]
            print(f"   {nom:<28}{lignes:>8}{N - distincts:>8}{lignes - distincts:>10}"
                  + ("" if plantages == VIES else f"  ({plantages} plantages)"))

        print("\n2. lire, transformer, écrire dans un autre sujet")
        print(f"   {'méthode':<16}{'lecture':<18}{'messages':>9}{'distincts':>11}")
        for graine, mode in enumerate(("simple", "transaction"), start=10):
            lancer_vies(["--transfo", mode, f"sortie-{mode}"], graine)
            for isolation in ("read_uncommitted", "read_committed"):
                total, distincts = compter_sortie(f"sortie-{mode}", isolation)
                print(f"   {mode:<16}{isolation:<18}{total:>9}{distincts:>11}")
        print("   débit sans plantage, messages par seconde :")
        for lot in (100, 1000):
            debits = []
            for mode in ("simple", "transaction"):
                r = subprocess.run([sys.executable, __file__, "--transfo-debit", mode, f"debit-{mode}", "0",
                                    str(lot)], check=True, capture_output=True, text=True)
                n, d = r.stdout.split()
                debits.append(int(n) / float(d))
            print(f"      lots de {lot:>4} : simple {commun.nombre(debits[0]):>6},"
                  f" transaction {commun.nombre(debits[1]):>6}")
    finally:
        commun.supprimer("kafka")
        shutil.rmtree(DOSSIER, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--vie":
        vie(sys.argv[2], int(sys.argv[4]), sys.argv[3])
    elif len(sys.argv) > 1 and sys.argv[1] in ("--transfo", "--transfo-debit"):
        if sys.argv[1] == "--transfo-debit":
            vie_transformation(sys.argv[2], 0, sys.argv[3], f"debit{sys.argv[5]}", int(sys.argv[5]))
        else:
            vie_transformation(sys.argv[2], int(sys.argv[4]), sys.argv[3])
    else:
        main()
