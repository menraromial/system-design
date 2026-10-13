"""Chapitre 20 : deux B-tree et un arbre LSM, sous la même charge.

2 millions de paires clé-valeur (clé de 16 octets, valeur de 100 octets
aléatoires, donc incompressibles) sont écrites par lots de 1 000, chaque lot
étant durable avant le suivant (synchronisation du disque à chaque lot), dans :
  - LMDB : un B+-tree en copie sur écriture ;
  - SQLite : un B-tree (table WITHOUT ROWID), journal WAL, synchronous = FULL ;
  - RocksDB : un arbre LSM, compaction par niveaux (leveled), avec et sans
    filtre de Bloom (10 bits par clé), puis compaction par paliers (universal).
Les réglages de RocksDB sont réduits 16 fois (memtable de 4 Mio, niveau 1 de
16 Mio, facteur 10 entre niveaux) pour que 2 millions de clés occupent plusieurs
niveaux, comme le feraient 32 millions de clés avec les réglages par défaut.
Chaque moteur est mesuré avec des clés aléatoires, puis LMDB et RocksDB avec des
clés croissantes ; enfin, toutes les clés sont réécrites une fois (mise à jour).
Mesures : débit d'écriture ; octets écrits sur le disque par le processus,
rapportés aux octets écrits par l'application (amplification en écriture),
mesurés par le compteur du disque (voir commun.py) ;
place occupée sur le disque à la fin et au plus fort de l'écriture, rapportée
aux données (amplification en espace) ;
durée moyenne d'une lecture d'une clé présente et d'une clé absente (100 000
lectures, données dans le cache du système) ; pour RocksDB, fichiers par niveau
fichiers écartés par le filtre de Bloom, et amplification en écriture comptée
par RocksDB lui-même, pour vérifier celle du disque.
Prérequis : Linux, pip install -r requirements.txt. Usage : python3 moteurs.py
"""
import os
import random
import re
import sqlite3
import threading
import time

import lmdb
import rocksdict as rd

import commun

N, LOT, LECTURES = 2_000_000, 1_000, 100_000
UTILE = N * (16 + 100)                          # octets écrits par l'application


class Lmdb:
    nom = "LMDB"

    def __init__(self, chemin):
        os.makedirs(chemin)
        self.env = lmdb.open(chemin, map_size=8 << 30, sync=True, metasync=True)

    def ecrire(self, lot):
        with self.env.begin(write=True) as txn:
            for k, v in lot:
                txn.put(k, v)

    def lire(self, cles):
        with self.env.begin() as txn:
            for k in cles:
                txn.get(k)

    def fermer(self):
        self.env.close()


class Sqlite:
    nom = "SQLite"

    def __init__(self, chemin):
        os.makedirs(chemin)
        self.cx = sqlite3.connect(os.path.join(chemin, "kv.db"), isolation_level=None)
        self.cx.execute("PRAGMA journal_mode = WAL")
        self.cx.execute("PRAGMA synchronous = FULL")
        self.cx.execute("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID")

    def ecrire(self, lot):
        self.cx.execute("BEGIN")
        self.cx.executemany("INSERT OR REPLACE INTO kv VALUES (?, ?)", lot)
        self.cx.execute("COMMIT")

    def lire(self, cles):
        cur = self.cx.cursor()
        for k in cles:
            cur.execute("SELECT v FROM kv WHERE k = ?", (k,)).fetchone()

    def fermer(self):
        self.cx.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.cx.close()


class Rocks:
    def __init__(self, chemin, style="niveaux", bloom=True):
        self.nom = f"RocksDB {style}{'' if bloom else ' sans Bloom'}"
        o = rd.Options(raw_mode=True)
        o.create_if_missing(True)
        o.enable_statistics()
        o.set_compression_type(rd.DBCompressionType.none())
        o.set_write_buffer_size(4 << 20)
        o.set_target_file_size_base(4 << 20)
        o.set_max_bytes_for_level_base(16 << 20)
        o.set_max_bytes_for_level_multiplier(10)
        o.set_level_compaction_dynamic_level_bytes(False)
        o.set_max_background_jobs(4)
        if style == "paliers":
            o.set_compaction_style(rd.DBCompactionStyle.universal())
        table = rd.BlockBasedOptions()
        if bloom:
            table.set_bloom_filter(10, False)
        o.set_block_based_table_factory(table)
        self.options = o
        self.db = rd.Rdict(chemin, options=o)
        self.sync = rd.WriteOptions()
        self.sync.sync = True

    def ecrire(self, lot):
        wb = rd.WriteBatch(raw_mode=True)
        for k, v in lot:
            wb.put(k, v)
        self.db.write(wb, self.sync)

    def attendre_compactions(self):
        """Attend la fin des vidages et des compactions. La compaction par paliers
        peut laisser l'indicateur « compaction-pending » levé sans rien lancer :
        on s'arrête quand rien ne tourne et que les fichiers n'ont pas changé
        depuis 3 s."""
        calme, avant = 0.0, None
        while calme < 3:
            time.sleep(0.2)
            fichiers = sorted(os.listdir(self.db.path()))
            actif = (self.db.property_int_value("rocksdb.num-running-compactions")
                     or self.db.property_int_value("rocksdb.num-running-flushes"))
            pending = self.db.property_int_value("rocksdb.compaction-pending")
            if actif or fichiers != avant:
                calme = 0.0
            elif pending:
                calme += 0.2
            else:
                return
            avant = fichiers

    def lire(self, cles):
        for k in cles:
            self.db.get(k)

    def niveaux(self):
        return [self.db.property_int_value(f"rocksdb.num-files-at-level{i}") for i in range(7)]

    def stat(self, nom):
        m = re.search(rf"^{re.escape(nom)} COUNT : (\d+)", self.options.get_statistics(), re.M)
        return int(m.group(1)) if m else 0

    def fermer(self):
        self.db.close()


def charger(moteur, chemin, cles, valeurs):
    """Écrit les paires par lots ; renvoie le débit, l'amplification en écriture et
    la plus grande place occupée pendant l'écriture (relevée toutes les 0,2 s)."""
    pic, fini = [0], threading.Event()

    def surveiller():
        while not fini.wait(0.2):
            pic[0] = max(pic[0], commun.taille(chemin))

    fil = threading.Thread(target=surveiller)
    fil.start()
    avant = commun.octets_ecrits()
    t = time.perf_counter()
    for i in range(0, len(cles), LOT):
        moteur.ecrire(list(zip(cles[i:i + LOT], valeurs[i:i + LOT])))
    if isinstance(moteur, Rocks):
        moteur.attendre_compactions()
    duree = time.perf_counter() - t
    ecrits = commun.octets_ecrits() - avant
    fini.set()
    fil.join()
    return len(cles) / duree, ecrits / (len(cles) * 116), max(pic[0], commun.taille(chemin))


def lectures(moteur, presentes, absentes):
    res = []
    for cles in (presentes, absentes):
        t = time.perf_counter()
        moteur.lire(cles)
        res.append(1e6 * (time.perf_counter() - t) / len(cles))
    return res


def nombre(x):
    return f"{x:,.0f}".replace(",", " ")


def ligne(nom, debit, ampl, place, pic, suite=""):
    print(f"   {nom:<26}{nombre(debit):>9}{ampl:>7.1f}×{mo(place):>8}{place / UTILE:>6.2f}×"
          f"{pic / UTILE:>6.2f}×{suite}")


def mo(octets):
    return f"{octets / 2**20:.0f} Mo"


def main():
    alea = random.Random(20)
    cles = [alea.randbytes(16) for _ in range(N)]
    valeurs = [alea.randbytes(100) for _ in range(N)]
    croissantes = [i.to_bytes(16, "big") for i in range(N)]
    presentes = alea.sample(cles, LECTURES)
    absentes = [alea.randbytes(16) for _ in range(LECTURES)]
    dossier = commun.preparer()
    print(f"{nombre(N)} clés de 16 o, valeurs de 100 o : {mo(UTILE)} ; lots durables de {nombre(LOT)}")
    try:
        print("\n1. clés aléatoires")
        print("   écr./s : écritures par seconde ; écrit : octets écrits sur le disque")
        print("   / octets utiles ; espace : place finale / octets utiles ; pic : place")
        print("   maximale pendant l'écriture / octets utiles")
        print(f"   {'moteur':<26}{'écr./s':>9}{'écrit':>8}{'taille':>8}{'espace':>7}{'pic':>7}")
        fabriques = [("lmdb", Lmdb), ("sqlite", Sqlite),
                     ("rocks-niv", lambda c: Rocks(c, "niveaux", True)),
                     ("rocks-niv-sans", lambda c: Rocks(c, "niveaux", False)),
                     ("rocks-pal", lambda c: Rocks(c, "paliers", True))]
        details, lus = [], []
        for nom, fab in fabriques:
            chemin = os.path.join(dossier, nom)
            m = fab(chemin)
            debit, ampl, pic = charger(m, chemin, cles, valeurs)
            lu, absent = lectures(m, presentes, absentes)
            if isinstance(m, Rocks):
                interne = sum(m.stat(f"rocksdb.{x}") for x in ("wal.bytes", "flush.write.bytes", "compact.write.bytes"))
                details.append((m.nom, m.niveaux(), m.stat("rocksdb.bloom.filter.useful"), interne / UTILE))
            m.fermer()
            place = commun.taille(chemin)
            ligne(m.nom, debit, ampl, place, pic)
            lus.append((m.nom, lu, absent))
            if nom in ("lmdb", "rocks-niv", "rocks-pal"):      # gardés pour la partie 3
                os.rename(chemin, chemin + "-garde")
            else:
                commun.supprimer(chemin)

        print("\n   durée moyenne d'une lecture, données dans le cache du système")
        print(f"   {'moteur':<26}{'clé présente':>14}{'clé absente':>14}")
        for nom, lu, absent in lus:
            print(f"   {nom:<26}{lu:>11.1f} µs{absent:>11.1f} µs")

        print("\n   RocksDB : fichiers par niveau (L0 à L6) ; filtre : fichiers écartés")
        print("   par le filtre de Bloom ; interne : octets écrits comptés par RocksDB")
        print("   (journal, vidages, compactions) / octets utiles")
        print(f"   {'moteur':<28}{'niveaux':<14}{'filtre':>10}{'interne':>9}")
        for nom, niv, utiles, interne in details:
            print(f"   {nom:<28}{' '.join(str(n) for n in niv):<14}{nombre(utiles):>10}{interne:>8.1f}×")

        print("\n2. clés croissantes")
        for nom, fab in (("lmdb-c", Lmdb), ("rocks-c", lambda c: Rocks(c, "niveaux", True))):
            chemin = os.path.join(dossier, nom)
            m = fab(chemin)
            debit, ampl, pic = charger(m, chemin, croissantes, valeurs)
            m.fermer()
            place = commun.taille(chemin)
            commun.supprimer(chemin)
            ligne(m.nom, debit, ampl, place, pic)

        print("\n3. réécriture de toutes les clés (nouvelles valeurs, ordre aléatoire)")
        reecrites = cles[:]
        alea.shuffle(reecrites)
        nouvelles = [alea.randbytes(100) for _ in range(N)]
        for nom in ("lmdb", "rocks-niv", "rocks-pal"):
            chemin = os.path.join(dossier, nom + "-garde")
            if nom == "lmdb":
                m = Lmdb.__new__(Lmdb)
                m.env = lmdb.open(chemin, map_size=8 << 30, sync=True, metasync=True)
            else:
                m = Rocks(chemin, "niveaux" if nom == "rocks-niv" else "paliers", True)
            debit, ampl, pic = charger(m, chemin, reecrites, nouvelles)
            place = commun.taille(chemin)
            m.fermer()
            ligne(m.nom, debit, ampl, place, pic)
    finally:
        commun.nettoyer()


if __name__ == "__main__":
    main()
