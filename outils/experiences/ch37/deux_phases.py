"""Chapitre 37 : la validation à deux phases (2PC) entre deux PostgreSQL.

Un virement débite un compte de la base pg1 et crédite un compte de la base pg2.
Le coordinateur, ce script, ouvre une transaction sur chacune, y fait sa mise à
jour, demande à chacune de se préparer (PREPARE TRANSACTION), écrit sa décision
dans son propre journal, synchronisé sur disque, puis demande à chacune de
valider (COMMIT PREPARED).

1. durée d'un virement : une seule base, deux bases sans 2PC, deux bases avec
   2PC, puis les mêmes à travers un relais qui retarde chaque sens de 5 ms ;
2. débit quand 16 clients débitent tous le même compte ;
3. panne du coordinateur après la préparation : verrous tenus, lectures,
   redémarrage d'un participant, nettoyage (VACUUM) empêché, reprise par le
   journal du coordinateur ; avec ou sans décision écrite avant la panne.

    python3 deux_phases.py
"""
import os
import random
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg

import commun

ICI = Path(__file__).parent
CACHE = Path.home() / ".cache" / "cours-sd-ch37"
JOURNAL = CACHE / "coordinateur.journal"
P1, P2 = commun.PG["pg1"], commun.PG["pg2"]
R1, R2 = 11711, 11712                     # relais vers pg1 et pg2
COMPTES = 1000


def preparer_bases():
    for port in (P1, P2):
        with commun.connecter(port) as c:
            c.execute("drop table if exists comptes, compteur")
            c.execute("create table comptes (id int primary key, solde bigint not null)")
            c.execute("insert into comptes select i, 1000000 from generate_series(1, %s) i",
                      (COMPTES,))
            c.execute("create table compteur (id int primary key, v bigint)")
            c.execute("insert into compteur values (1, 0)")
            c.execute("vacuum analyze")


def ecrire_journal(ligne):
    with open(JOURNAL, "a") as f:
        f.write(ligne + "\n")
        f.flush()
        os.fsync(f.fileno())


class Virements:
    """Un client qui fait des virements selon un mode ; les connexions passent
    par les relais si `loin` est vrai."""

    def __init__(self, mode, loin, nom, ordonne=False, attente_ms=0):
        p1, p2 = (R1, R2) if loin else (P1, P2)
        self.mode, self.nom, self.k, self.ordonne = mode, nom, 0, ordonne
        self.c1 = commun.connecter(p1)
        self.c2 = self.c1 if mode == "une base" else commun.connecter(p2)
        for c in {self.c1, self.c2}:
            c.execute(f"set lock_timeout = {attente_ms}")
        self.parallele = ThreadPoolExecutor(2)

    def deux(self, f1, f2):
        a, b = self.parallele.submit(f1), self.parallele.submit(f2)
        a.result()
        b.result()

    def virer(self, debit, credit):
        """Renvoie la durée pendant laquelle le compte débité est verrouillé."""
        c1, c2 = self.c1, self.c2
        if self.mode == "une base":
            debut = time.perf_counter()
            with c1.transaction():
                c1.execute("update comptes set solde = solde - 1 where id = %s", (debit,))
                c1.execute("update comptes set solde = solde + 1 where id = %s", (credit,))
            return time.perf_counter() - debut
        if self.mode == "sans 2PC":
            debut = time.perf_counter()
            c1.execute("update comptes set solde = solde - 1 where id = %s", (debit,))
            verrou = time.perf_counter() - debut
            c2.execute("update comptes set solde = solde + 1 where id = %s", (credit,))
            return verrou
        self.k += 1
        gid = f"v-{self.nom}-{self.k}"
        debut = time.perf_counter()
        debiter = lambda: (c1.execute("begin"), c1.execute(
            "update comptes set solde = solde - 1 where id = %s", (debit,)))
        crediter = lambda: (c2.execute("begin"), c2.execute(
            "update comptes set solde = solde + 1 where id = %s", (credit,)))
        try:
            if self.ordonne:                # toujours pg1, puis pg2
                debiter()
                crediter()
            else:                           # les deux en même temps
                self.deux(debiter, crediter)
        except psycopg.errors.LockNotAvailable:
            for c in (c1, c2):
                c.execute("rollback")
            return None                     # abandon : le client recommencera
        self.deux(lambda: c1.execute(f"prepare transaction '{gid}'"),
                  lambda: c2.execute(f"prepare transaction '{gid}'"))
        ecrire_journal(f"valider {gid}")
        self.deux(lambda: c1.execute(f"commit prepared '{gid}'"),
                  lambda: c2.execute(f"commit prepared '{gid}'"))
        return time.perf_counter() - debut

    def fermer(self):
        self.parallele.shutdown()
        self.c1.close()
        if self.c2 is not self.c1:
            self.c2.close()


MODES = [("une base", False), ("sans 2PC", False), ("2PC", False),
         ("une base", True), ("sans 2PC", True), ("2PC", True)]


def nom_mode(mode, loin):
    return f"{mode}{', 5 ms' if loin else ''}"


def partie_latence():
    print("1. durée d'un virement (300 virements, comptes au hasard)")
    print(f"   {'mode':22} {'médiane':>9} {'99e c.':>9}")
    for mode, loin in MODES:
        v = Virements(mode, loin, "lat")
        durees = []
        for _ in range(300):
            debut = time.perf_counter()
            v.virer(random.randint(2, COMPTES), random.randint(2, COMPTES))
            durees.append((time.perf_counter() - debut) * 1000)
        v.fermer()
        med, p99 = commun.centiles(durees)
        print(f"   {nom_mode(mode, loin):22} {med:7.2f}ms {p99:7.2f}ms")


def terminer_clients():
    for port in (P1, P2):
        with commun.connecter(port) as c:
            c.execute("select pg_terminate_backend(pid) from pg_stat_activity"
                      " where backend_type = 'client backend' and pid <> pg_backend_pid()")


def partie_debit():
    print("\n2. 16 clients débitent tous le compte 1 (pg1) et créditent l'un de")
    print("   20 comptes (pg2), pendant 5 s")
    print(f"   {'mode':34} {'virements/s':>11} {'abandons':>9} {'durée':>9}")
    variantes = [("une base", False, False, 0, "une base"),
                 ("2PC", False, False, 0, "2PC, verrous en parallèle"),
                 ("2PC", False, False, 200, "  + attente des verrous 200 ms"),
                 ("2PC", False, True, 0, "2PC, pg1 puis pg2"),
                 ("une base", True, False, 0, "une base, 5 ms"),
                 ("2PC", True, True, 0, "2PC, pg1 puis pg2, 5 ms")]
    for mode, loin, ordonne, attente, nom in variantes:
        preparer_bases()
        compte, abandons, verrous, arret = [0], [0], [], threading.Event()
        verrou = threading.Lock()

        def client(i):
            try:
                v = Virements(mode, loin, f"d{i}", ordonne, attente)
                while not arret.is_set():
                    t = v.virer(1, random.randint(2, 21))
                    with verrou:
                        if t is None:
                            abandons[0] += 1
                        else:
                            compte[0] += 1
                            verrous.append(t)
                v.fermer()
            except psycopg.OperationalError:
                pass                        # session terminée après un gel
        fils = [threading.Thread(target=client, args=(i,), daemon=True) for i in range(16)]
        debut = time.monotonic()
        for f in fils:
            f.start()
        gel, dernier, vu = None, 0, time.monotonic()
        while time.monotonic() - debut < 5:
            time.sleep(0.1)
            if compte[0] != dernier:
                dernier, vu = compte[0], time.monotonic()
            elif time.monotonic() - vu > 2:
                gel = (compte[0], vu - debut)
                break
        arret.set()
        if gel:
            terminer_clients()
        for f in fils:
            f.join(10)
        if gel:
            print(f"   {nom:34} figé après {gel[0]} virements ({gel[1]:.2f} s) ;")
            print(f"   {'':34} aucun serveur ne le détecte")
            continue
        moyenne = sum(verrous) / len(verrous) * 1000
        print(f"   {nom:34} {compte[0] / 5:11.0f} {abandons[0]:9} {moyenne:6.2f} ms")


COORDINATEUR = """
import os, sys, time
sys.path.insert(0, ".")
import commun
from deux_phases import ecrire_journal
decide = sys.argv[1] == "1"
c1, c2 = commun.connecter(commun.PG["pg1"]), commun.connecter(commun.PG["pg2"])
for c in (c1, c2):
    c.execute("begin")
    c.execute("update comptes set solde = solde + 100 where id = 7")
    c.execute("prepare transaction 'virement-42'")
if decide:
    ecrire_journal("valider virement-42")
print("pret", flush=True)
time.sleep(3600)
"""


def preparees(port):
    with commun.connecter(port) as c:
        return c.execute("select gid, round(extract(epoch from now() - prepared)::numeric, 1)"
                         " from pg_prepared_xacts").fetchall()


def essai_ecriture(port, compte, delai_ms):
    with commun.connecter(port) as c:
        c.execute(f"set lock_timeout = {delai_ms}")
        debut = time.perf_counter()
        try:
            c.execute("update comptes set solde = solde + 1 where id = %s", (compte,))
            return f"réussie en {(time.perf_counter() - debut) * 1000:.1f} ms"
        except psycopg.errors.LockNotAvailable:
            return f"bloquée, abandon au bout de {time.perf_counter() - debut:.2f} s"


def partie_panne():
    print("\n3. panne du coordinateur après la préparation sur pg1 et pg2")
    for decide in (True, False):
        preparer_bases()
        JOURNAL.unlink(missing_ok=True)
        p = subprocess.Popen([sys.executable, "-c", COORDINATEUR, "1" if decide else "0"],
                             stdout=subprocess.PIPE, text=True, cwd=ICI)
        p.stdout.readline()
        p.send_signal(signal.SIGKILL)
        p.wait()
        print(f"\n   décision écrite avant la panne : {'oui' if decide else 'non'}")
        print(f"   pg1 et pg2 voient : {[g for g, _ in preparees(P1)]} et"
              f" {[g for g, _ in preparees(P2)]}")
        print(f"   écriture du compte 7 : {essai_ecriture(P1, 7, 3000)}")
        print(f"   écriture du compte 8 : {essai_ecriture(P1, 8, 3000)}")
        with commun.connecter(P1) as c:
            solde = c.execute("select solde from comptes where id = 7").fetchone()[0]
        print(f"   lecture du compte 7 : {solde}, non bloquée")
        # nettoyage empêché : 20 000 mises à jour d'une autre table, puis VACUUM
        with commun.connecter(P1) as c:
            c.execute("set synchronous_commit = off")
            for _ in range(20000):
                c.execute("update compteur set v = v + 1 where id = 1")
            c.execute("vacuum compteur")
            mortes = c.execute("select n_dead_tup from pg_stat_user_tables"
                               " where relname = 'compteur'").fetchone()[0]
        print(f"   20 000 mises à jour d'une autre table, puis VACUUM : {mortes}")
        print("   versions mortes conservées")
        commun.docker("restart", "cours-sd-ch37-pg1")
        commun.attendre_pg(P1)
        print(f"   pg1 redémarré, voit encore : {[g for g, _ in preparees(P1)]}")
        # un client attend sans limite le verrou du compte 7 ; reprise 10 s après
        attente = {}

        def bloque():
            with commun.connecter(P1) as c:
                debut = time.monotonic()
                c.execute("update comptes set solde = solde + 1 where id = 7")
                attente["duree"] = time.monotonic() - debut
        f = threading.Thread(target=bloque)
        f.start()
        time.sleep(10)
        decision = "valider" if JOURNAL.exists() and "valider virement-42" in \
            JOURNAL.read_text() else "annuler"
        ordre = "commit" if decision == "valider" else "rollback"
        for port in (P1, P2):
            with commun.connecter(port) as c:
                c.execute(f"{ordre} prepared 'virement-42'")
        f.join()
        with commun.connecter(P1) as c:
            s1 = c.execute("select solde from comptes where id = 7").fetchone()[0]
            c.execute("vacuum compteur")
            mortes = c.execute("select n_dead_tup from pg_stat_user_tables"
                               " where relname = 'compteur'").fetchone()[0]
        with commun.connecter(P2) as c:
            s2 = c.execute("select solde from comptes where id = 7").fetchone()[0]
        print(f"   reprise : le journal dit « {decision} », {ordre} prepared")
        print(f"   un client a attendu le verrou du compte 7 {attente['duree']:.2f} s,"
              f" jusqu'à la reprise")
        print(f"   soldes du compte 7 : {s1} sur pg1 (avec ce client), {s2} sur pg2")
        print(f"   VACUUM après la reprise : {mortes} versions mortes")


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    commun.lancer_pg("pg1", "pg2")
    relais = [commun.Relais(R1, P1, 0.005), commun.Relais(R2, P2, 0.005)]
    try:
        with commun.connecter(P1) as c:
            version = c.execute("show server_version").fetchone()[0]
        print(f"PostgreSQL {version}, deux serveurs ; journal du coordinateur"
              f" synchronisé (fsync)\n")
        preparer_bases()
        partie_latence()
        partie_debit()
        partie_panne()
    finally:
        commun.supprimer("pg1", "pg2")
        JOURNAL.unlink(missing_ok=True)
        del relais


if __name__ == "__main__":
    main()
