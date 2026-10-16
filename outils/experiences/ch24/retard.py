"""Chapitre 24 : d'où vient le retard d'une réplique, et comment lire ses écritures.

Un primaire PostgreSQL 18 et une réplique asynchrone r1 (voir commun.py). Trois
mesures :
  1. une grosse transaction (UPDATE d'un million de lignes) : durée sur le
     primaire, journal produit, et délai jusqu'à ce que la réplique la montre ;
  2. lire ses propres écritures, liaison retardée de 5 ms dans chaque sens :
     500 fois, écrire sur le primaire puis lire aussitôt sur la réplique ;
     puis la même chose en attendant que la réplique ait rejoué la position
     du journal (LSN) de l'écriture ;
  3. une requête longue sur la réplique pendant que le primaire modifie et
     nettoie (VACUUM) la table qu'elle lit : la réplique suspend le rejeu du
     journal (max_standby_streaming_delay = 30 s par défaut), puis annule la
     requête ; avec hot_standby_feedback = on, ni pause ni annulation, mais le
     primaire ne peut plus nettoyer.
Le retard est lu sur une ligne « battement » que le primaire met à jour toutes
les 20 ms : retard = heure actuelle - heure inscrite dans la ligne lue sur la
réplique (les deux conteneurs partagent l'horloge de la machine).
Prérequis : docker, image postgres:18-alpine, pip install -r requirements.txt.
Usage : python3 retard.py
"""
import threading
import time

import psycopg

import commun

LIGNES = 1_000_000


def battement(arret):
    with commun.connecter(commun.PRIMAIRE) as c:
        c.execute("SET synchronous_commit = local")
        while not arret.is_set():
            c.execute("UPDATE battement SET t = clock_timestamp()")
            time.sleep(0.02)


def retard(r):
    return r.execute("SELECT extract(epoch FROM clock_timestamp() - t) FROM battement").fetchone()[0]


def conflit(p, r):
    """Requête de 60 s sur la réplique pendant UPDATE + VACUUM sur le primaire."""
    resultat, avis = {}, []

    def lecteur():
        with commun.connecter(commun.REPLIQUES["r1"][0]) as c:
            t = time.perf_counter()
            try:
                c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
                c.execute("SELECT count(*) FROM grosse").fetchone()
                c.execute("SELECT pg_sleep(60)")
                c.execute("COMMIT")
                resultat["fin"] = f" terminée normalement après {time.perf_counter() - t:.0f} s"
            except psycopg.Error as e:
                resultat["fin"] = (f" annulée après {time.perf_counter() - t:.1f} s :\n"
                                   f"      « {e.diag.message_primary} »")

    fil = threading.Thread(target=lecteur)
    fil.start()
    time.sleep(1)
    p.execute("UPDATE grosse SET v = v + 1 WHERE id % 10 = 0")
    noter = lambda d: avis.append(d.message_primary)
    p.add_notice_handler(noter)
    p.execute("VACUUM (VERBOSE) grosse")
    p.remove_notice_handler(noter)
    pire, t0 = 0.0, time.monotonic()
    echantillons = []
    while fil.is_alive() and time.monotonic() - t0 < 70:
        d = retard(r)
        pire = max(pire, d)
        echantillons.append(d)
        time.sleep(0.5)
    fil.join()
    print(f"   requête sur la réplique{resultat['fin']}")
    print(f"   retard maximal de la réplique {pire:.1f} s ; toutes les 5 s :")
    print("      " + " ".join(f"{x:.0f}" for x in echantillons[::10]))
    for a in avis:
        for ligne in a.splitlines():
            if "tuples:" in ligne and "0 removed, 0 remain" not in ligne:
                debut, _, fin = ligne.strip().rpartition(", ")
                print(f"   VACUUM sur le primaire :\n      {debut},\n      {fin}")
    time.sleep(2)
    print(f"   retard après la requête : {retard(r):.1f} s")


def main():
    commun.supprimer()
    relais = commun.Relais(commun.REPLIQUES["r1"][1], commun.PRIMAIRE)
    arret = threading.Event()
    try:
        p = commun.lancer_primaire()
        p.execute("CREATE TABLE battement (t timestamptz)")
        p.execute("INSERT INTO battement VALUES (clock_timestamp())")
        p.execute("CREATE TABLE comptes (id int PRIMARY KEY, solde int NOT NULL DEFAULT 0)")
        p.execute("INSERT INTO comptes (id) SELECT generate_series(0, 999)")
        p.execute("CREATE TABLE grosse (id int PRIMARY KEY, v int NOT NULL, texte text NOT NULL)"
                  " WITH (autovacuum_enabled = off)")
        p.execute(f"INSERT INTO grosse SELECT i, 0, repeat('x', 60) FROM generate_series(0, {LIGNES - 1}) i")
        p.execute("VACUUM ANALYZE grosse")
        r = commun.lancer_replique("r1")
        commun.attendre_flux(p, "r1")
        threading.Thread(target=battement, args=(arret,), daemon=True).start()
        time.sleep(2)
        print(f"PostgreSQL {p.execute('SHOW server_version').fetchone()[0]}, primaire et réplique asynchrone r1")
        print(f"retard au repos : {1000 * retard(r):.0f} ms (battement toutes les 20 ms)")

        print("\n1. une transaction qui modifie un million de lignes (125 Mo de table)")
        avant, = p.execute("SELECT pg_current_wal_lsn()").fetchone()
        t = time.perf_counter()
        p.execute("UPDATE grosse SET v = v + 1")
        d = time.perf_counter() - t
        lsn, octets = p.execute("SELECT pg_current_wal_lsn(), pg_wal_lsn_diff(pg_current_wal_lsn(), %s)",
                                (avant,)).fetchone()
        t = time.perf_counter()
        while r.execute("SELECT v FROM grosse WHERE id = %s", (LIGNES - 1,)).fetchone()[0] != 1:
            time.sleep(0.01)
        vu = time.perf_counter() - t
        print(f"   durée sur le primaire {d:.1f} s, journal produit {octets / 2**20:.0f} Mo")
        print(f"   visible sur la réplique {1000 * vu:.0f} ms après la confirmation")

        print("\n2. lire ses propres écritures, 500 essais, liaison à 5 ms par sens")
        relais.delai = 0.005
        time.sleep(1)
        perimees, attentes = 0, []
        for i in range(500):
            p.execute("UPDATE comptes SET solde = %s WHERE id = 7", (i + 1,))
            if r.execute("SELECT solde FROM comptes WHERE id = 7").fetchone()[0] != i + 1:
                perimees += 1
        print(f"   lecture aussitôt sur la réplique : {perimees} lectures périmées sur 500")
        perimees = 0
        for i in range(500, 1000):
            p.execute("UPDATE comptes SET solde = %s WHERE id = 7", (i + 1,))
            lsn, = p.execute("SELECT pg_current_wal_lsn()").fetchone()
            t = time.perf_counter()
            while not r.execute("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,)).fetchone()[0]:
                time.sleep(0.001)
            attentes.append(1000 * (time.perf_counter() - t))
            if r.execute("SELECT solde FROM comptes WHERE id = 7").fetchone()[0] != i + 1:
                perimees += 1
        med, p99 = commun.centiles(attentes)
        print(f"   en attendant le LSN de l'écriture : {perimees} lecture périmée,")
        print(f"   attente médiane {med:.1f} ms, p99 {p99:.1f} ms")
        relais.delai = 0.0
        time.sleep(1)

        print("\n3. requête de 60 s sur la réplique, UPDATE et VACUUM sur le primaire")
        print("   a) réglages par défaut (max_standby_streaming_delay = 30 s)")
        conflit(p, r)
        print("   b) hot_standby_feedback = on sur la réplique")
        commun.regler(r, "hot_standby_feedback", "on")
        time.sleep(1)
        conflit(p, r)
    finally:
        arret.set()
        commun.supprimer()


if __name__ == "__main__":
    main()
