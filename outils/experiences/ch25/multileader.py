"""Chapitre 25 : deux leaders PostgreSQL qui se répliquent l'un l'autre.

Deux PostgreSQL 18 (cours-sd-ch25-pga, port 10532, et cours-sd-ch25-pgb, port
10533, en réseau hôte) ont la même table comptes. Chacun publie la table et
s'abonne à la publication de l'autre par réplication logique, avec
origin = none pour ne pas renvoyer ce qu'il a reçu : les deux acceptent des
écritures, et chacun applique celles de l'autre.
  1. délai de réplication : une écriture sur A, combien de temps avant de la
     voir sur B (médiane sur 200) ;
  2. 100 lignes modifiées en même temps sur A (valeur « A ») et sur B (« B ») :
     que contient chaque base ensuite ? quels conflits PostgreSQL a-t-il notés
     (pg_stat_subscription_stats) ?
  3. 50 fois, A ajoute 10 et B ajoute 20 au même solde, en même temps ;
  4. la même clé insérée sur A et sur B : que devient la réplication des
     écritures suivantes ? puis on supprime la ligne en conflit sur chaque base.
Prérequis : docker, image postgres:18-alpine, pip install -r requirements.txt.
Usage : python3 multileader.py
"""
import statistics
import subprocess
import threading
import time

import psycopg

IMAGE = "postgres:18-alpine"
PORTS = {"A": 10532, "B": 10533}


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def nettoyer():
    subprocess.run(["docker", "rm", "-f", "-v", "cours-sd-ch25-pga", "cours-sd-ch25-pgb"], capture_output=True)


def dsn(nom):
    return f"host=127.0.0.1 port={PORTS[nom]} dbname=postgres user=postgres"


def connecter(nom, delai=60):
    fin = time.monotonic() + delai
    while True:
        try:
            return psycopg.connect(dsn(nom), autocommit=True)
        except psycopg.OperationalError:
            if time.monotonic() > fin:
                raise
            time.sleep(0.5)


def lancer(nom):
    docker("run", "-d", "--name", f"cours-sd-ch25-pg{nom.lower()}", "--network", "host",
           "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGE,
           "-c", f"port={PORTS[nom]}", "-c", "listen_addresses=127.0.0.1", "-c", "wal_level=logical",
           "-c", "track_commit_timestamp=on")
    return connecter(nom)


def attendre(cx, requete, args, attendu, delai=10):
    t = time.perf_counter()
    while time.perf_counter() - t < delai:
        if cx.execute(requete, args).fetchone() == attendu:
            return time.perf_counter() - t
        time.sleep(0.001)
    return None


def conflits(cx):
    ligne = cx.execute("SELECT * FROM pg_stat_subscription_stats").fetchone()
    noms = [d.name for d in cx.execute("SELECT * FROM pg_stat_subscription_stats").description]
    return {n: v for n, v in zip(noms, ligne) if (n.startswith("confl_") or n.endswith("error_count")) and v}


def en_meme_temps(fa, fb):
    """Lance fa et fb dans deux fils qui partent ensemble."""
    barriere = threading.Barrier(2)
    fils = [threading.Thread(target=lambda f=f: (barriere.wait(), f())) for f in (fa, fb)]
    for f in fils:
        f.start()
    for f in fils:
        f.join()


def main():
    nettoyer()
    try:
        cx = {n: lancer(n) for n in PORTS}
        for c in cx.values():
            c.execute("CREATE TABLE comptes (id int PRIMARY KEY, solde int NOT NULL, ecrit_par text)")
            c.execute("INSERT INTO comptes SELECT i, 0, 'init' FROM generate_series(0, 299) i")
        cx["A"].execute("CREATE PUBLICATION pa FOR TABLE comptes")
        cx["B"].execute("CREATE PUBLICATION pb FOR TABLE comptes")
        for moi, autre in (("A", "B"), ("B", "A")):
            cx[moi].execute(f"CREATE SUBSCRIPTION s{moi.lower()} CONNECTION '{dsn(autre)}'"
                            f" PUBLICATION p{autre.lower()} WITH (copy_data = false, origin = none)")
        time.sleep(3)
        version = cx["A"].execute("SHOW server_version").fetchone()[0]
        print(f"PostgreSQL {version}, deux leaders A et B abonnés l'un à l'autre (origin = none)")

        print("\n1. une écriture sur A, visible sur B après (200 essais)")
        durees = []
        for k in range(200):
            cx["A"].execute("UPDATE comptes SET solde = %s WHERE id = 299", (k + 1,))
            d = attendre(cx["B"], "SELECT solde FROM comptes WHERE id = 299", (), (k + 1,))
            durees.append(1000 * d)
        durees.sort()
        print(f"   médiane {statistics.median(durees):.1f} ms, maximum {durees[-1]:.1f} ms")

        print("\n2. 100 lignes modifiées en même temps sur A et sur B")
        avant = {n: conflits(c) for n, c in cx.items()}
        for k in range(100):
            en_meme_temps(
                lambda: cx["A"].execute("UPDATE comptes SET solde = 1, ecrit_par = 'A' WHERE id = %s", (k,)),
                lambda: cx["B"].execute("UPDATE comptes SET solde = 2, ecrit_par = 'B' WHERE id = %s", (k,)))
        time.sleep(3)
        lignes = {n: dict(c.execute("SELECT id, ecrit_par FROM comptes WHERE id < 100").fetchall())
                  for n, c in cx.items()}
        egales = sum(lignes["A"][k] == lignes["B"][k] for k in range(100))
        croisees = sum(lignes["A"][k] == "B" and lignes["B"][k] == "A" for k in range(100))
        print(f"   lignes identiques sur A et B : {egales}")
        print(f"   lignes croisées (A a « B », B a « A ») : {croisees}")
        for n, c in cx.items():
            apres = conflits(c)
            diff = {k: v - avant[n].get(k, 0) for k, v in apres.items() if v - avant[n].get(k, 0)}
            print(f"   conflits notés par {n} : {diff or 'aucun'}")

        print("\n3. 50 fois, en même temps, A ajoute 10 et B ajoute 20 au même solde")
        for _ in range(50):
            en_meme_temps(lambda: cx["A"].execute("UPDATE comptes SET solde = solde + 10 WHERE id = 200"),
                          lambda: cx["B"].execute("UPDATE comptes SET solde = solde + 20 WHERE id = 200"))
            time.sleep(0.05)
        time.sleep(2)
        sa, = cx["A"].execute("SELECT solde FROM comptes WHERE id = 200").fetchone()
        sb, = cx["B"].execute("SELECT solde FROM comptes WHERE id = 200").fetchone()
        print(f"   attendu 1 500 ; A a {sa}, B a {sb}")

        print("\n4. la clé 1000 insérée sur A et sur B en même temps")
        en_meme_temps(lambda: cx["A"].execute("INSERT INTO comptes VALUES (1000, 1, 'A')"),
                      lambda: cx["B"].execute("INSERT INTO comptes VALUES (1000, 2, 'B')"))
        time.sleep(1)
        cx["A"].execute("INSERT INTO comptes VALUES (2000, 0, 'A')")
        d = attendre(cx["B"], "SELECT count(*) FROM comptes WHERE id = 2000", (), (1,), delai=15)
        print(f"   la clé 2000, insérée ensuite sur A seul, arrive sur B :"
              f" {'après %.0f ms' % (1000 * d) if d is not None else 'non (15 s)'}")
        for n, c in cx.items():
            print(f"   {n} : " + ", ".join(f"{k} {v}" for k, v in conflits(c).items()
                                        if k in ("apply_error_count", "confl_insert_exists")))
        journal = docker("logs", "cours-sd-ch25-pgb", check=False).stderr
        erreur = next((l for l in journal.splitlines() if "conflict=insert_exists" in l), "")
        message = erreur.split("ERROR:")[-1].split("LOG:")[-1].strip()
        print("   journal de B :")
        for morceau in message.split(": "):
            print(f"      {morceau}")
        for c in cx.values():
            c.execute("DELETE FROM comptes WHERE id = 1000")
        d = attendre(cx["B"], "SELECT count(*) FROM comptes WHERE id = 2000", (), (1,), delai=30)
        print("   ligne 1000 supprimée à la main sur A et sur B :")
        print(f"   la clé 2000 arrive sur B {'%.1f s plus tard' % d if d is not None else 'jamais (30 s)'}")
        time.sleep(3)
        va = cx["A"].execute("SELECT ecrit_par FROM comptes WHERE id = 1000").fetchone()
        vb = cx["B"].execute("SELECT ecrit_par FROM comptes WHERE id = 1000").fetchone()
        print(f"   ligne 1000 ensuite : A a {va[0] if va else 'rien'}, B a {vb[0] if vb else 'rien'}")
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
