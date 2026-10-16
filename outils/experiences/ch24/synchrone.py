"""Chapitre 24 : ce que coûte d'attendre une réplique avant de confirmer.

Un primaire PostgreSQL 18 et deux répliques (r1, r2) reliées par des relais à
délai (voir commun.py). Quatre mesures :
  1. latence d'une validation (UPDATE d'une ligne, validé seul), médiane et 99e
     centile sur 300, pour chaque valeur de synchronous_commit, avec r1 déclarée
     synchrone, et pour une liaison retardée de 0, 0,5 et 25 ms dans chaque sens
     (aller-retour d'environ 0, 1 et 50 ms) ; « asynchrone » = aucune réplique
     synchrone déclarée ;
  2. quorum : r1 à 25 ms, r2 à 0,5 ms ; FIRST 1 (r1, r2), ANY 1 (r1, r2), ANY 2 ;
  3. débit avec 32 connexions pendant 10 s, asynchrone et synchrone (on) ;
  4. la seule réplique synchrone tombe : que devient une validation, et que
     fait un délai d'attente (statement_timeout) ou une annulation ?
Prérequis : docker, image postgres:18-alpine, pip install -r requirements.txt.
Usage : python3 synchrone.py
"""
import asyncio
import random
import textwrap
import threading
import time

import psycopg

import commun

MODES = ["off", "local", "remote_write", "on", "remote_apply"]
LIGNES = 10_000


def valider(cx, mode, n=300):
    cx.execute(f"SET synchronous_commit = {mode}")
    alea, durees = random.Random(24), []
    for _ in range(n):
        t = time.perf_counter()
        cx.execute("UPDATE comptes SET solde = solde + 1 WHERE id = %s", (alea.randrange(LIGNES),))
        durees.append(1000 * (time.perf_counter() - t))
    return commun.centiles(durees)


async def debit(mode, duree=10, connexions=32):
    fin, n = time.monotonic() + duree, 0

    async def client(k):
        nonlocal n
        alea = random.Random(k)
        async with await psycopg.AsyncConnection.connect(commun.dsn(commun.PRIMAIRE), autocommit=True) as cx:
            await cx.execute(f"SET synchronous_commit = {mode}")
            while time.monotonic() < fin:
                await cx.execute("UPDATE comptes SET solde = solde + 1 WHERE id = %s", (alea.randrange(LIGNES),))
                n += 1

    await asyncio.gather(*(client(k) for k in range(connexions)))
    return n / duree


def main():
    commun.supprimer()
    relais = {nom: commun.Relais(r, commun.PRIMAIRE) for nom, (_, r) in commun.REPLIQUES.items()}
    try:
        p = commun.lancer_primaire()
        p.execute("CREATE TABLE comptes (id int PRIMARY KEY, solde int NOT NULL DEFAULT 0)")
        p.execute(f"INSERT INTO comptes (id) SELECT generate_series(0, {LIGNES - 1})")
        for nom in commun.REPLIQUES:
            commun.lancer_replique(nom)
        commun.attendre_flux(p, "r1", "r2")
        version = p.execute("SHOW server_version").fetchone()[0]
        print(f"PostgreSQL {version}, un primaire et deux répliques (r1, r2)")

        print("\n1. une validation, médiane et 99e centile en ms, r1 synchrone")
        print(f"   {'aller-retour':<14}" + "".join(f"{d:>15}" for d in ("≈ 0 ms", "≈ 1 ms", "≈ 50 ms")))
        resultats = {}
        for delai in (0.0, 0.0005, 0.025):
            for r in relais.values():
                r.delai = delai
            time.sleep(1)
            commun.regler(p, "synchronous_standby_names", "")
            resultats[("asynchrone", delai)] = valider(p, "on")
            commun.regler(p, "synchronous_standby_names", "r1")
            for mode in MODES:
                resultats[(mode, delai)] = valider(p, mode)
        for mode in ["asynchrone", *MODES]:
            print(f"   {mode:<14}" + "".join(f"{resultats[(mode, d)][0]:>8.2f}{resultats[(mode, d)][1]:>7.1f}"
                                          for d in (0.0, 0.0005, 0.025)))

        print("\n2. quorum : r1 à 25 ms, r2 à 0,5 ms dans chaque sens, mode on")
        relais["r1"].delai, relais["r2"].delai = 0.025, 0.0005
        time.sleep(1)
        for regle in ("FIRST 1 (r1, r2)", "ANY 1 (r1, r2)", "ANY 2 (r1, r2)"):
            commun.regler(p, "synchronous_standby_names", regle)
            med, p99 = valider(p, "on", 200)
            print(f"   {regle:<20}médiane {med:6.2f} ms   p99 {p99:6.1f} ms")

        print("\n3. débit avec 32 connexions pendant 10 s, validations par seconde")
        commun.regler(p, "synchronous_standby_names", "r1")
        for delai in (0.0005, 0.025):
            for r in relais.values():
                r.delai = delai
            time.sleep(1)
            asyn = asyncio.run(debit("local"))
            syn = asyncio.run(debit("on"))
            print(f"   ≈ {2000 * delai:>2.0f} ms : sans attendre r1 {commun.nombre(asyn):>6} /s,"
                  f" en attendant r1 {commun.nombre(syn):>6} /s")

        print("\n4. la seule réplique synchrone (r1) tombe (docker kill)")
        for r in relais.values():
            r.delai = 0.0005
        commun.regler(p, "synchronous_standby_names", "r1")
        commun.docker("kill", "cours-sd-ch24-r1")
        attente = {}

        def bloquee():
            with commun.connecter(commun.PRIMAIRE) as c:
                t = time.perf_counter()
                c.execute("UPDATE comptes SET solde = solde + 1 WHERE id = 1")
                attente["a"] = time.perf_counter() - t

        fil = threading.Thread(target=bloquee)
        fil.start()
        time.sleep(3)
        n, = p.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'").fetchone()
        print(f"   après 3 s, sessions en attente de r1 (SyncRep) : {n}")
        avis, rendu = [], {}

        def avec_delai():
            with commun.connecter(commun.PRIMAIRE) as c2:
                c2.add_notice_handler(lambda d: avis.extend([d.message_primary, d.message_detail]))
                c2.execute("SET statement_timeout = '2s'")
                rendu["avant"], = c2.execute("SELECT solde FROM comptes WHERE id = 2").fetchone()
                t = time.perf_counter()
                c2.execute("UPDATE comptes SET solde = solde + 1 WHERE id = 2")
                rendu["duree"] = time.perf_counter() - t

        fil2 = threading.Thread(target=avec_delai)
        fil2.start()
        time.sleep(6)
        print(f"   UPDATE avec statement_timeout = 2 s : "
              f"{'toujours en attente après 6 s' if fil2.is_alive() else 'rendu'}")
        pid, = p.execute("SELECT pid FROM pg_stat_activity WHERE wait_event = 'SyncRep'"
                         " AND query LIKE '%%id = 2'").fetchone()
        p.execute("SELECT pg_cancel_backend(%s)", (pid,))
        fil2.join(5)
        print(f"   annulé par pg_cancel_backend : rendu sans erreur après {rendu['duree']:.1f} s,")
        print("   avec cet avertissement :")
        for a in filter(None, avis):
            print(textwrap.fill(a, 66, initial_indent=" " * 6, subsequent_indent=" " * 6))
        apres, = p.execute("SELECT solde FROM comptes WHERE id = 2").fetchone()
        print(f"   solde de la ligne 2 lu sur le primaire : {rendu['avant']} puis {apres}")
        commun.regler(p, "synchronous_standby_names", "ANY 1 (r1, r2)")
        fil.join(10)
        print(f"   passage à ANY 1 (r1, r2) : session en attente libérée après"
              f" {attente.get('a', float('nan')):.1f} s")
    finally:
        commun.supprimer()


if __name__ == "__main__":
    main()
