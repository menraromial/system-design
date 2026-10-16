"""Chapitre 24 : une bascule qui perd des écritures confirmées, puis un cerveau divisé.

Un primaire PostgreSQL 18 et une réplique r1, reliée par un relais qui retarde
le journal de 5 ms dans chaque sens (voir commun.py). Huit clients insèrent des
lignes numérotées et notent chaque insertion confirmée par le primaire.
  1. réplication asynchrone : au bout de 3 s, la liaison entre le primaire et r1
     est coupée (le primaire est isolé de sa réplique mais reste joignable par les
     clients) ; 2 s plus tard, le primaire est tué (docker kill), la liaison
     rompue avec ce qu'elle retenait, et r1 promue (pg_promote). Combien
     d'écritures confirmées manquent sur r1 ?
  2. cerveau divisé : l'ancien primaire redémarre ; il se croit toujours
     primaire, et un client resté configuré sur lui y écrit 100 lignes, pendant
     que d'autres en écrivent 100 sur r1. Puis l'ancien primaire est rattaché à
     r1 comme réplique avec pg_rewind : que deviennent ses lignes ?
  3. la même bascule qu'en 1, avec r1 déclarée synchrone (synchronous_commit = on).
Prérequis : docker, image postgres:18-alpine, pip install -r requirements.txt.
Usage : python3 bascule.py
"""
import threading
import time

import psycopg

import commun

CLIENTS, AVANT, ISOLE = 8, 3, 2


def ecrire(port, confirmees, arret, client, debut=0):
    """Insère (client, n) en boucle ; note chaque insertion confirmée."""
    try:
        with commun.connecter(port, delai=5) as c:
            n = debut
            while not arret.is_set():
                c.execute("INSERT INTO ecritures (client, n) VALUES (%s, %s)", (client, n))
                confirmees.add((client, n))
                n += 1
    except psycopg.Error:
        pass


def lignes(port):
    with commun.connecter(port) as c:
        return set(c.execute("SELECT client, n FROM ecritures").fetchall())


def preparer(synchrone):
    commun.supprimer()
    p = commun.lancer_primaire()
    p.execute("CREATE TABLE ecritures (client int, n int, PRIMARY KEY (client, n))")
    commun.lancer_replique("r1")
    commun.attendre_flux(p, "r1")
    if synchrone:
        commun.regler(p, "synchronous_standby_names", "r1")
    return p


def bascule(p, relais):
    """Écrit, isole le primaire de r1, le tue, promeut r1 ; renvoie les confirmées et les perdues."""
    confirmees, arret = set(), threading.Event()
    fils = [threading.Thread(target=ecrire, args=(commun.PRIMAIRE, confirmees, arret, k))
            for k in range(CLIENTS)]
    for f in fils:
        f.start()
    time.sleep(AVANT)
    avant = len(confirmees)
    relais.couper()
    time.sleep(ISOLE)
    pendant = len(confirmees) - avant
    commun.docker("kill", "cours-sd-ch24-primaire")
    relais.rompre()
    arret.set()
    for f in fils:
        f.join()
    t = time.perf_counter()
    with commun.connecter(commun.REPLIQUES["r1"][0]) as r:
        r.execute("SELECT pg_promote(wait => true)")
    promotion = time.perf_counter() - t
    sur_r1 = lignes(commun.REPLIQUES["r1"][0])
    perdues = confirmees - sur_r1
    print(f"   insertions confirmées : {commun.nombre(avant)} en {AVANT} s avant la coupure,")
    print(f"   {commun.nombre(pendant)} pendant les {ISOLE} s d'isolement")
    print(f"   r1 promue en {1000 * promotion:.0f} ms ; confirmées absentes de r1 : {len(perdues)}")
    return confirmees, perdues


def main():
    relais = commun.Relais(commun.REPLIQUES["r1"][1], commun.PRIMAIRE, delai=0.005)
    try:
        p = preparer(synchrone=False)
        print(f"PostgreSQL {p.execute('SHOW server_version').fetchone()[0]}, primaire et réplique r1,"
              f" journal retardé de 5 ms")
        print("\n1. asynchrone : primaire isolé de r1 pendant 2 s, puis tué ; r1 promue")
        confirmees, perdues = bascule(p, relais)

        print("\n2. cerveau divisé : l'ancien primaire redémarre")
        commun.docker("start", "cours-sd-ch24-primaire")
        with commun.connecter(commun.PRIMAIRE) as ancien:
            role, = ancien.execute("SELECT CASE WHEN pg_is_in_recovery() THEN 'réplique' ELSE 'primaire' END")\
                .fetchone()
            print(f"   l'ancien primaire répond, et se déclare {role}")
        print(f"   les {len(perdues)} lignes perdues sont sur lui : "
              f"{len(perdues & lignes(commun.PRIMAIRE))}")
        for port, client in ((commun.PRIMAIRE, 100), (commun.REPLIQUES["r1"][0], 200)):
            with commun.connecter(port) as c:
                for n in range(100):
                    c.execute("INSERT INTO ecritures (client, n) VALUES (%s, %s)", (client, n))
        ancien, nouveau = lignes(commun.PRIMAIRE), lignes(commun.REPLIQUES["r1"][0])
        print("   100 insertions acceptées par chacun ; lignes présentes")
        print(f"   seulement sur l'ancien : {len(ancien - nouveau)}, seulement sur r1 : {len(nouveau - ancien)}")
        commun.docker("stop", "cours-sd-ch24-primaire")
        t = time.perf_counter()
        r = commun.docker("run", "--rm", "--network", "host", "--user", "postgres",
                          "--volumes-from", "cours-sd-ch24-primaire", commun.IMAGE,
                          "pg_rewind", "-D", commun.PGDATA, "-R",
                          f"--source-server={commun.dsn(commun.REPLIQUES['r1'][0])}", check=False)
        fin = [l for l in (r.stdout + r.stderr).splitlines() if l.strip()][-1]
        print(f"   pg_rewind en {time.perf_counter() - t:.1f} s : « {fin.strip()} »")
        commun.docker("start", "cours-sd-ch24-primaire")
        with commun.connecter(commun.PRIMAIRE) as ancien:
            role, = ancien.execute("SELECT CASE WHEN pg_is_in_recovery() THEN 'réplique' ELSE 'primaire' END")\
                .fetchone()
        time.sleep(2)
        ancien, nouveau = lignes(commun.PRIMAIRE), lignes(commun.REPLIQUES["r1"][0])
        print(f"   rattaché à r1, il se déclare {role} ; lignes seulement sur lui : {len(ancien - nouveau)}")
        print(f"   écritures confirmées puis disparues : {len(perdues)} pendant la bascule,")
        print("   100 pendant le cerveau divisé")

        print("\n3. la même bascule avec r1 synchrone (synchronous_commit = on)")
        relais.retablir()
        p = preparer(synchrone=True)
        bascule(p, relais)
    finally:
        commun.supprimer()


if __name__ == "__main__":
    main()
