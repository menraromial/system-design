"""Chapitre 46 : trois régions simulées sur un poste, avec les délais publiés par
Azure entre France Central, East US et Southeast Asia (médianes P50 sur 30 jours).

Chaque région est un conteneur (image cours-sd-ch46-region, construite par le
Dockerfile du dossier) qui porte un espace réseau ; le serveur PostgreSQL 18 de
la région tourne dans cet espace (--network container:...), et le client
(client.py) y est lancé par docker exec. tc et netem retardent les paquets qui
sortent vers chacune des autres régions de la moitié de l'aller-retour publié.

  1. allers-retours mesurés entre régions, requête et nouvelle connexion ;
  2. actif-passif, primaire à Paris : écriture locale, synchrone vers une ou
     deux répliques distantes, écriture depuis Singapour, lecture locale ;
  3. retard de réplication vers la Virginie et Singapour ;
  4. perte de la région primaire : transactions acquittées perdues en
     réplication asynchrone et synchrone, durée de la promotion ;
  5. actif-actif entre Paris et la Virginie (réplication logique dans les deux
     sens) : lignes divergentes, toutes les clés écrites partout ou chaque clé
     dans sa région d'attache ;
  6. volume envoyé sur le réseau par la réplication, et son prix.

    docker build -t cours-sd-ch46-region .
    python3 regions.py           (environ 8 minutes)
"""
import json
import statistics
import subprocess
import time
from pathlib import Path

import psycopg

ICI = Path(__file__).resolve().parent
IMAGE_PG = "postgres:18-alpine"
RESEAU = "cours-sd-ch46-reseau"
IP = {"paris": "172.30.46.10", "virginie": "172.30.46.20", "singapour": "172.30.46.30"}
# allers-retours P50 publiés par Azure (France Central, East US, Southeast Asia)
RTT = {frozenset(("paris", "virginie")): 88, frozenset(("paris", "singapour")): 162,
       frozenset(("virginie", "singapour")): 224}
# prix AWS du transfert entre régions, par Go, selon la région d'origine
PRIX_INTER = {"paris": 0.02, "virginie": 0.02, "singapour": 0.09}


def nb(x, d=0):
    """Nombre avec espace pour les milliers (point décimal)."""
    return f"{x:,.{d}f}".replace(",", " ")


def docker(*a, check=True):
    r = subprocess.run(["docker", *a], capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(f"docker {' '.join(a[:3])} : {r.stderr.strip()[:300]}")
    return r


def region(nom):
    return f"cours-sd-ch46-{nom}"


def client(nom, *args):
    r = docker("exec", region(nom), "python3", "/code/client.py", *map(str, args))
    return json.loads(r.stdout.strip().splitlines()[-1])


def client_fond(nom, *args):
    return subprocess.Popen(["docker", "exec", region(nom), "python3", "/code/client.py", *map(str, args)],
                            stdout=subprocess.PIPE, text=True)


def admin(nom, port=5432, delai=180):
    fin = time.monotonic() + delai
    while True:
        try:
            return psycopg.connect(f"host={IP[nom]} port={port} user=postgres connect_timeout=3",
                                   autocommit=True)
        except psycopg.OperationalError:
            if time.monotonic() > fin:
                raise
            time.sleep(0.3)


# ---- les régions et leurs délais -------------------------------------------------------
def nettoyer():
    noms = docker("ps", "-a", "--format", "{{.Names}}").stdout.split()
    for n in noms:
        if n.startswith("cours-sd-ch46-pg"):
            docker("rm", "-f", "-v", n, check=False)
    for n in IP:
        docker("rm", "-f", "-v", region(n), check=False)
    docker("network", "rm", RESEAU, check=False)


def preparer():
    nettoyer()
    docker("network", "create", "--subnet", "172.30.46.0/24", RESEAU)
    for n, ip in IP.items():
        docker("run", "-d", "--name", region(n), "--network", RESEAU, "--ip", ip, "--cap-add", "NET_ADMIN",
               "-v", f"{ICI}:/code:ro", "cours-sd-ch46-region", "sleep", "infinity")
    delais(1.0)


def delais(facteur):
    """Retarde les paquets vers chaque autre région de facteur × aller-retour / 2."""
    for n in IP:
        docker("exec", region(n), "tc", "qdisc", "del", "dev", "eth0", "root", check=False)
        if facteur == 0:
            continue
        docker("exec", region(n), "tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio",
               "bands", "4", "priomap", *["0"] * 16)
        for bande, autre in enumerate([a for a in IP if a != n], start=2):
            ms = facteur * RTT[frozenset((n, autre))] / 2
            docker("exec", region(n), "tc", "qdisc", "add", "dev", "eth0", "parent", f"1:{bande}",
                   "handle", f"{bande}0:", "netem", "delay", f"{ms}ms", "limit", "100000")
            docker("exec", region(n), "tc", "filter", "add", "dev", "eth0", "parent", "1:", "protocol",
                   "ip", "prio", "1", "u32", "match", "ip", "dst", f"{IP[autre]}/32", "flowid", f"1:{bande}")


# ---- PostgreSQL --------------------------------------------------------------------------
def pg_nom(n, port):
    return f"cours-sd-ch46-pg-{n}" + ("" if port == 5432 else f"-{port}")


def pg_primaire(n, port=5432, *options):
    docker("run", "-d", "--name", pg_nom(n, port), "--network", f"container:{region(n)}",
           "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGE_PG, "-c", f"port={port}",
           "-c", "listen_addresses=*", "-c", "shared_buffers=256MB", "-c", "wal_keep_size=1GB", *options)
    c = admin(n, port)
    docker("exec", pg_nom(n, port), "sh", "-c",
           'echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"')
    c.execute("select pg_reload_conf()")
    return c


def pg_replique(n, source):
    copie = (f"pg_basebackup -d 'host={IP[source]} port=5432 user=postgres application_name={n}'"
             " -D /tmp/donnees -R -X stream -c fast && chmod 700 /tmp/donnees"
             " && exec postgres -D /tmp/donnees -c listen_addresses=* -c port=5432")
    docker("run", "-d", "--name", pg_nom(n, 5432), "--network", f"container:{region(n)}",
           "--user", "postgres", "--entrypoint", "sh", IMAGE_PG, "-c", copie)
    return admin(n)


def topologie():
    """Primaire à Paris, répliques physiques en Virginie et à Singapour."""
    for n in IP:
        docker("rm", "-f", "-v", pg_nom(n, 5432), check=False)
    p = pg_primaire("paris")
    p.execute("create table journal (id bigserial primary key, ts double precision, charge text)")
    reps = {n: pg_replique(n, "paris") for n in ("virginie", "singapour")}
    fin = time.monotonic() + 120
    while time.monotonic() < fin:
        etats = dict(p.execute("select application_name, state from pg_stat_replication").fetchall())
        if all(etats.get(n) == "streaming" for n in reps):
            break
        time.sleep(0.3)
    return p, reps


def synchrones(p, valeur):
    p.execute(f"alter system set synchronous_standby_names = '{valeur}'")
    p.execute("select pg_reload_conf()")
    time.sleep(0.5)


# ---- 1. allers-retours ----------------------------------------------------------------------
def allers_retours():
    print("1. aller-retour mesuré entre régions (médiane, ms) ; délais réglés sur")
    print("   les médianes publiées par Azure : 88, 162 et 224 ms")
    p, reps = topologie()
    res = {(c, s): client(c, "latence", IP[s], 5432) for c in IP for s in IP}
    for titre, cle in (("SELECT 1 sur une connexion gardée", "requete"),
                       ("nouvelle connexion (TCP, démarrage, SELECT 1)", "connexion")):
        print(f"   {titre}")
        print(f"   {'client':<12}{'vers paris':>12}{'vers virginie':>15}{'vers singapour':>16}")
        for c in IP:
            v = [res[(c, s)][cle]["med"] for s in IP]
            print(f"   {c:<12}{v[0]:>12.1f}{v[1]:>15.1f}{v[2]:>16.1f}")


# ---- 2. actif-passif -------------------------------------------------------------------------
def ecritures():
    print("\n2. actif-passif, primaire à Paris : 100 opérations successives (ms)")
    print(f"   {'cas':<44}{'médiane':>9}{'99e':>8}")
    p, reps = topologie()
    cas = [("écriture à Paris, réplication asynchrone", "paris", "", "local"),
           ("écriture à Paris, synchrone vers la Virginie", "paris", "virginie", "on"),
           ("écriture à Paris, synchrone ANY 1 des deux", "paris", "ANY 1 (virginie, singapour)", "on"),
           ("écriture à Paris, synchrone ANY 2 des deux", "paris", "ANY 2 (virginie, singapour)", "on"),
           ("écriture depuis Singapour vers Paris", "singapour", "", "local")]
    for libelle, c, sync, commit in cas:
        synchrones(p, sync)
        r = client(c, "ecrire", IP["paris"], 5432, 100, 1, commit)
        print(f"   {libelle:<44}{r['lat']['med']:>9.1f}{r['lat']['p99']:>8.1f}")
    synchrones(p, "")
    r = client("singapour", "lire", IP["singapour"], 5432, 100)
    print(f"   {'lecture à Singapour sur la réplique locale':<44}{r['lat']['med']:>9.1f}{r['lat']['p99']:>8.1f}")


# ---- 3. retard de réplication -----------------------------------------------------------------
def retard():
    print("\n3. retard de réplication : 500 écritures à Paris, vues dans chaque")
    print("   réplique (ms, de l'acquittement à Paris à la lecture dans la réplique)")
    p, reps = topologie()
    obs = {n: client_fond(n, "observer", IP[n], 5432, 500) for n in ("virginie", "singapour")}
    for o in obs.values():
        o.stdout.readline()                       # {"pret": true}
    client("paris", "ecrire", IP["paris"], 5432, 500, 1, "local")
    print(f"   {'réplique':<12}{'médiane':>9}{'99e':>8}{'demi aller-retour':>19}")
    for n, o in obs.items():
        r = json.loads(o.stdout.readline())
        o.wait()
        print(f"   {n:<12}{r['retard']['med']:>9.1f}{r['retard']['p99']:>8.1f}"
              f"{RTT[frozenset(('paris', n))] / 2:>19.0f}")


# ---- 4. perte de la région primaire --------------------------------------------------------------
def bascule():
    print("\n4. perte de la région de Paris après 3 s d'écritures (8 fils) : réseau")
    print("   coupé (paquets en route perdus), primaire tué, Virginie promue")
    print(f"   {'réplication':<26}{'acquittées':>11}{'tx/s':>7}{'perdues':>9}{'promotion':>11}")
    for libelle, sync, commit in (("asynchrone", "", "local"), ("synchrone vers Virginie", "virginie", "on")):
        p, reps = topologie()
        synchrones(p, sync)
        p.close()
        for c in reps.values():
            c.close()
        ecrivain = client_fond("paris", "ecrire", IP["paris"], 5432, 0, 8, commit, 6)
        time.sleep(3.2)                     # environ 3 s d'écritures après le démarrage du client
        docker("exec", region("paris"), "tc", "qdisc", "replace", "dev", "eth0", "root", "netem",
               "loss", "100%")
        docker("kill", pg_nom("paris", 5432))
        r = json.loads(ecrivain.stdout.read().strip().splitlines()[-1])
        v = admin("virginie")
        t = time.perf_counter()
        v.execute("select pg_promote(true, 60)")
        promotion = (time.perf_counter() - t) * 1000
        presents = v.execute("select count(*) from journal where id = any(%s)", (r["ids"],)).fetchone()[0]
        v.close()
        print(f"   {libelle:<26}{r['acquis']:>11}{r['debit']:>7.0f}{r['acquis'] - presents:>9}"
              f"{promotion:>9.0f} ms")
        delais(1.0)


# ---- 5. actif-actif ------------------------------------------------------------------------------
def actif_actif():
    print("\n5. actif-actif Paris et Virginie, réplication logique dans les deux sens ;")
    print("   100 lignes, 100 mises à jour par seconde dans chaque région, 10 s ;")
    print("   lignes qui diffèrent entre les deux régions une fois la réplication finie")
    print(f"   {'clés écrites':<26}{'aller-retour':>13}{'divergentes':>13}")
    for mode, facteur in (("tous", 1.0), ("tous", 0.0), ("attache", 1.0)):
        delais(facteur)
        cx = {}
        for n in ("paris", "virginie"):
            docker("rm", "-f", "-v", pg_nom(n, 5433), check=False)
            cx[n] = pg_primaire(n, 5433, "-c", "wal_level=logical", "-c", "track_commit_timestamp=on")
            cx[n].execute("create table profils (id int primary key, valeur bigint, region text)")
            cx[n].execute("insert into profils select g, 0, '-' from generate_series(0, 99) g")
            cx[n].execute(f"create publication pub_{n} for table profils")
        for n, autre in (("paris", "virginie"), ("virginie", "paris")):
            cx[n].execute(f"create subscription sub_{n} connection 'host={IP[autre]} port=5433 user=postgres'"
                          f" publication pub_{autre} with (origin = none, copy_data = false)")
        for n in cx:
            while not cx[n].execute("select bool_and(pid is not null) from pg_stat_subscription"
                                    " where subname like 'sub_%'").fetchone()[0]:
                time.sleep(0.2)
        travaux = [client_fond(n, "maj", IP[n], 5433, 10, 100, mode, k, 100)
                   for k, n in enumerate(("paris", "virginie"))]
        for t in travaux:
            t.wait()
        time.sleep(3)                         # laisser la réplication finir
        lignes = {n: dict((i, (v, r)) for i, v, r in cx[n].execute("select id, valeur, region from profils"))
                  for n in cx}
        divergentes = sum(1 for i in range(100) if lignes["paris"][i] != lignes["virginie"][i])
        cle = "toutes partout" if mode == "tous" else "région d'attache"
        rtt = f"{RTT[frozenset(('paris', 'virginie'))] * facteur:.0f} ms"
        print(f"   {cle:<26}{rtt:>13}{divergentes:>13}")
        for n in cx:
            cx[n].execute(f"drop subscription sub_{n}")
            cx[n].close()
        for n in ("paris", "virginie"):
            docker("rm", "-f", "-v", pg_nom(n, 5433), check=False)
    delais(1.0)


# ---- 6. volume et prix ------------------------------------------------------------------------------
def volume():
    print("\n6. réplication de 10 000 INSERT de 200 octets vers 2 répliques")
    p, reps = topologie()

    def tx():
        return int(docker("exec", region("paris"), "cat", "/sys/class/net/eth0/statistics/tx_bytes").stdout)

    lsn0 = p.execute("select pg_current_wal_lsn()").fetchone()[0]
    o0 = tx()
    client("paris", "ecrire", IP["paris"], 5432, 2500, 4, "local")
    lsn1 = p.execute("select pg_current_wal_lsn()").fetchone()[0]
    while p.execute("select bool_and(replay_lsn >= %s) from pg_stat_replication", (lsn1,)).fetchone()[0] is not True:
        time.sleep(0.2)
    time.sleep(1)
    o1 = tx()
    wal = p.execute("select pg_wal_lsn_diff(%s, %s)", (lsn1, lsn0)).fetchone()[0]
    print(f"   journal (WAL) écrit : {float(wal) / 10000:.0f} octets par transaction")
    print(f"   envoyé par Paris : {(o1 - o0) / 10000:.0f} octets par transaction,"
          f" {(o1 - o0) / 20000:.0f} par réplique")
    par_replique = (o1 - o0) / 20000
    go_mois = 1000 * par_replique * 730 * 3600 / 1e9
    print(f"   à 1 000 transactions par seconde : {nb(go_mois)} Go par mois et par réplique")
    print(f"   prix depuis Paris (0,02 $/Go) : {nb(go_mois * PRIX_INTER['paris'] * 2)} $ par mois"
          " pour 2 répliques")
    print(f"   même flux depuis Singapour (0,09 $/Go) : {nb(go_mois * PRIX_INTER['singapour'] * 2)} $ par mois")


def main():
    preparer()
    try:
        allers_retours()
        ecritures()
        retard()
        bascule()
        actif_actif()
        volume()
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
