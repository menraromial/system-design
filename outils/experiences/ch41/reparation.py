"""Chapitre 41 : la réparation de Cassandra, par arbres de Merkle, mesurée.

Deux nœuds Cassandra 5 (cours-sd-ch41-c1 et -c2, réseau cours-sd-ch41-reseau),
facteur de réplication 2 ; une table de 200 000 lignes de 100 octets écrites en
ALL (valeurs aléatoires, incompressibles). Pour d = 0, 10, 1 000 et 20 000 :
c2 est suspendu (docker pause), les
indications différées sont désactivées, d lignes existantes sont réécrites en
ONE par c1 ; c2 revient, puis `nodetool repair -full` est lancé depuis c2. On
mesure la durée de la réparation, le nombre d'intervalles déclarés désynchronisés
dans le journal de Cassandra, les octets que c2 reçoit (lignes « Receiving N
files(…) » du journal ; la colonne bytes_received de system_views.streaming
compte davantage que ce qui est annoncé), et l'on vérifie la copie de c2 (c1
suspendu, lecture en ONE par c2).

    python3 reparation.py      (environ 6 minutes)
"""
import os
import random
import re
import subprocess
import time

from cassandra import ConsistencyLevel as CL
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy

NOEUDS, RESEAU, LIGNES = ("c1", "c2"), "cours-sd-ch41-reseau", 200_000


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def nodetool(noeud, *args):
    return docker("exec", f"cours-sd-ch41-{noeud}", "nodetool", *args, check=False).stdout


def cqlsh(noeud, requete):
    return docker("exec", f"cours-sd-ch41-{noeud}", "cqlsh", "-e", requete, check=False).stdout


def nettoyer():
    subprocess.run(["docker", "rm", "-f", "-v", *(f"cours-sd-ch41-{n}" for n in NOEUDS)], capture_output=True)
    subprocess.run(["docker", "network", "rm", RESEAU], capture_output=True)


def lancer(noeud):
    docker("run", "-d", "--name", f"cours-sd-ch41-{noeud}", "--hostname", f"cours-sd-ch41-{noeud}",
           "--network", RESEAU, "--memory", "1400m",
           "-e", "CASSANDRA_SEEDS=cours-sd-ch41-c1", "-e", "CASSANDRA_CLUSTER_NAME=cours",
           "-e", "MAX_HEAP_SIZE=512M", "-e", "HEAP_NEWSIZE=128M",
           "-e", "JVM_EXTRA_OPTS=-Dcassandra.ring_delay_ms=1000 -Dcassandra.skip_wait_for_gossip_to_settle=0",
           "cassandra:5")


def adresse(noeud):
    return docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                  f"cours-sd-ch41-{noeud}").stdout.strip()


def attendre(etat, n, vu_de="c1", delai=300):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        if sum(l.startswith(etat) for l in nodetool(vu_de, "status").splitlines()) >= n:
            return
        time.sleep(1)
    raise RuntimeError(f"pas {n} nœuds {etat}")


UNITES = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3}


def octets_annonces(journal):
    """Octets à recevoir annoncés par les sessions de flux (« Receiving N files(X KiB) »)."""
    return sum(float(v) * UNITES[u] for v, u in re.findall(r"Receiving \d+ files\(([\d.]+)(B|KiB|MiB|GiB)\)", journal))


def main():
    nettoyer()
    try:
        docker("network", "create", RESEAU)
        t = time.monotonic()
        for i, n in enumerate(NOEUDS, 1):
            lancer(n)
            attendre("UN", i)
        print(f"Cassandra {nodetool('c1', 'version').split()[-1]}, 2 nœuds prêts en {time.monotonic() - t:.0f} s")
        ip = {n: adresse(n) for n in NOEUDS}
        profils = {f"{n}-{nom}": ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([ip[n]]),
                                                  retry_policy=FallthroughRetryPolicy(),
                                                  consistency_level=niveau, request_timeout=30)
                   for n in NOEUDS for nom, niveau in (("ONE", CL.ONE), ("ALL", CL.ALL))}
        cluster = Cluster([ip["c1"]], connection_class=AsyncioConnection, execution_profiles=profils,
                          protocol_version=5)
        s = cluster.connect()
        s.execute("CREATE KEYSPACE cours WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
        s.execute("CREATE TABLE cours.t (k int PRIMARY KEY, v text)")
        time.sleep(2)
        ecrire = s.prepare("INSERT INTO cours.t (k, v) VALUES (?, ?)")
        lire = s.prepare("SELECT v FROM cours.t WHERE k = ?")
        t = time.monotonic()
        execute_concurrent_with_args(s, ecrire,
                                     [(k, os.urandom(50).hex()) for k in range(LIGNES)], concurrency=100,
                                     execution_profile="c1-ALL")
        for n in NOEUDS:
            nodetool(n, "flush", "cours", "t")
        taille = re.search(r"Space used \(live\): (\d+)", nodetool("c1", "tablestats", "cours.t"))
        print(f"{LIGNES} lignes de 100 o écrites en ALL en {time.monotonic() - t:.0f} s ;"
              f" table sur disque, par nœud : {int(taille.group(1)) / 1e6:.1f} Mo")
        for n in NOEUDS:
            nodetool(n, "disablehandoff")

        alea = random.Random(41)
        print(f"\n{'lignes':>8}{'durée':>8}{'intervalles':>13}{'reçu par c2':>13}{'octets/ligne':>14}{'c2 à jour':>11}")
        print(f"{'manquées':>8}{'':>8}{'désynchr.':>13}")
        for d in (0, 10, 1_000, 20_000):
            cles = alea.sample(range(LIGNES), d)
            if d:
                docker("pause", "cours-sd-ch41-c2")
                attendre("DN", 1)
                nouvelles = {k: os.urandom(50).hex() for k in cles}
                execute_concurrent_with_args(s, ecrire, list(nouvelles.items()), concurrency=100,
                                             execution_profile="c1-ONE")
                docker("unpause", "cours-sd-ch41-c2")
                attendre("UN", 2)
                attendre("UN", 2, vu_de="c2")
            for n in NOEUDS:
                nodetool(n, "flush", "cours", "t")
            debut_journal = str(int(time.time()) - 1)
            t = time.monotonic()
            nodetool("c2", "repair", "-full", "cours", "t")
            duree = time.monotonic() - t
            journal = docker("logs", "--since", debut_journal, "cours-sd-ch41-c2").stdout
            desync = sum(int(m) for m in re.findall(r"have (\d+) range\(s\) out of sync", journal))
            recu = octets_annonces(journal)
            # vérification : c1 suspendu, lecture en ONE par c2
            echantillon = cles[:200]
            a_jour = "-"
            if echantillon:
                docker("pause", "cours-sd-ch41-c1")
                attendre("DN", 1, vu_de="c2")
                ok = sum(s.execute(lire, (k,), execution_profile="c2-ONE").one().v == nouvelles[k]
                         for k in echantillon)
                docker("unpause", "cours-sd-ch41-c1")
                attendre("UN", 2)
                a_jour = f"{ok}/{len(echantillon)}"
            par_ligne = f"{recu / d:.0f}" if d else "-"
            print(f"{d:>8}{duree:>7.1f}s{desync:>13}{recu / 1e6:>11.2f}Mo{par_ligne:>14}{a_jour:>11}")
        cluster.shutdown()
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
