"""Chapitre 25 : quorums, réparation et dernier écrivain dans une grappe Cassandra.

Trois nœuds Cassandra 5 (cours-sd-ch25-c1 à c3) sur un réseau Docker ; le client
choisit le nœud coordinateur de chaque requête. Le facteur de réplication est 3,
si bien que chaque nœud a une copie de chaque ligne.
  1. latence d'une écriture et d'une lecture selon le niveau de cohérence
     (ONE, QUORUM, ALL), médiane sur 300 ;
  2. c3 est suspendu (docker pause), indications différées désactivées : 1 000
     écritures en QUORUM réussissent, une écriture en ALL échoue ; c3 revient ;
     on compte les clés absentes de la copie de c3 (lue seule, c1 et c2 étant
     suspendus le temps de la lecture), puis après avoir lu les 1 000 clés en
     QUORUM (réparation à la lecture), puis après nodetool repair ;
  3. même chose avec les indications différées (hinted handoff) actives : la
     copie de c3 est lue aussitôt après son retour, puis 60 s après ;
  4. dernier écrivain gagne : (a) un client dont l'horloge retarde de 2 s écrit
     après un autre, et son écriture disparaît ; (b) 8 clients incrémentent la
     même valeur par lecture puis écriture en QUORUM, 200 fois chacun ; puis la
     même chose avec une transaction légère (UPDATE ... IF, Paxos).
Prérequis : docker, image cassandra:5, pip install -r requirements.txt.
Usage : python3 quorum.py
"""
import statistics
import subprocess
import threading
import time

from cassandra import ConsistencyLevel as CL
from cassandra import Unavailable, WriteTimeout, ReadTimeout
from cassandra.cluster import Cluster, ExecutionProfile, NoHostAvailable
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import FallthroughRetryPolicy, WhiteListRoundRobinPolicy

RESEAU, NOEUDS = "cours-sd-ch25-reseau", ["c1", "c2", "c3"]


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def nettoyer():
    subprocess.run(["docker", "rm", "-f", "-v", *(f"cours-sd-ch25-{n}" for n in NOEUDS)],
                   capture_output=True)
    subprocess.run(["docker", "network", "rm", RESEAU], capture_output=True)


def nodetool(noeud, *args):
    return docker("exec", f"cours-sd-ch25-{noeud}", "nodetool", *args, check=False).stdout


def lancer(noeud):
    docker("run", "-d", "--name", f"cours-sd-ch25-{noeud}", "--hostname", f"cours-sd-ch25-{noeud}",
           "--network", RESEAU, "--memory", "1400m",
           "-e", "CASSANDRA_SEEDS=cours-sd-ch25-c1", "-e", "CASSANDRA_CLUSTER_NAME=cours",
           "-e", "MAX_HEAP_SIZE=512M", "-e", "HEAP_NEWSIZE=128M",
           "-e", "JVM_EXTRA_OPTS=-Dcassandra.ring_delay_ms=1000 -Dcassandra.skip_wait_for_gossip_to_settle=0",
           "cassandra:5")


def adresse(noeud):
    return docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                  f"cours-sd-ch25-{noeud}").stdout.strip()


def attendre_etat(etat, n, delai=300):
    """Attend que nodetool status (vu de c1) compte n nœuds dans l'état donné (UN, DN)."""
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        lignes = nodetool("c1", "status").splitlines()
        if sum(l.startswith(etat) for l in lignes) >= n:
            return
        time.sleep(1)
    raise RuntimeError(f"pas {n} nœuds {etat}")


def centiles(durees):
    durees = sorted(durees)
    return statistics.median(durees), durees[int(0.99 * (len(durees) - 1))]


class Grappe:
    def __init__(self):
        self.ip = {n: adresse(n) for n in NOEUDS}
        profils = {}
        for n in NOEUDS:
            for nom, niveau in (("ONE", CL.ONE), ("QUORUM", CL.QUORUM), ("ALL", CL.ALL), ("ANY", CL.ANY),
                               ("SERIAL", CL.SERIAL)):
                profils[f"{n}-{nom}"] = ExecutionProfile(
                    load_balancing_policy=WhiteListRoundRobinPolicy([self.ip[n]]),
                    retry_policy=FallthroughRetryPolicy(), consistency_level=niveau,
                    serial_consistency_level=CL.SERIAL, request_timeout=20)
        self.cluster = Cluster([self.ip["c1"]], connection_class=AsyncioConnection,
                               execution_profiles=profils, protocol_version=5)
        self.s = self.cluster.connect()

    def run(self, cql, args=(), noeud="c1", niveau="QUORUM"):
        return self.s.execute(cql, args, execution_profile=f"{noeud}-{niveau}")


def copie_c3(g, cles):
    """Clés absentes de la copie de c3 : c1 et c2 suspendus, lecture en ONE par c3."""
    for n in ("c1", "c2"):
        docker("pause", f"cours-sd-ch25-{n}")
    fin = time.monotonic() + 120
    while time.monotonic() < fin:
        etat = nodetool("c3", "status").splitlines()
        if sum(l.startswith("DN") for l in etat) >= 2:
            break
        time.sleep(1)
    n = manquantes(g, cles, "c3", "ONE")
    for x in ("c1", "c2"):
        docker("unpause", f"cours-sd-ch25-{x}")
    attendre_etat("UN", 3)
    fin = time.monotonic() + 120
    while time.monotonic() < fin and sum(l.startswith("UN") for l in nodetool("c3", "status").splitlines()) < 3:
        time.sleep(1)
    return n


def manquantes(g, cles, noeud, niveau):
    n = 0
    for k in cles:
        ligne = g.run("SELECT v FROM cours.kv WHERE k = %s", (k,), noeud, niveau).one()
        if ligne is None or ligne.v != k:
            n += 1
    return n


def main():
    nettoyer()
    try:
        docker("network", "create", RESEAU)
        t = time.monotonic()
        for i, n in enumerate(NOEUDS, 1):
            lancer(n)
            attendre_etat("UN", i)
        print(f"Cassandra {nodetool('c1', 'version').split()[-1]}, 3 nœuds prêts en {time.monotonic() - t:.0f} s")
        g = Grappe()
        g.run("CREATE KEYSPACE cours WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
        g.run("CREATE TABLE cours.kv (k int PRIMARY KEY, v int)")
        g.run("CREATE TABLE cours.compteur (k int PRIMARY KEY, v int)")
        time.sleep(2)

        print("\n1. latence médiane sur 300, en ms (coordinateur c1, réplication 3)")
        print(f"   {'niveau':<10}{'écriture':>10}{'lecture':>10}")
        for niveau in ("ONE", "QUORUM", "ALL"):
            ecr, lec = [], []
            for k in range(300):
                t = time.perf_counter()
                g.run("INSERT INTO cours.kv (k, v) VALUES (%s, %s)", (k, k), "c1", niveau)
                ecr.append(1000 * (time.perf_counter() - t))
                t = time.perf_counter()
                g.run("SELECT v FROM cours.kv WHERE k = %s", (k,), "c1", niveau)
                lec.append(1000 * (time.perf_counter() - t))
            print(f"   {niveau:<10}{centiles(ecr)[0]:>10.2f}{centiles(lec)[0]:>10.2f}")

        print("\n2. c3 suspendu, indications différées désactivées")
        for n in NOEUDS:
            nodetool(n, "disablehandoff")
        docker("pause", "cours-sd-ch25-c3")
        attendre_etat("DN", 1)
        cles = range(10_000, 11_000)
        for k in cles:
            g.run("INSERT INTO cours.kv (k, v) VALUES (%s, %s)", (k, k), "c1", "QUORUM")
        print("   1 000 écritures en QUORUM (2 réponses sur 3) : réussies")
        try:
            g.run("INSERT INTO cours.kv (k, v) VALUES (%s, %s)", (99_999, 1), "c1", "ALL")
            print("   une écriture en ALL : réussie")
        except (Unavailable, WriteTimeout, NoHostAvailable) as e:
            print(f"   une écriture en ALL : refusée ({type(e).__name__})")
        docker("unpause", "cours-sd-ch25-c3")
        attendre_etat("UN", 3)
        print("   c3 revenu ; clés absentes de sa copie (lue seule, c1 et c2 suspendus) :")
        print(f"      après son retour : {copie_c3(g, cles)} sur 1 000")
        print(f"      lecture des 1 000 clés en QUORUM par c3 : {manquantes(g, cles, 'c3', 'QUORUM')} absentes")
        print(f"      après ces lectures en QUORUM : {copie_c3(g, cles)} sur 1 000")
        t = time.perf_counter()
        nodetool("c3", "repair", "cours", "kv")
        print(f"      après nodetool repair ({time.perf_counter() - t:.0f} s) : {copie_c3(g, cles)} sur 1 000")

        print("\n3. c3 suspendu, indications différées actives")
        for n in NOEUDS:
            nodetool(n, "enablehandoff")
        docker("pause", "cours-sd-ch25-c3")
        attendre_etat("DN", 1)
        cles = range(20_000, 21_000)
        for k in cles:
            g.run("INSERT INTO cours.kv (k, v) VALUES (%s, %s)", (k, k), "c1", "QUORUM")
        print("   1 000 écritures en QUORUM : réussies")
        docker("unpause", "cours-sd-ch25-c3")
        t = time.monotonic()
        attendre_etat("UN", 3)
        print(f"   c3 vu de nouveau UP par c1 après {time.monotonic() - t:.0f} s ;"
              " clés absentes de sa copie :")
        print(f"      aussitôt : {copie_c3(g, cles)} sur 1 000")
        while time.monotonic() - t < 60:
            time.sleep(1)
        print(f"      60 s après son retour : {copie_c3(g, cles)} sur 1 000")

        print("\n4. dernier écrivain gagne")
        maintenant = int(time.time() * 1_000_000)
        g.run("INSERT INTO cours.kv (k, v) VALUES (1, 100) USING TIMESTAMP %s", (maintenant,))
        time.sleep(0.5)
        g.run("INSERT INTO cours.kv (k, v) VALUES (1, 200) USING TIMESTAMP %s", (maintenant + 500_000 - 2_000_000,),
              "c2")
        v = g.run("SELECT v FROM cours.kv WHERE k = 1").one().v
        print(f"   a) 100 écrit, puis 200 écrit 0,5 s plus tard par un client")
        print(f"      dont l'horloge retarde de 2 s :")
        print(f"      valeur lue {v}, aucune erreur")

        g.run("INSERT INTO cours.compteur (k, v) VALUES (1, 0)")
        def incrementer(n_noeud):
            for _ in range(200):
                v = g.run("SELECT v FROM cours.compteur WHERE k = 1", (), n_noeud).one().v
                g.run("UPDATE cours.compteur SET v = %s WHERE k = 1", (v + 1,), n_noeud)
        fils = [threading.Thread(target=incrementer, args=(NOEUDS[i % 3],)) for i in range(8)]
        t = time.perf_counter()
        for f in fils:
            f.start()
        for f in fils:
            f.join()
        d = time.perf_counter() - t
        v = g.run("SELECT v FROM cours.compteur WHERE k = 1").one().v
        print(f"   b) 8 clients x 200 incréments, lecture puis écriture QUORUM :")
        print(f"      {v} au lieu de 1 600 ({d:.1f} s)")

        g.run("INSERT INTO cours.compteur (k, v) VALUES (2, 0)")
        durees, essais, inconnus = [], [0], [0]
        def incrementer_lwt(n_noeud):
            for _ in range(200):
                while True:
                    v = g.run("SELECT v FROM cours.compteur WHERE k = 2", (), n_noeud, "QUORUM").one().v
                    t = time.perf_counter()
                    try:
                        r = g.run("UPDATE cours.compteur SET v = %s WHERE k = 2 IF v = %s", (v + 1, v), n_noeud)
                        ok = r.was_applied
                    except (WriteTimeout, ReadTimeout):
                        ok = False
                    except Exception as e:          # résultat inconnu : appliqué ou non ?
                        if "unknown" not in str(e):
                            raise
                        inconnus[0] += 1
                        ok = False
                    durees.append(1000 * (time.perf_counter() - t))
                    essais[0] += 1
                    if ok:
                        break
        fils = [threading.Thread(target=incrementer_lwt, args=(NOEUDS[i % 3],)) for i in range(8)]
        t = time.perf_counter()
        for f in fils:
            f.start()
        for f in fils:
            f.join()
        d = time.perf_counter() - t
        v = g.run("SELECT v FROM cours.compteur WHERE k = 2", (), "c1", "SERIAL").one().v
        med, p99 = centiles(durees)
        print(f"   c) même chose avec UPDATE ... IF v = ancienne valeur :")
        print(f"      {v} ({d:.1f} s), {essais[0]} tentatives,")
        print(f"      médiane {med:.1f} ms, p99 {p99:.0f} ms par tentative,")
        print(f"      {inconnus[0]} au résultat inconnu (« CAS operation result is unknown »)")
        g.cluster.shutdown()
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
