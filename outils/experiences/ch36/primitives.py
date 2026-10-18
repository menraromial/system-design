"""Chapitre 36 : les primitives de coordination de ZooKeeper et d'etcd.

1. un processus crée un nœud éphémère (ZooKeeper, session de 4 s) ou une clé
   attachée à un bail (etcd, TTL 4 s), puis il est tué (SIGKILL) : au bout de
   combien de temps le nœud, ou la clé, disparaît-il ? Et s'il se ferme
   proprement ?
2. délai entre une écriture et la notification d'un observateur (watch),
   200 mesures pour chaque service ;
3. verrou ZooKeeper attendu par 50 clients, de deux façons : tous surveillent
   le nœud du verrou (effet de troupeau), ou chacun surveille le nœud qui le
   précède (recette séquentielle de l'article).

    python3 primitives.py
"""
import logging
import statistics
import subprocess
import sys
import threading
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

import commun

logging.getLogger("kazoo").setLevel(logging.CRITICAL)
ZK = f"127.0.0.1:{commun.ZK_PORT}"

PORTEUR_ZK = f"""
import sys, time, logging
logging.disable(logging.CRITICAL)
from kazoo.client import KazooClient
c = KazooClient(hosts="{ZK}", timeout=4)
c.start()
c.create("/cours/vivant", b"x", ephemeral=True, makepath=True)
print("pret", flush=True)
if sys.argv[1] == "propre":
    time.sleep(0.5)
    print("fermeture", flush=True)
    c.stop()
    c.close()
    sys.exit()
time.sleep(60)
"""

PORTEUR_ETCD = """
import sys, time
sys.path.insert(0, ".")
import commun
b = commun.bail(4)
commun.etcd_put("/cours/vivant", "x", b)
print("pret", flush=True)
if sys.argv[1] == "propre":
    time.sleep(0.5)
    print("fermeture", flush=True)
    commun.etcd("/v3/lease/revoke", {"ID": b})
    sys.exit()
while True:
    commun.renouveler(b)
    time.sleep(1)
"""


def zk():
    c = KazooClient(hosts=ZK, timeout=10)
    c.start()
    return c


def disparition_zk(fin):
    obs = zk()
    vu = threading.Event()

    def guet(evt):
        if evt.type == "DELETED":
            vu.set()
    p = subprocess.Popen([sys.executable, "-c", PORTEUR_ZK, fin],
                         stdout=subprocess.PIPE, text=True)
    p.stdout.readline()
    obs.exists("/cours/vivant", watch=guet)
    if fin == "propre":
        p.stdout.readline()
    t0 = time.monotonic()
    if fin == "tue":
        p.kill()
    p.wait()
    vu.wait(30)
    duree = time.monotonic() - t0
    obs.stop()
    obs.close()
    return duree


def disparition_etcd(fin):
    vu = threading.Event()

    def rappel(t, k):
        if t == "DELETE":
            vu.set()
            return False
    p = subprocess.Popen([sys.executable, "-c", PORTEUR_ETCD, fin],
                         stdout=subprocess.PIPE, text=True)
    p.stdout.readline()
    threading.Thread(target=commun.surveiller, args=("/cours/vivant", rappel),
                     daemon=True).start()
    time.sleep(0.3)
    if fin == "propre":
        p.stdout.readline()
    t0 = time.monotonic()
    if fin == "tue":
        p.kill()
    p.wait()
    vu.wait(30)
    return time.monotonic() - t0


def partie_disparition():
    print("1. durée avant la disparition d'un nœud éphémère (session de 4 s)")
    print("   ou d'une clé attachée à un bail (TTL de 4 s), 3 essais")
    for nom, f in (("ZooKeeper", disparition_zk), ("etcd", disparition_etcd)):
        tues = [f("tue") for _ in range(3)]
        propre = f("propre")
        print(f"   {nom:10} processus tué : "
              + ", ".join(f"{t:.2f}" for t in tues) + " s")
        print(f"   {'':10} fermeture propre : {propre * 1000:.0f} ms")


def partie_notification():
    print("\n2. écriture puis notification d'un observateur (200 mesures)")
    c = zk()
    c.ensure_path("/cours/n")
    lat = []
    for k in range(200):
        vu = threading.Event()
        c.get("/cours/n", watch=lambda e, v=vu: v.set())
        t0 = time.perf_counter()
        c.set("/cours/n", str(k).encode())
        vu.wait(5)
        lat.append((time.perf_counter() - t0) * 1000)
    c.stop()
    c.close()
    lat.sort()
    print(f"   ZooKeeper : médiane {statistics.median(lat):5.2f} ms, 99e c. "
          f"{lat[197]:5.2f} ms")
    temps, debuts = [], {}

    def rappel(t, k):
        if k in debuts:
            temps.append((time.perf_counter() - debuts[k]) * 1000)
        return len(temps) < 200
    th = threading.Thread(target=commun.surveiller, args=("/cours/w/", rappel),
                          daemon=True)
    th.start()
    time.sleep(0.3)
    for k in range(200):
        cle = f"/cours/w/{k}"
        debuts[cle] = time.perf_counter()
        commun.etcd_put(cle, "x")
        time.sleep(0.005)
    th.join(10)
    temps.sort()
    print(f"   etcd      : médiane {statistics.median(temps):5.2f} ms, 99e c. "
          f"{temps[197]:5.2f} ms")


def troupeau(n, sequentiel):
    """n clients prennent le verrou l'un après l'autre ; on compte les
    notifications reçues et les tentatives de création."""
    clients = [zk() for _ in range(n)]
    clients[0].ensure_path("/cours/verrous")
    notif, essais, fini = [0], [0], threading.Barrier(n + 1)
    verrou_lock = threading.Lock()

    def travailleur(c, i):
        if sequentiel:
            moi = c.create("/cours/verrous/v-", b"", ephemeral=True,
                           sequence=True)
            nom = moi.rsplit("/", 1)[1]
            while True:
                enfants = sorted(c.get_children("/cours/verrous"))
                if enfants[0] == nom:
                    break
                avant = enfants[enfants.index(nom) - 1]
                libre = threading.Event()

                def guet(e, l=libre):
                    with verrou_lock:
                        notif[0] += 1
                    l.set()
                if c.exists(f"/cours/verrous/{avant}", watch=guet):
                    libre.wait()
            c.delete(moi)                   # section critique vide
        else:
            while True:
                with verrou_lock:
                    essais[0] += 1
                try:
                    c.create("/cours/verrou", b"", ephemeral=True)
                    break
                except NodeExistsError:
                    libre = threading.Event()

                    def guet(e, l=libre):
                        with verrou_lock:
                            notif[0] += 1
                        l.set()
                    if c.exists("/cours/verrou", watch=guet):
                        libre.wait()
            c.delete("/cours/verrou")
        fini.wait()

    debut = time.monotonic()
    fils = [threading.Thread(target=travailleur, args=(c, i))
            for i, c in enumerate(clients)]
    for f in fils:
        f.start()
    fini.wait()
    duree = time.monotonic() - debut
    for c in clients:
        c.stop()
        c.close()
    return notif[0], essais[0], duree


def partie_troupeau():
    print("\n3. verrou ZooKeeper pris tour à tour par 50 clients")
    print(f"   {'recette':40} {'notifications':>13} {'créations':>10} "
          f"{'durée':>7}")
    for nom, seq in (("tous surveillent le nœud du verrou", False),
                     ("chacun surveille son prédécesseur", True)):
        n, e, d = troupeau(50, seq)
        print(f"   {nom:40} {n:13} {e if not seq else 50:10} {d:6.2f}s")


def main():
    commun.lancer(zk=True, etcd=True)
    try:
        version = commun.sh("docker", "exec", commun.NOMS[0], "sh", "-c",
                            "ls /apache-zookeeper*/lib | grep -o "
                            "'zookeeper-3[0-9.]*[0-9]' | head -1").stdout.strip()
        print(f"{version or 'ZooKeeper 3.9'}, etcd 3.7 ; un serveur chacun\n")
        partie_disparition()
        partie_notification()
        partie_troupeau()
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
