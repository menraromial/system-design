"""Chapitre 17 : une clé chaude est limitée par le nœud qui la porte.

Quatre nœuds Redis indépendants (conteneurs « cours-sd-ch17-n1 » à « -n4 »,
ports 9781 à 9784), comme les quatre partitions d'un cache distribué. Une même
clé de 1 Kio est lue par redis-benchmark (GET, 50 connexions par instance,
2 millions de lectures), de trois façons :
  1. une instance de redis-benchmark contre un seul nœud ;
  2. quatre instances contre le même nœud : la clé n'existe qu'à un endroit ;
  3. quatre instances, chacune contre un nœud différent qui porte une copie de
     la clé : la clé chaude a été recopiée sur les quatre partitions.
On publie le total des lectures par seconde.
Prérequis : docker, image redis:8.8-alpine. Usage : python3 chaudes.py
"""
import subprocess
import time

import redis

PORTS = (9781, 9782, 9783, 9784)
NOEUDS = [f"cours-sd-ch17-n{i}" for i in range(1, 5)]
CLE = "key:__rand_int__"       # sans -r, redis-benchmark lit toujours cette clé


def docker(*args, **kw):
    return subprocess.run(["docker", *args], **kw)


def nettoyer():
    for nom in NOEUDS + [f"cours-sd-ch17-bench{i}" for i in range(4)]:
        docker("rm", "-f", nom, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def lancer():
    nettoyer()
    for nom, port in zip(NOEUDS, PORTS):
        docker("run", "-d", "--network", "host", "--name", nom, "redis:8.8-alpine", "redis-server",
               "--port", str(port), "--save", "", "--appendonly", "no",
               check=True, stdout=subprocess.DEVNULL)
    for port in PORTS:
        r = redis.Redis(port=port)
        for _ in range(100):
            try:
                r.set(CLE, b"v" * 1024)
                break
            except redis.ConnectionError:
                time.sleep(0.1)


def bancs(ports):
    """Lance un redis-benchmark par port, en parallèle ; renvoie le total des GET/s."""
    procs = [subprocess.Popen(["docker", "run", "--rm", "--network", "host", "--name",
                               f"cours-sd-ch17-bench{i}", "redis:8.8-alpine", "redis-benchmark",
                               "-p", str(p), "-t", "get", "-n", "2000000", "-c", "50", "-q", "--csv"],
                              stdout=subprocess.PIPE, text=True) for i, p in enumerate(ports)]
    total = 0.0
    for p in procs:
        sortie, _ = p.communicate()
        ligne = [l for l in sortie.splitlines() if l.startswith('"GET"')][0]
        total += float(ligne.split(",")[1].strip('"'))
    return total


def main():
    lancer()
    try:
        print("une clé de 1 Kio lue par redis-benchmark (GET, 50 connexions par instance)")
        print(f"{'montage':<44}{'lectures/s':>14}")
        for nom, ports in (("1 banc, 1 nœud", [PORTS[0]]),
                           ("4 bancs, la clé sur un seul nœud", [PORTS[0]] * 4),
                           ("4 bancs, une copie de la clé par nœud", list(PORTS))):
            debit = f"{bancs(ports):,.0f}".replace(",", " ")
            print(f"{nom:<44}{debit:>14}")
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
