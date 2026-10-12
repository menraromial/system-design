"""Chapitre 16 : quelles clés garder quand le cache est plein ?

Un cache Redis limité (maxmemory 12 Mo, soit environ 8 250 valeurs de 1 Kio,
Redis comptant aussi ses propres structures, pour 100 000 clés possibles) reçoit trois profils d'accès de 200 000
lectures chacun, en cache-aside (GET, puis SET si la clé manque) :
  - zipf : popularité de Zipf (s = 1) sur 100 000 clés ;
  - zipf + balayages : le même, interrompu toutes les 20 000 lectures par un
    balayage de 15 000 clés froides lues une fois chacune (un traitement par lots) ;
  - popularité qui change : quatre phases de 50 000 lectures, chacune avec un
    ensemble de clés populaires différent.
Politiques de Redis : allkeys-lru, allkeys-lfu, allkeys-random (approximations
par échantillonnage). La même suite est rejouée dans une simulation exacte de
LRU, LFU et FIFO, à la capacité observée dans Redis. Seules les lectures
ordinaires (hors balayages) sont comptées dans le taux de succès.
Prérequis : docker, image redis:8.8-alpine. Usage : python3 eviction.py
"""
import collections
import heapq
import random
import subprocess
import time

import redis

PORT, NOM = 9679, "cours-sd-ch16-redis"
CLES, LECTURES = 100_000, 200_000
VALEUR = b"v" * 1024
REFUS = [0]                                   # SET refusés par Redis (mémoire pleine)


def profils():
    r = random.Random(16)
    poids = [1 / (k + 1) for k in range(CLES)]

    def zipf(n, permutation=None):
        rangs = r.choices(range(CLES), weights=poids, k=n)
        return [(permutation[k] if permutation else k, True) for k in rangs]

    zip_seul = zipf(LECTURES)
    balayages = []
    morceaux = zipf(LECTURES)
    for i in range(0, LECTURES, 20_000):
        balayages += morceaux[i:i + 20_000]
        cle0 = 50_000 + (i // 20_000) * 15_000 % 50_000
        balayages += [(50_000 + (cle0 - 50_000 + j) % 50_000, False) for j in range(15_000)]
    changeant = []
    for phase in range(4):
        perm = list(range(CLES))
        random.Random(phase).shuffle(perm)
        changeant += zipf(LECTURES // 4, perm)
    return {"zipf": zip_seul, "zipf + balayages": balayages, "popularité qui change": changeant}


def lancer_redis():
    subprocess.run(["docker", "rm", "-f", NOM], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "run", "-d", "--network", "host", "--name", NOM, "redis:8.8-alpine",
                    "redis-server", "--port", str(PORT), "--save", "", "--appendonly", "no",
                    "--maxmemory", "12mb"], check=True, stdout=subprocess.DEVNULL)
    r = redis.Redis(port=PORT)
    for _ in range(100):
        try:
            r.ping()
            return r
        except redis.ConnectionError:
            time.sleep(0.1)
    raise RuntimeError("Redis muet")


def rejouer_redis(r, suite, politique):
    r.flushall(asynchronous=False)            # FLUSHALL SYNC : mémoire rendue avant de repartir
    while r.info("memory")["used_memory"] > 2_000_000:
        time.sleep(0.05)
    r.config_set("maxmemory-policy", politique)
    succes = ordinaires = 0
    tailles = []
    for i, (k, ordinaire) in enumerate(suite):
        cle = f"k{k}"
        trouve = r.get(cle) is not None
        if not trouve:
            for _ in range(50):                # Redis refuse parfois, le temps d'évincer
                try:
                    r.set(cle, VALEUR)
                    break
                except redis.exceptions.OutOfMemoryError:
                    REFUS[0] += 1
                    time.sleep(0.001)
        if ordinaire:
            ordinaires += 1
            succes += trouve
        if i % 20_000 == 19_999:
            tailles.append(r.dbsize())
    return succes / ordinaires, sum(tailles[1:]) / len(tailles[1:])


def rejouer_exact(suite, capacite, politique):
    succes = ordinaires = 0
    if politique in ("LRU", "FIFO"):
        cache = collections.OrderedDict()
        for k, ordinaire in suite:
            trouve = k in cache
            if trouve and politique == "LRU":
                cache.move_to_end(k)
            if not trouve:
                cache[k] = True
                if len(cache) > capacite:
                    cache.popitem(last=False)
            ordinaires += ordinaire
            succes += trouve and ordinaire
    else:                                    # LFU exact, sans vieillissement des compteurs
        compte, tas, present, t = collections.Counter(), [], set(), 0
        for k, ordinaire in suite:
            t += 1
            trouve = k in present
            compte[k] += 1
            heapq.heappush(tas, (compte[k], t, k))
            if not trouve:
                present.add(k)
                while len(present) > capacite:
                    c, _, v = heapq.heappop(tas)
                    if v in present and c == compte[v]:
                        present.discard(v)
            ordinaires += ordinaire
            succes += trouve and ordinaire
    return succes / ordinaires


def main():
    suites = profils()
    r = lancer_redis()
    try:
        print(f"cache de 12 Mo (Redis), {CLES} clés possibles de 1 Kio, {LECTURES} lectures par profil")
        print(f"{'profil':<24}{'lru':>8}{'lfu':>8}{'random':>8}   {'exact :':<8}{'LRU':>6}{'LFU':>6}{'FIFO':>6}")
        for nom, suite in suites.items():
            taux, capacites = {}, []
            for pol in ("allkeys-lru", "allkeys-lfu", "allkeys-random"):
                t, cap = rejouer_redis(r, suite, pol)
                taux[pol], capacites = t, capacites + [cap]
            capacite = int(sum(capacites) / len(capacites))
            ex = {p: rejouer_exact(suite, capacite, p) for p in ("LRU", "LFU", "FIFO")}
            print(f"{nom:<24}" + "".join(f"{100 * taux[p]:>7.1f}%" for p in taux)
                  + "   " + " " * 8 + "".join(f"{100 * ex[p]:>5.1f}%" for p in ex))
        print(f"capacité observée dans Redis : environ {capacite} clés ; SET refusés puis réessayés : {REFUS[0]}")
    finally:
        subprocess.run(["docker", "rm", "-f", NOM], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
