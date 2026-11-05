"""Chapitre 40 : outils communs aux trois scripts.

Redis 8.8 (module RedisBloom compris : BF, CF, CMS, TOPK) dans le conteneur
cours-sd-ch40-redis, publié sur 127.0.0.1:12079, supprimé à la fin.
"""
import hashlib
import subprocess
import time

import redis

NOM, PORT = "cours-sd-ch40-redis", 12079


def demarrer_redis():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)
    subprocess.run(["docker", "run", "-d", "--name", NOM, "-p", f"127.0.0.1:{PORT}:6379",
                    "redis:8.8-alpine", "--save", "", "--appendonly", "no"],
                   check=True, capture_output=True)
    r = redis.Redis(port=PORT)
    for _ in range(100):
        try:
            r.ping()
            return r
        except redis.ConnectionError:
            time.sleep(0.2)
    raise RuntimeError("Redis absent")


def arreter_redis():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)


def hache128(texte, sel=b""):
    """Deux entiers de 64 bits tirés d'un BLAKE2b de 128 bits."""
    h = hashlib.blake2b(texte.encode(), digest_size=16, salt=sel.ljust(16, b"\0")[:16]).digest()
    return int.from_bytes(h[:8], "little"), int.from_bytes(h[8:], "little")


def par_lots(suite, taille):
    lot = []
    for x in suite:
        lot.append(x)
        if len(lot) == taille:
            yield lot
            lot = []
    if lot:
        yield lot


def mo(octets):
    return octets / 1e6


def nb(x, largeur=0):
    """Entier avec des espaces entre les milliers, aligné à droite."""
    return f"{x:,.0f}".replace(",", " ").rjust(largeur)
