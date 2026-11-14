"""Fonction limitée par le processeur : hache 64 Mo de données, sur un seul fil."""
import hashlib
import os
import time

DONNEES = os.urandom(1 << 20)


def handler(event, context):
    t = time.perf_counter()
    h = hashlib.sha256()
    for _ in range(int(event.get("mo", 64))):
        h.update(DONNEES)
    return {"ms": (time.perf_counter() - t) * 1000, "memoire": context.memory_limit_in_mb}
