"""Fonction minimale : rien à initialiser."""
import os
import time

T0 = time.perf_counter()
INIT_MS = (time.perf_counter() - T0) * 1000
APPELS = 0


def handler(event, context):
    global APPELS
    APPELS += 1
    return {"init_ms": INIT_MS, "appels": APPELS, "pid": os.getpid()}
