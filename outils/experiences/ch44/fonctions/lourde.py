"""Fonction qui importe numpy et pandas à l'initialisation (code statique)."""
import os
import time

T0 = time.perf_counter()
import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402
INIT_MS = (time.perf_counter() - T0) * 1000
APPELS = 0


def handler(event, context):
    global APPELS
    APPELS += 1
    s = pd.Series(np.arange(1000)).sum()
    return {"init_ms": INIT_MS, "appels": APPELS, "pid": os.getpid(), "somme": int(s)}
