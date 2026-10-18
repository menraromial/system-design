"""Chapitre 33 : les horloges de la machine.

1. résolution et coût de lecture des horloges de Linux (CLOCK_REALTIME,
   CLOCK_MONOTONIC, CLOCK_MONOTONIC_RAW) depuis Python ;
2. état de la discipline d'horloge du noyau, lu par adjtimex(2) sans rien
   modifier : correction de fréquence, erreur estimée, erreur maximale ;
3. écart de vitesse entre CLOCK_MONOTONIC, que NTP corrige, et
   CLOCK_MONOTONIC_RAW, qui suit l'oscillateur brut, mesuré sur 60 s.

    python3 horloges.py
"""
import ctypes
import ctypes.util
import time

HORLOGES = {
    "CLOCK_REALTIME": time.CLOCK_REALTIME,
    "CLOCK_MONOTONIC": time.CLOCK_MONOTONIC,
    "CLOCK_MONOTONIC_RAW": time.CLOCK_MONOTONIC_RAW,
}


class Timex(ctypes.Structure):
    """struct timex de <sys/timex.h> (Linux, 64 bits)."""
    _fields_ = [("modes", ctypes.c_uint), ("offset", ctypes.c_long),
                ("freq", ctypes.c_long), ("maxerror", ctypes.c_long),
                ("esterror", ctypes.c_long), ("status", ctypes.c_int),
                ("constant", ctypes.c_long), ("precision", ctypes.c_long),
                ("tolerance", ctypes.c_long), ("tv_sec", ctypes.c_long),
                ("tv_usec", ctypes.c_long), ("tick", ctypes.c_long),
                ("ppsfreq", ctypes.c_long), ("jitter", ctypes.c_long),
                ("shift", ctypes.c_int), ("stabil", ctypes.c_long),
                ("jitcnt", ctypes.c_long), ("calcnt", ctypes.c_long),
                ("errcnt", ctypes.c_long), ("stbcnt", ctypes.c_long),
                ("tai", ctypes.c_int), ("_reserve", ctypes.c_int * 11)]


def lecture():
    print("1. lecture des horloges (1 million de lectures)")
    for nom, h in HORLOGES.items():
        res = time.clock_getres(h)
        debut = time.perf_counter_ns()
        for _ in range(1_000_000):
            time.clock_gettime_ns(h)
        cout = (time.perf_counter_ns() - debut) / 1_000_000
        print(f"   {nom:21} résolution {res * 1e9:3.0f} ns, "
              f"lecture {cout:4.0f} ns")


def discipline():
    print("\n2. discipline de l'horloge par le noyau (adjtimex, lecture seule)")
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    tx = Timex()
    etat = libc.adjtimex(ctypes.byref(tx))
    noms = {0: "TIME_OK", 1: "TIME_INS", 2: "TIME_DEL", 3: "TIME_OOP",
            4: "TIME_WAIT", 5: "TIME_ERROR"}
    print(f"   état {noms.get(etat, etat)}, horloge "
          f"{'non synchronisée' if tx.status & 0x40 else 'synchronisée'}")
    print(f"   correction de fréquence {tx.freq / 65536:+.3f} ppm "
          f"(tolérance {tx.tolerance / 65536:.0f} ppm)")
    print(f"   erreur estimée {tx.esterror} µs, erreur maximale "
          f"{tx.maxerror} µs")


def vitesse(duree=60):
    print(f"\n3. CLOCK_MONOTONIC contre CLOCK_MONOTONIC_RAW sur {duree} s")
    m0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    r0 = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
    time.sleep(duree)
    m1 = time.clock_gettime_ns(time.CLOCK_MONOTONIC)
    r1 = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
    ecart = (m1 - m0) - (r1 - r0)
    print(f"   la monotone a avancé de {ecart / 1000:+.1f} µs de plus que la "
          f"brute,")
    print(f"   soit {ecart / (r1 - r0) * 1e6:+.3f} ppm")


if __name__ == "__main__":
    lecture()
    discipline()
    vitesse()
