"""Chapitre 32 : battements de cœur, délais fixes et détecteur phi accrual.

Un processus émetteur envoie un battement UDP toutes les 100 ms pendant
5 minutes. Un relais (dans ce script) retarde chaque paquet d'un délai tiré
au hasard (2 ms de médiane, queue lognormale) et en perd 1 %. Le script
suspend aussi réellement l'émetteur (SIGSTOP) une douzaine de fois, de 0,25 à
2 s, comme le ferait une pause du ramasse-miettes ou d'une machine virtuelle.
Les heures d'arrivée sont enregistrées, puis plusieurs détecteurs sont rejoués
sur la trace : fausses suspicions pendant que l'émetteur est vivant, et temps
de détection d'une panne placée à 2 000 instants tirés au hasard.

    python3 battements.py [graine]
"""
import asyncio
import math
import os
import random
import signal
import socket
import statistics
import subprocess
import sys
import time
from statistics import NormalDist

PORT = 11234
INTERVALLE, DUREE = 0.1, 300

EMETTEUR = f"""
import socket, time
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
n = 0
prochain = time.monotonic()
while True:
    s.sendto(str(n).encode(), ("127.0.0.1", {PORT}))
    n += 1
    prochain = max(prochain + {INTERVALLE}, time.monotonic())
    time.sleep(max(0.0, prochain - time.monotonic()))
"""


async def enregistrer(graine):
    rng = random.Random(graine)
    arrivees, pauses = [], []
    boucle = asyncio.get_running_loop()
    debut = time.monotonic()

    class Relais(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            if rng.random() < 0.01:
                return                      # paquet perdu
            retard = 0.002 * math.exp(rng.gauss(0, 0.8))
            boucle.call_later(retard, lambda: arrivees.append(
                time.monotonic() - debut))
    transport, _ = await boucle.create_datagram_endpoint(
        Relais, local_addr=("127.0.0.1", PORT))
    p = subprocess.Popen([sys.executable, "-c", EMETTEUR])
    try:
        while (t := time.monotonic() - debut) < DUREE:
            attente = rng.expovariate(1 / 25)
            if t + attente + 3 > DUREE:
                await asyncio.sleep(DUREE - t)
                break
            await asyncio.sleep(attente)
            d = rng.choice([0.25, 0.5, 1.0, 2.0])
            pauses.append((time.monotonic() - debut, d))
            os.kill(p.pid, signal.SIGSTOP)
            await asyncio.sleep(d)
            os.kill(p.pid, signal.SIGCONT)
            if p.poll() is not None:
                raise RuntimeError("l'émetteur s'est arrêté")
    finally:
        p.kill()
        p.wait()
        transport.close()
    return arrivees, pauses


class Fixe:
    def __init__(self, delai):
        self.delai = delai

    def seuil(self, moyenne, ecart):
        return self.delai


class PhiNormal:
    """Hayashibara et al. : loi normale des intervalles (comme Akka)."""

    def __init__(self, phi, ecart_min=0.01):
        self.z = NormalDist().inv_cdf(1 - 10 ** -phi)
        self.ecart_min = ecart_min

    def seuil(self, moyenne, ecart):
        return moyenne + self.z * max(ecart, self.ecart_min)


class PhiExp:
    """Cassandra : loi exponentielle, phi = t / (moyenne × ln 10)."""

    def __init__(self, phi):
        self.phi = phi

    def seuil(self, moyenne, ecart):
        return self.phi * math.log(10) * moyenne


DETECTEURS = {
    "délai fixe 250 ms": Fixe(0.25), "délai fixe 500 ms": Fixe(0.5),
    "délai fixe 1 s": Fixe(1.0), "délai fixe 2 s": Fixe(2.0),
    "délai fixe 3 s": Fixe(3.0),
    "phi normal, seuil 3": PhiNormal(3), "phi normal, seuil 8": PhiNormal(8),
    "phi normal, seuil 16": PhiNormal(16),
    "phi exponentiel, seuil 8": PhiExp(8),
}
FENETRE = 1000                              # intervalles retenus (Akka : 1 000)


def evaluer(arrivees, det, graine):
    """Fausses suspicions sur la trace, puis temps de détection de pannes
    placées au hasard (la panne arrête les battements à l'instant t)."""
    inter = [b - a for a, b in zip(arrivees, arrivees[1:])]
    fausses, temps_faux, seuils = 0, 0.0, {}
    somme = carres = 0.0
    for i in range(1, len(arrivees) - 1):
        # fenêtre : les FENETRE derniers intervalles avant le battement i
        x = inter[i - 1]
        somme, carres = somme + x, carres + x * x
        if i > FENETRE:
            y = inter[i - 1 - FENETRE]
            somme, carres = somme - y, carres - y * y
        if i < 20:
            continue
        n = min(i, FENETRE)
        m = somme / n
        e = math.sqrt(max(carres / n - m * m, 0.0))
        s = det.seuil(m, e)
        seuils[i] = s
        if inter[i] > s:
            fausses += 1
            temps_faux += inter[i] - s
    rng = random.Random(graine)
    detections = []
    for _ in range(2000):
        i = rng.randrange(20, len(arrivees) - 1)
        panne = rng.uniform(arrivees[i], arrivees[i + 1])
        detections.append(arrivees[i] + seuils[i] - panne)
    detections.sort()
    return fausses, temps_faux, statistics.median(detections), detections[-20]


def main():
    graine = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    arrivees, pauses = asyncio.run(enregistrer(graine))
    trous = sorted((b - a for a, b in zip(arrivees, arrivees[1:])),
                   reverse=True)
    print(f"trace {graine} : {len(arrivees)} battements reçus en {DUREE} s, "
          f"un toutes les {INTERVALLE * 1000:.0f} ms")
    total = sum(d for _, d in pauses)
    print(f"   {len(pauses)} pauses de l'émetteur, {total:g} s en tout : "
          + " ".join(f"{d:g}" for _, d in pauses))
    inter = [b - a for a, b in zip(arrivees, arrivees[1:])]
    q = statistics.quantiles(inter, n=1000)
    print(f"   intervalle médian {statistics.median(inter) * 1000:.1f} ms, "
          f"99,9e centile {q[998] * 1000:.0f} ms ;")
    print(f"   plus grands trous {trous[0]:.2f}, {trous[1]:.2f}, "
          f"{trous[2]:.2f} s")
    print(f"\n   {'détecteur':26} {'fausses':>7} {'à tort':>7} "
          f"{'détection méd.':>14} {'99e c.':>7}")
    for nom, det in DETECTEURS.items():
        f, tf, med, p99 = evaluer(arrivees, det, graine)
        print(f"   {nom:26} {f:7} {tf:6.1f}s {med * 1000:11.0f} ms "
              f"{p99 * 1000:5.0f} ms")


if __name__ == "__main__":
    main()
