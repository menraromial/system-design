"""Chapitre 8 : mémoire d'un serveur qui garde N connexions ouvertes et
inactives, en WebSocket puis en SSE.

Lance un serveur.py neuf pour chaque mode (Python ne rend pas au système la
mémoire libérée, qu'une seconde mesure réutiliserait), mesure sa mémoire résidente (RSS)
et la mémoire des tampons TCP du noyau (/proc/net/sockstat) avant et après
l'ouverture des connexions.
Usage : python3 memoire.py [N]   (pip install -r requirements.txt)
"""
import asyncio
import pathlib
import subprocess
import sys
import time

import websockets

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000
PORT = 9802
PAGE = 4096


def rss_kio(pid):
    for ligne in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
        if ligne.startswith("VmRSS:"):
            return int(ligne.split()[1])


def tampons_tcp_kio():
    for ligne in pathlib.Path("/proc/net/sockstat").read_text().splitlines():
        if ligne.startswith("TCP:"):
            champs = ligne.split()
            return int(champs[champs.index("mem") + 1]) * PAGE // 1024


async def ouvrir_ws():
    return await asyncio.gather(*(websockets.connect(f"ws://127.0.0.1:{PORT}/ws", ping_interval=None)
                                  for _ in range(N)))


async def ouvrir_sse():
    flux = []
    for _ in range(N):
        r, w = await asyncio.open_connection("127.0.0.1", PORT)
        w.write(b"GET /flux HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n")
        flux.append((r, w))
    for r, _ in flux:
        await r.readuntil(b"retry: 3000\n\n")
    return flux


async def mesurer(pid, nom, ouvrir, fermer):
    avant, noyau_avant = rss_kio(pid), tampons_tcp_kio()
    t = time.perf_counter()
    conns = await ouvrir()
    ouverture = time.perf_counter() - t
    await asyncio.sleep(3)
    apres, noyau_apres = rss_kio(pid), tampons_tcp_kio()
    print(f"{nom:10} {N} connexions ouvertes en {ouverture:.1f} s ; serveur : "
          f"{avant / 1024:.0f} → {apres / 1024:.0f} Mio, soit {(apres - avant) * 1024 / N / 1024:.1f} Kio "
          f"par connexion ; tampons TCP du noyau : {noyau_avant / 1024:.0f} → {noyau_apres / 1024:.0f} Mio")
    await fermer(conns)
    await asyncio.sleep(3)


async def main():
    async def fermer_ws(conns):
        await asyncio.gather(*(c.close() for c in conns))

    async def fermer_sse(conns):
        for _, w in conns:
            w.close()

    for nom, ouvrir, fermer in (("WebSocket", ouvrir_ws, fermer_ws), ("SSE", ouvrir_sse, fermer_sse)):
        serveur = subprocess.Popen([sys.executable, str(pathlib.Path(__file__).parent / "serveur.py"),
                                    str(PORT), "3600"], stderr=subprocess.DEVNULL)
        await asyncio.sleep(2)
        try:
            print(f"serveur neuf au repos : {rss_kio(serveur.pid) / 1024:.0f} Mio")
            await mesurer(serveur.pid, nom, ouvrir, fermer)
        finally:
            serveur.terminate()
            serveur.wait()
            await asyncio.sleep(1)


asyncio.run(main())
