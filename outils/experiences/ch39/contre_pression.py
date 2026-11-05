"""Chapitre 39 : contre-pression (back-pressure) entre un producteur et un consommateur.

1. Dans un processus : un producteur veut émettre 2 000 messages par seconde,
   un consommateur en traite 1 000 par seconde, pendant 20 s. Trois façons de
   relier les deux par une file asyncio :
     illimitée    le producteur dépose sans jamais attendre ;
     contre-pr.   file de 1 000 places, le producteur attend une place libre ;
     délestage    file de 1 000 places, un message qui ne trouve pas de place
                  est jeté.
   On mesure la longueur maximale de la file, la mémoire qu'elle occupe et
   l'âge des messages quand le consommateur les prend.

2. Sur une connexion TCP locale : un lecteur lit 1 Mo par seconde, un écrivain
   écrit 64 Kio à la fois, soit en attendant que le tampon d'envoi se vide
   (await drain()), soit sans attendre. On mesure les octets écrits, lus, et
   ceux qui attendent dans les tampons du noyau et du processus.

    python3 contre_pression.py
"""
import asyncio
import socket
import statistics
import time
import tracemalloc

DUREE = 20
PORT = 11920


async def pipeline(mode):
    file = asyncio.Queue() if mode == "illimitée" else asyncio.Queue(maxsize=1000)
    produits = jetes = consommes = 0
    longueur_max = 0
    ages = []
    fin = time.monotonic() + DUREE

    async def producteur():
        nonlocal produits, jetes, longueur_max
        prochain = time.monotonic()
        while (t := time.monotonic()) < fin:
            for _ in range(20):                    # 20 messages toutes les 10 ms
                message = (t, bytearray(1000))     # 1 000 octets de charge utile
                if mode == "contre-pr.":
                    try:
                        await asyncio.wait_for(file.put(message), max(0.001, fin - time.monotonic()))
                    except TimeoutError:
                        return
                    produits += 1
                elif mode == "délestage" and file.full():
                    jetes += 1
                else:
                    file.put_nowait(message)
                    produits += 1
            longueur_max = max(longueur_max, file.qsize())
            prochain += 0.01
            await asyncio.sleep(max(0, prochain - time.monotonic()))

    async def consommateur():
        nonlocal consommes
        prochain = time.monotonic()
        while time.monotonic() < fin:
            for _ in range(10):                    # 10 messages toutes les 10 ms
                try:
                    t, _ = file.get_nowait()
                except asyncio.QueueEmpty:
                    break
                consommes += 1
                ages.append(time.monotonic() - t)
            prochain += 0.01
            await asyncio.sleep(max(0, prochain - time.monotonic()))

    tracemalloc.start()
    await asyncio.gather(producteur(), consommateur())
    _, pic = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    ages.sort()
    print(f"   {mode:<11}{produits:>9}{jetes:>7}{consommes:>10}{longueur_max:>9}{pic / 1e6:>6.1f}"
          f"{statistics.median(ages):>8.2f}s{ages[-1]:>7.2f}s")


async def tcp(attendre):
    lus = 0
    pret = asyncio.Event()

    async def lecteur(r, w):
        nonlocal lus
        pret.set()
        prochain = time.monotonic()
        while True:
            data = await r.read(10_000)            # 10 ko toutes les 10 ms = 1 Mo/s
            if not data:
                break
            lus += len(data)
            prochain += 0.01
            await asyncio.sleep(max(0, prochain - time.monotonic()))
        w.close()

    serveur = await asyncio.start_server(lecteur, "127.0.0.1", PORT)
    r, w = await asyncio.open_connection("127.0.0.1", PORT)
    sock = w.get_extra_info("socket")
    sndbuf = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    await pret.wait()
    ecrits, bloc = 0, bytes(64 * 1024)
    t0 = time.monotonic()
    releves = []
    while (t := time.monotonic() - t0) < 10:
        w.write(bloc)
        ecrits += len(bloc)
        if attendre:
            await w.drain()
        else:
            await asyncio.sleep(0.005)             # environ 13 Mo/s, laisse tourner le lecteur
        if not releves or t - releves[-1][0] >= 2:
            releves.append((t, ecrits, lus, w.transport.get_write_buffer_size()))
    releves.append((time.monotonic() - t0, ecrits, lus, w.transport.get_write_buffer_size()))
    nom = "await drain()" if attendre else "sans attendre"
    print(f"   {nom}  (SO_SNDBUF de l'écrivain : {sndbuf // 1024} Kio)")
    print(f"     {'t (s)':>6}{'écrits (Mo)':>13}{'lus (Mo)':>10}{'en attente (Mo)':>17}{'dont processus':>16}")
    for t, e, l, b in releves:
        print(f"     {t:>6.1f}{e / 1e6:>13.1f}{l / 1e6:>10.1f}{(e - l) / 1e6:>17.1f}{b / 1e6:>16.1f}")
    w.transport.abort()
    serveur.close()


async def main():
    print(f"1. producteur 2 000 messages/s, consommateur 1 000/s, {DUREE} s, messages de 1 ko")
    print(f"   {'mode':<11}{'produits':>9}{'jetés':>7}{'consommés':>10}{'file max':>9}"
          f"{'Mo':>6}{'âge méd.':>9}{'âge max':>8}")
    for mode in ("illimitée", "contre-pr.", "délestage"):
        await pipeline(mode)
    print("\n2. TCP local : lecteur à 1 Mo/s, écrivain par blocs de 64 Kio, 10 s")
    for attendre in (True, False):
        await tcp(attendre)


if __name__ == "__main__":
    asyncio.run(main())
