"""Relais TCP qui retarde chaque bloc de données de 25 ms dans chaque sens,
soit 50 ms ajoutées à chaque aller-retour applicatif (poignée de main TLS,
requête et réponse). La poignée de main TCP entre le client et le relais,
elle, n'est pas retardée : elle reste locale.

Usage : python3 relais_latence.py PORT_ECOUTE PORT_CIBLE [DELAI_MS]
"""
import asyncio
import sys

ECOUTE, CIBLE = int(sys.argv[1]), int(sys.argv[2])
DELAI = (int(sys.argv[3]) if len(sys.argv) > 3 else 25) / 1000


async def acheminer(lecteur, ecrivain):
    file = asyncio.Queue()

    async def recevoir():
        boucle = asyncio.get_running_loop()
        while data := await lecteur.read(65536):
            await file.put((boucle.time() + DELAI, data))
        await file.put((boucle.time() + DELAI, b""))

    async def emettre():
        boucle = asyncio.get_running_loop()
        while True:
            echeance, data = await file.get()
            await asyncio.sleep(max(0, echeance - boucle.time()))
            if not data:
                ecrivain.close()
                return
            ecrivain.write(data)
            await ecrivain.drain()

    await asyncio.gather(recevoir(), emettre())


async def client(lecteur_c, ecrivain_c):
    lecteur_s, ecrivain_s = await asyncio.open_connection("127.0.0.1", CIBLE)
    await asyncio.gather(acheminer(lecteur_c, ecrivain_s), acheminer(lecteur_s, ecrivain_c),
                         return_exceptions=True)


async def main():
    serveur = await asyncio.start_server(client, "127.0.0.1", ECOUTE)
    async with serveur:
        await serveur.serve_forever()


asyncio.run(main())
