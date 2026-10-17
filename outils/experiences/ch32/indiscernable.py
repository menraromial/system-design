"""Chapitre 32 : ce que voit un client, et ce qui s'est vraiment passé.

Un client envoie un virement avec un délai d'attente de 1 s, dans sept
situations : tout va bien, processus arrêté, machine injoignable, requête
perdue, réponse perdue, serveur lent (3 s), serveur en pause (SIGSTOP 2 s,
comme une longue pause du ramasse-miettes). Après chaque essai, on relit le
compteur du serveur pour savoir si le virement a été exécuté.

Le serveur (serveur.py, port 11232) tourne dans un sous-processus ; un relais
TCP (port 11233) perd au choix la requête ou la réponse.

    python3 indiscernable.py
"""
import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import aiohttp

SERVEUR, RELAIS = 11232, 11233
ICI = Path(__file__).parent
MACHINE_ABSENTE = "10.255.255.1"            # adresse privée sans machine
mode = {"perdre": None}                     # None, "requete" ou "reponse"


async def relais(lecteur, ecrivain):
    try:
        amont_l, amont_e = await asyncio.open_connection("127.0.0.1", SERVEUR)
    except OSError:
        ecrivain.close()
        return

    async def copier(src, dst, sens):
        try:
            while data := await src.read(65536):
                if mode["perdre"] != sens:
                    dst.write(data)
                    await dst.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            dst.close()
    await asyncio.gather(copier(lecteur, amont_e, "requete"),
                         copier(amont_l, ecrivain, "reponse"))


class Serveur:
    def __init__(self):
        self.p = None

    async def demarrer(self):
        self.p = subprocess.Popen([sys.executable, str(ICI / "serveur.py"),
                                   "--port", str(SERVEUR)])
        async with aiohttp.ClientSession() as s:
            for _ in range(100):
                try:
                    async with s.get(f"http://127.0.0.1:{SERVEUR}/compte"):
                        return
                except aiohttp.ClientError:
                    await asyncio.sleep(0.05)

    def arreter(self):
        if self.p:
            self.p.kill()
            self.p.wait()
            self.p = None

    async def compte(self):
        async with aiohttp.ClientSession() as s:
            async with s.get(f"http://127.0.0.1:{SERVEUR}/compte") as r:
                return (await r.json())["executes"]


async def essai(url):
    debut = time.monotonic()
    try:
        async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=1)) as s:
            async with s.post(url) as r:
                await r.read()
                vu = f"réponse {r.status}"
    except asyncio.TimeoutError:
        vu = "délai dépassé"
    except aiohttp.ClientConnectorError as e:
        vu = f"connexion refusée" if isinstance(
            e.os_error, ConnectionRefusedError) else f"erreur ({e.os_error})"
    except aiohttp.ClientError as e:
        vu = f"erreur {type(e).__name__}"
    return vu, (time.monotonic() - debut) * 1000


async def main():
    srv = Serveur()
    await srv.demarrer()
    relai = await asyncio.start_server(relais, "127.0.0.1", RELAIS)
    via_relais = f"http://127.0.0.1:{RELAIS}/virement"
    direct = f"http://127.0.0.1:{SERVEUR}/virement"
    print("virement envoyé avec un délai d'attente de 1 s")
    print(f"   {'situation':26} {'vu par le client':22} {'après':>7}  exécuté ?")

    async def scenario(nom, url, avant=None, apres=None, attente=0.0):
        if srv.p is None:
            await srv.demarrer()
        n0 = await srv.compte()
        if avant:
            avant()
        vu, ms = await essai(url)
        if apres:
            apres()
        await asyncio.sleep(attente)
        if srv.p is None:
            await srv.demarrer()
            fait = "non (redémarré)"
        else:
            fait = "oui" if await srv.compte() > n0 else "non"
        mode["perdre"] = None
        print(f"   {nom:26} {vu:22} {ms:5.0f} ms  {fait}")

    await scenario("tout va bien", via_relais)
    await scenario("processus arrêté", direct, avant=srv.arreter)
    await scenario("machine injoignable",
                   f"http://{MACHINE_ABSENTE}:{SERVEUR}/virement")
    await scenario("requête perdue", via_relais,
                   avant=lambda: mode.update(perdre="requete"))
    await scenario("réponse perdue", via_relais,
                   avant=lambda: mode.update(perdre="reponse"), attente=0.2)
    await scenario("serveur lent (3 s)", via_relais + "?lent=3", attente=2.5)

    def pause():
        os.kill(srv.p.pid, signal.SIGSTOP)

    def reprise():
        time.sleep(1.0)                     # la pause dure 2 s en tout
        os.kill(srv.p.pid, signal.SIGCONT)
    await scenario("serveur en pause (2 s)", via_relais, avant=pause,
                   apres=reprise, attente=0.3)
    relai.close()
    srv.arreter()


if __name__ == "__main__":
    asyncio.run(main())
