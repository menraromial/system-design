"""Chapitre 27 : un pic d'inscriptions et un service d'e-mail qui ne suit pas.

Un faux service d'e-mail (serveur HTTP sur le port 10780) envoie au plus 50
messages par seconde : 4 envois à la fois, 80 ms chacun. Un service
d'inscription enregistre l'utilisateur (2 ms) puis demande l'e-mail de
bienvenue. Pic : 2 000 inscriptions en 4 secondes (500 par seconde), dix fois
ce que le service d'e-mail peut envoyer.
  1. appel direct : l'inscription appelle le service d'e-mail et attend sa
     réponse, au plus 2 s ; au-delà, elle échoue ;
  2. par une file : l'inscription dépose un message durable dans une file
     RabbitMQ et répond ; un consommateur lit la file, 4 messages à la fois
     au plus, et appelle le service d'e-mail à son rythme.
Pour chacune : inscriptions réussies et échouées, latence vue par
l'utilisateur, e-mails envoyés et quand le dernier part ; pour la file, sa
profondeur maximale.
Prérequis : docker, image rabbitmq:4-management, pip install -r requirements.txt.
Usage : python3 pic.py
"""
import asyncio
import statistics
import time

import aio_pika
import aiohttp
from aiohttp import web

import commun

INSCRIPTIONS, DUREE_PIC, PORT = 2000, 4.0, 10780


class ServiceEmail:
    def __init__(self):
        self.parallele = asyncio.Semaphore(4)
        self.envois = []

    async def envoyer(self, requete):
        async with self.parallele:
            await asyncio.sleep(0.08)
            self.envois.append(time.monotonic())
        return web.json_response({"ok": True})

    async def demarrer(self):
        app = web.Application()
        app.router.add_post("/envoyer", self.envoyer)
        self.runner = web.AppRunner(app, handler_cancellation=False)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", PORT).start()

    async def arreter(self):
        await self.runner.cleanup()


def centiles(durees):
    durees = sorted(durees)
    return statistics.median(durees), durees[int(0.99 * (len(durees) - 1))]


async def attendre_fin_envois(service, attendus, delai=120):
    fin = time.monotonic() + delai
    while len(service.envois) < attendus and time.monotonic() < fin:
        await asyncio.sleep(0.1)


async def direct():
    service = ServiceEmail()
    await service.demarrer()
    reussies, echouees = [], 0
    debut = time.monotonic()
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as http:
        async def inscrire(i):
            nonlocal echouees
            await asyncio.sleep(i * DUREE_PIC / INSCRIPTIONS)
            t = time.monotonic()
            await asyncio.sleep(0.002)                                  # enregistrement
            try:
                async with http.post(f"http://127.0.0.1:{PORT}/envoyer", json={"a": i},
                                     timeout=aiohttp.ClientTimeout(total=2)) as r:
                    await r.read()
                reussies.append(1000 * (time.monotonic() - t))
            except (asyncio.TimeoutError, aiohttp.ClientError):
                echouees += 1
        await asyncio.gather(*(inscrire(i) for i in range(INSCRIPTIONS)))
        fin_inscriptions = time.monotonic() - debut
        await attendre_fin_envois(service, INSCRIPTIONS)
    med, p99 = centiles(reussies)
    print("1. appel direct au service d'e-mail (délai d'attente 2 s)")
    print(f"   inscriptions réussies {len(reussies)}, échouées {echouees}")
    print(f"   latence des réussies : médiane {med:.0f} ms, p99 {p99:.0f} ms")
    print(f"   e-mails envoyés {len(service.envois)}, dont {len(service.envois) - len(reussies)}"
          f" pour des inscriptions échouées")
    print(f"   dernier e-mail {service.envois[-1] - debut:.0f} s après le début du pic"
          f" (dernière inscription à {fin_inscriptions:.0f} s)")
    await service.arreter()


async def par_file():
    service = ServiceEmail()
    await service.demarrer()
    connexion = await aio_pika.connect_robust(commun.RABBIT)
    canal = await connexion.channel()                       # accusés de publication (confirms) par défaut
    file = await canal.declare_queue("courriels", durable=True)
    latences, profondeur_max, compte = [], [0], {"publies": 0, "acquittes": 0}
    connexions = []
    debut = time.monotonic()

    async def consommateur():
        cx = await aio_pika.connect_robust(commun.RABBIT)
        connexions.append(cx)
        ch = await cx.channel()
        await ch.set_qos(prefetch_count=4)
        q = await ch.declare_queue("courriels", durable=True)
        async with aiohttp.ClientSession() as http:
            async with q.iterator() as messages:
                async for m in messages:
                    async with http.post(f"http://127.0.0.1:{PORT}/envoyer", data=m.body) as r:
                        await r.read()
                    await m.ack()
                    compte["acquittes"] += 1

    async def observer():
        while True:                       # messages publiés et pas encore traités
            profondeur_max[0] = max(profondeur_max[0], compte["publies"] - compte["acquittes"])
            await asyncio.sleep(0.05)

    taches = [asyncio.create_task(consommateur()) for _ in range(4)] + [asyncio.create_task(observer())]

    async def inscrire(i):
        await asyncio.sleep(i * DUREE_PIC / INSCRIPTIONS)
        t = time.monotonic()
        await asyncio.sleep(0.002)
        await canal.default_exchange.publish(
            aio_pika.Message(b'{"a": %d}' % i, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="courriels")
        compte["publies"] += 1
        latences.append(1000 * (time.monotonic() - t))

    await asyncio.gather(*(inscrire(i) for i in range(INSCRIPTIONS)))
    fin_inscriptions = time.monotonic() - debut
    await attendre_fin_envois(service, INSCRIPTIONS)
    for t in taches:
        t.cancel()
    await asyncio.gather(*taches, return_exceptions=True)
    for cx in connexions:
        await cx.close()
    med, p99 = centiles(latences)
    print("\n2. par une file RabbitMQ (message durable, accusé de publication)")
    print(f"   inscriptions réussies {len(latences)}, échouées {INSCRIPTIONS - len(latences)}")
    print(f"   latence : médiane {med:.1f} ms, p99 {p99:.1f} ms")
    print(f"   messages en attente au plus fort : {profondeur_max[0]}")
    print(f"   e-mails envoyés {len(service.envois)}")
    print(f"   dernier e-mail {service.envois[-1] - debut:.0f} s après le début du pic"
          f" (dernière inscription à {fin_inscriptions:.0f} s)")
    await file.delete(if_unused=False, if_empty=False)
    await connexion.close()
    await service.arreter()


async def principal():
    print(f"{INSCRIPTIONS} inscriptions en {DUREE_PIC:.0f} s ; service d'e-mail limité à 50 envois/s\n")
    await direct()
    await par_file()


def main():
    try:
        version = commun.lancer_rabbitmq()
        print(f"RabbitMQ {version}")
        asyncio.run(principal())
    finally:
        commun.supprimer("rabbitmq")


if __name__ == "__main__":
    main()
