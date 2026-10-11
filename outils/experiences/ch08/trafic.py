"""Chapitre 8 : octets échangés, requêtes et délai de livraison, pendant DUREE
secondes, pour quatre clients du serveur de serveur.py (une mise à jour toutes
les 10 s) : polling chaque seconde, long polling, SSE, WebSocket.

Les clients parlent HTTP directement sur des sockets pour compter exactement
les octets applicatifs (en-têtes HTTP compris, en-têtes TCP/IP non compris).
Usage : python3 trafic.py PORT [DUREE]
"""
import asyncio
import base64
import json
import os
import struct
import sys
import time

PORT = int(sys.argv[1])
DUREE = float(sys.argv[2]) if len(sys.argv) > 2 else 65
ENTETES = ("Host: 127.0.0.1\r\nUser-Agent: Mozilla/5.0 (X11; Linux x86_64) Firefox/140.0\r\n"
           "Accept: application/json\r\nAccept-Language: fr-FR,fr;q=0.9\r\n")


class Compteur:
    def __init__(self, nom):
        self.nom, self.envoye, self.recu, self.requetes, self.delais, self.vues = nom, 0, 0, 0, [], set()

    def livraison(self, texte):
        m = json.loads(texte)
        if m["version"] > 0 and m["version"] not in self.vues:
            self.vues.add(m["version"])
            self.delais.append((time.time() - m["emis_a"]) * 1000)
        return m["version"]


async def connexion(c):
    r, w = await asyncio.open_connection("127.0.0.1", PORT)
    return r, w


async def envoyer(c, w, data):
    w.write(data)
    c.envoye += len(data)
    await w.drain()


async def lire_reponse(c, r):
    tete = await r.readuntil(b"\r\n\r\n")
    c.recu += len(tete)
    longueur = 0
    for ligne in tete.decode().split("\r\n"):
        if ligne.lower().startswith("content-length:"):
            longueur = int(ligne.split(":")[1])
    corps = await r.readexactly(longueur) if longueur else b""
    c.recu += len(corps)
    return tete.split(b" ")[1], corps


async def polling(c):
    r, w = await connexion(c)
    while True:
        await envoyer(c, w, f"GET /etat HTTP/1.1\r\n{ENTETES}\r\n".encode())
        c.requetes += 1
        _, corps = await lire_reponse(c, r)
        c.livraison(corps)
        await asyncio.sleep(1)


async def long_polling(c):
    r, w = await connexion(c)
    version = 0
    while True:
        await envoyer(c, w, f"GET /attendre?apres={version} HTTP/1.1\r\n{ENTETES}\r\n".encode())
        c.requetes += 1
        statut, corps = await lire_reponse(c, r)
        if statut == b"200":
            version = c.livraison(corps)


async def sse(c):
    r, w = await connexion(c)
    await envoyer(c, w, f"GET /flux HTTP/1.1\r\n{ENTETES}Accept: text/event-stream\r\n\r\n".encode())
    c.requetes += 1
    tete = await r.readuntil(b"\r\n\r\n")
    c.recu += len(tete)
    while True:
        ligne = await r.readline()
        c.recu += len(ligne)
        if b"data: " in ligne:
            c.livraison(ligne.split(b"data: ", 1)[1])


async def websocket(c):
    r, w = await connexion(c)
    cle = base64.b64encode(os.urandom(16)).decode()
    await envoyer(c, w, (f"GET /ws HTTP/1.1\r\n{ENTETES}Upgrade: websocket\r\nConnection: Upgrade\r\n"
                         f"Sec-WebSocket-Key: {cle}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
    c.requetes += 1
    c.recu += len(await r.readuntil(b"\r\n\r\n"))  # 101 Switching Protocols
    while True:
        b1, b2 = await r.readexactly(2)
        longueur, extra = b2 & 0x7F, b""
        if longueur == 126:
            extra = await r.readexactly(2)
            longueur = struct.unpack("!H", extra)[0]
        donnees = await r.readexactly(longueur)
        c.recu += 2 + len(extra) + longueur
        if b1 & 0x0F == 0x1:  # trame texte
            c.livraison(donnees)
        elif b1 & 0x0F == 0x9:  # ping du serveur : pong masqué, comme l'exige la RFC 6455
            masque = os.urandom(4)
            await envoyer(c, w, bytes([0x8A, 0x80 | len(donnees)]) + masque +
                          bytes(x ^ masque[i % 4] for i, x in enumerate(donnees)))


async def main():
    clients = {"polling (1 s)": polling, "long polling": long_polling, "SSE": sse, "WebSocket": websocket}
    compteurs = {nom: Compteur(nom) for nom in clients}
    taches = [asyncio.create_task(f(compteurs[n])) for n, f in clients.items()]
    await asyncio.sleep(DUREE)
    for t in taches:
        t.cancel()
    print(f"{DUREE:.0f} s, une mise à jour toutes les 10 s ; délais en ms")
    print(f"{'client':14} {'requêtes':>8} {'envoyés':>8} {'reçus':>7} {'reçues':>6} "
          f"{'moyen':>6} {'max':>6}")
    for c in compteurs.values():
        moy = sum(c.delais) / len(c.delais) if c.delais else float("nan")
        print(f"{c.nom:14} {c.requetes:8d} {c.envoye:8d} {c.recu:7d} {len(c.delais):6d} "
              f"{moy:6.1f} {max(c.delais, default=float('nan')):6.1f}")


asyncio.run(main())
