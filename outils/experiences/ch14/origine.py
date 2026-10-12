"""Chapitre 14 : une origine à fils de travail limités, comme un serveur
d'application classique (un fil par requête en cours, K fils au plus).

  GET /gros     envoie TAILLE octets (64 Mio par défaut) par blocs de 64 Kio
  GET /petit    une réponse de quelques octets
  GET /stats    début et fin de chaque envoi de /gros, en secondes (hors fils)

Le tampon d'envoi de chaque connexion est réduit à 64 Kio : l'écriture bloque
dès que le client (ou le proxy) ne lit plus, comme sur un vrai réseau chargé.
Usage : python3 origine.py PORT [K] [TAILLE_MIO]
"""
import json
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1])
K = int(sys.argv[2]) if len(sys.argv) > 2 else 4
TAILLE = (int(sys.argv[3]) if len(sys.argv) > 3 else 64) * 1024 * 1024
fils = threading.BoundedSemaphore(K)
envois = []                      # [début, fin ou None]
BLOC = b"x" * 65536
T0 = time.monotonic()


class Gestion(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        # en-tête et corps partent en deux écritures : sans TCP_NODELAY, l'algorithme
        # de Nagle et l'accusé de réception retardé ajoutent environ 40 ms (ch. 4)
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def log_message(self, *args):
        pass

    def entete(self, longueur, type_="text/plain"):
        self.send_response(200)
        self.send_header("Content-Type", type_)
        self.send_header("Content-Length", str(longueur))
        self.end_headers()

    def do_GET(self):
        if self.path == "/stats":
            corps = json.dumps(envois).encode()
            self.entete(len(corps), "application/json")
            self.wfile.write(corps)
            return
        with fils:                         # un fil de travail par requête
            if self.path == "/gros":
                envoi = [time.monotonic() - T0, None]
                envois.append(envoi)
                self.entete(TAILLE, "application/octet-stream")
                try:
                    for _ in range(TAILLE // len(BLOC)):
                        self.wfile.write(BLOC)
                finally:
                    envoi[1] = time.monotonic() - T0
            else:
                self.entete(2)
                self.wfile.write(b"ok")


class Serveur(ThreadingHTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass                              # client parti en cours de route : sans intérêt ici


if __name__ == "__main__":
    Serveur(("127.0.0.1", PORT), Gestion).serve_forever()
