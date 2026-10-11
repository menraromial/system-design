"""Chapitre 4 : une connexion TCP par requête contre une connexion réutilisée.

Un serveur et un client tournent sur la même machine, sur une adresse qui
n'est pas une adresse de boucle locale (sinon Linux réutilise les ports en
TIME_WAIT, réglage net.ipv4.tcp_tw_reuse = 2, et l'épuisement n'apparaît pas).
Le client ferme la connexion le premier : c'est donc lui qui garde chaque
connexion fermée en TIME_WAIT pendant 60 secondes, avec son port éphémère.

Usage : python3 ch04_time_wait.py 192.168.122.1 [nombre de requêtes]
"""
import errno
import socket
import subprocess
import sys
import threading
import time

ADRESSE = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 40_000


def serveur(ecoute):
    while True:
        conn, _ = ecoute.accept()
        with conn:
            while True:  # répond « y » à chaque « x », jusqu'à la fermeture par le client
                if not conn.recv(1):
                    break
                conn.sendall(b"y")


def time_wait():
    sortie = subprocess.run(["ss", "-Htan", "state", "time-wait", "dst", ADRESSE],
                            capture_output=True, text=True).stdout
    return len(sortie.splitlines())


def une_connexion_par_requete(port):
    debut, faites = time.perf_counter(), 0
    for _ in range(N):
        s = socket.socket()
        try:
            s.connect((ADRESSE, port))
        except OSError as e:
            s.close()
            duree = time.perf_counter() - debut
            nom = errno.errorcode.get(e.errno, e.errno)
            return faites, duree, f"échec à la requête {faites + 1} : {nom} ({e.strerror})"
        s.sendall(b"x")
        s.recv(1)
        s.close()  # le client ferme en premier : TIME_WAIT de son côté
        faites += 1
    return faites, time.perf_counter() - debut, "aucune erreur"


def connexion_reutilisee(port):
    debut = time.perf_counter()
    with socket.create_connection((ADRESSE, port)) as s:
        for _ in range(N):
            s.sendall(b"x")
            s.recv(1)
    return N, time.perf_counter() - debut


def main():
    ecoute = socket.socket()
    ecoute.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    ecoute.bind((ADRESSE, 0))
    ecoute.listen(4096)
    port = ecoute.getsockname()[1]
    threading.Thread(target=serveur, args=(ecoute,), daemon=True).start()

    with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
        bas, haut = map(int, f.read().split())
    print(f"plage éphémère : {bas}-{haut}, soit {haut - bas + 1} ports")
    print(f"TIME_WAIT vers {ADRESSE} avant : {time_wait()}")

    faites, duree = connexion_reutilisee(port)
    print(f"connexion réutilisée : {faites} requêtes en {duree:.2f} s "
          f"({faites / duree:,.0f} par seconde, {duree / faites * 1e6:.0f} µs par requête)")

    faites, duree, bilan = une_connexion_par_requete(port)
    print(f"une connexion par requête : {faites} requêtes en {duree:.2f} s "
          f"({faites / duree:,.0f} par seconde, {duree / faites * 1e6:.0f} µs par requête) ; {bilan}")
    print(f"TIME_WAIT vers {ADRESSE} juste après : {time_wait()}")
    time.sleep(62)
    print(f"TIME_WAIT vers {ADRESSE} 62 s plus tard : {time_wait()}")


if __name__ == "__main__":
    main()
