"""Chapitre 14 : un proxy qui met les réponses en tampon protège l'origine des
clients lents.

L'origine (origine.py) a 4 fils de travail. Quatre clients lents téléchargent
chacun une réponse de 64 Mio à 640 Kio/s ; une seconde plus tard, un client
rapide demande trois fois une petite réponse. Trois montages :
  1. les clients parlent directement à l'origine ;
  2. Caddy en reverse proxy, réglage par défaut (la réponse passe au fil de l'eau) ;
  3. Caddy avec response_buffers 128MiB (il lit toute la réponse, puis la renvoie).
Mesures : latence des petites requêtes, temps au bout duquel les fils de
l'origine sont libérés (médiane), mémoire résidente de Caddy.
Prérequis : caddy. Usage : python3 tampon.py
"""
import http.client
import json
import pathlib
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

ICI = pathlib.Path(__file__).parent
ORIGINE, PROXY = 9401, 9400
CADDY = """{{
	admin off
	auto_https off
}}
:{proxy} {{
	reverse_proxy 127.0.0.1:{origine} {{
		{option}
	}}
}}
"""
MESURE = 10.0          # durée maximale d'attente d'une petite requête (s)


def attendre(port, chemin="/petit", delai=10):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}{chemin}", timeout=0.5).read()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} muet")


def client_lent(port, arret):
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    s.sendall(b"GET /gros HTTP/1.1\r\nHost: test\r\n\r\n")
    s.settimeout(1)
    while not arret.is_set():
        try:
            if not s.recv(65536):
                break
        except socket.timeout:
            pass
        time.sleep(0.1)                       # 64 Kio toutes les 100 ms
    s.close()


def petite_requete(port):
    t = time.monotonic()
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=MESURE)
    try:
        c.request("GET", "/petit")
        c.getresponse().read()
        return (time.monotonic() - t) * 1000
    except OSError:
        return None                           # pas de réponse dans le délai
    finally:
        c.close()


def rss_mio(pid):
    for ligne in open(f"/proc/{pid}/status"):
        if ligne.startswith("VmRSS:"):
            return int(ligne.split()[1]) / 1024
    return 0.0


def essai(nom, option):
    origine = subprocess.Popen([sys.executable, str(ICI / "origine.py"), str(ORIGINE)])
    attendre(ORIGINE)
    caddy, port = None, ORIGINE
    if option is not None:
        f = tempfile.NamedTemporaryFile("w", suffix=".caddyfile", delete=False)
        f.write(CADDY.format(proxy=PROXY, origine=ORIGINE, option=option))
        f.close()
        caddy = subprocess.Popen(["caddy", "run", "--adapter", "caddyfile", "--config", f.name],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        attendre(PROXY)
        port = PROXY
    arret = threading.Event()
    lents = [threading.Thread(target=client_lent, args=(port, arret)) for _ in range(4)]
    for t in lents:
        t.start()
    time.sleep(1)
    latences = [petite_requete(port) for _ in range(3)]
    envois = json.load(urllib.request.urlopen(f"http://127.0.0.1:{ORIGINE}/stats"))
    memoire = rss_mio(caddy.pid) if caddy else None
    arret.set()
    for t in lents:
        t.join()
    if caddy:
        caddy.terminate()
        caddy.wait()
    origine.terminate()
    origine.wait()
    # temps d'occupation des fils de l'origine par les quatre gros envois
    occupe = [(fin - debut) if fin is not None else None for debut, fin in envois[:4]]
    return latences, occupe, memoire


def texte_latence(v):
    return f"{v:.1f} ms" if v is not None else f"> {MESURE:.0f} s"


def main():
    print("origine à 4 fils ; 4 clients lents (64 Mio à 640 Kio/s) ; 3 petites requêtes")
    print(f"{'montage':<26}{'petites requêtes':>26}{'fils libérés après':>20}{'mémoire Caddy':>15}")
    for nom, option in (("direct", None),
                        ("Caddy, par défaut", ""),
                        ("Caddy, response_buffers", "response_buffers 128MiB")):
        latences, occupe, memoire = essai(nom, option)
        lat = " / ".join(texte_latence(v) for v in latences)
        finis = [v for v in occupe if v is not None]
        occ = (f"{statistics.median(finis) * 1000:.0f} ms" if len(finis) == len(occupe)
               else f"{len(occupe) - len(finis)} occupés")
        mem = f"{memoire:.0f} Mio" if memoire is not None else "-"
        print(f"{nom:<26}{lat:>26}{occ:>20}{mem:>15}")


if __name__ == "__main__":
    main()
