"""Chapitre 14 : ce que coûte et rapporte la compression faite par le proxy.

Un catalogue JSON de 1 000 produits (services.py) derrière Caddy, avec
« encode zstd gzip ». Pour chaque codage demandé par le client (aucun, gzip,
zstd) : taille transmise, puis débit et temps processeur de Caddy par requête
sous la charge de wrk (16 connexions, 8 s).
Prérequis : caddy, wrk. Usage : python3 compresser.py
"""
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import urllib.request

ICI = pathlib.Path(__file__).parent
ORIGINE, PROXY = 9411, 9410
CADDY = f"""{{
	admin off
	auto_https off
}}
:{PROXY} {{
	encode zstd gzip
	reverse_proxy 127.0.0.1:{ORIGINE}
}}
"""


def attendre(url, delai=10):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            urllib.request.urlopen(url, timeout=0.5).read()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"{url} muet")


def cpu_s(pid):
    champs = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    return (int(champs[11]) + int(champs[12])) / os.sysconf("SC_CLK_TCK")


def taille(codage):
    req = urllib.request.Request(f"http://127.0.0.1:{PROXY}/catalogue")
    if codage:
        req.add_header("Accept-Encoding", codage)
    with urllib.request.urlopen(req) as r:
        return len(r.read()), r.headers.get("Content-Encoding", "aucun")


def main():
    origine = subprocess.Popen([sys.executable, str(ICI / "services.py"), "origine", str(ORIGINE)])
    f = tempfile.NamedTemporaryFile("w", suffix=".caddyfile", delete=False)
    f.write(CADDY)
    f.close()
    caddy = subprocess.Popen(["caddy", "run", "--adapter", "caddyfile", "--config", f.name],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        attendre(f"http://127.0.0.1:{PROXY}/catalogue")
        print("catalogue JSON de 1 000 produits, servi par Caddy avec encode zstd gzip")
        print(f"{'demandé':<10}{'reçu':<8}{'octets':>9}{'rapport':>9}{'req/s':>8}{'CPU Caddy':>16}")
        brut = None
        for codage in ("", "gzip", "zstd"):
            octets, recu = taille(codage)
            brut = brut or octets
            avant = cpu_s(caddy.pid)
            args = ["wrk", "-t2", "-c16", "-d8s"]
            if codage:
                args += ["-H", f"Accept-Encoding: {codage}"]
            sortie = subprocess.run(args + [f"http://127.0.0.1:{PROXY}/catalogue"],
                                    capture_output=True, text=True).stdout
            cpu = cpu_s(caddy.pid) - avant
            n = int(re.search(r"(\d+) requests in", sortie).group(1))
            debit = float(re.search(r"Requests/sec:\s+([\d.]+)", sortie).group(1))
            print(f"{codage or 'rien':<10}{recu:<8}{octets:>9}{brut / octets:>8.1f}x{debit:>8.0f}"
                  f"{1000 * cpu / n:>10.2f} ms/req")
    finally:
        caddy.terminate()
        origine.terminate()
        caddy.wait()
        origine.wait()
        os.unlink(f.name)


if __name__ == "__main__":
    main()
