"""Chapitre 14 : ce que coûte l'authentification déléguée par la passerelle.

Une petite API (services.py origine) derrière Caddy, sans contrôle d'accès puis
avec « forward_auth » vers un service d'authentification (services.py auth) :
chaque requête est d'abord recopiée vers ce service, qui l'accepte ou la refuse.
Mesures : réponse sans jeton, puis latence et débit sous la charge de wrk
(16 connexions, 8 s) avec un jeton valide.
Prérequis : caddy, wrk. Usage : python3 auth.py
"""
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ICI = pathlib.Path(__file__).parent
ORIGINE, AUTH, PROXY = 9421, 9422, 9420
SANS = f"""{{
	admin off
	auto_https off
}}
:{PROXY} {{
	reverse_proxy 127.0.0.1:{ORIGINE}
}}
"""
AVEC = f"""{{
	admin off
	auto_https off
}}
:{PROXY} {{
	forward_auth 127.0.0.1:{AUTH} {{
		uri /verifier
		copy_headers X-Utilisateur
	}}
	reverse_proxy 127.0.0.1:{ORIGINE}
}}
"""


def statut(jeton):
    req = urllib.request.Request(f"http://127.0.0.1:{PROXY}/api")
    if jeton:
        req.add_header("Authorization", f"Bearer {jeton}")
    try:
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def attendre(delai=10):
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            if statut("jeton-valide")[0] == 200:
                return
        except OSError:
            pass
        time.sleep(0.05)
    raise RuntimeError("passerelle muette")


def wrk():
    sortie = subprocess.run(["wrk", "-t2", "-c16", "-d8s", "--latency",
                             "-H", "Authorization: Bearer jeton-valide",
                             f"http://127.0.0.1:{PROXY}/api"], capture_output=True, text=True).stdout
    debit = float(re.search(r"Requests/sec:\s+([\d.]+)", sortie).group(1))

    def centile(c):
        v, u = re.search(rf"^\s+{c}%\s+([\d.]+)(us|ms|s)", sortie, re.M).groups()
        return float(v) * {"us": 0.001, "ms": 1, "s": 1000}[u]
    return debit, centile(50), centile(99)


def main():
    services = [subprocess.Popen([sys.executable, str(ICI / "services.py"), "origine", str(ORIGINE)]),
                subprocess.Popen([sys.executable, str(ICI / "services.py"), "auth", str(AUTH)])]
    print("petite API derrière Caddy ; wrk, 16 connexions, 8 s, avec un jeton valide")
    print(f"{'montage':<28}{'sans jeton':>12}{'req/s':>8}{'médiane':>11}{'99e cent.':>11}")
    try:
        for nom, conf in (("proxy seul", SANS), ("proxy + forward_auth", AVEC)):
            f = tempfile.NamedTemporaryFile("w", suffix=".caddyfile", delete=False)
            f.write(conf)
            f.close()
            caddy = subprocess.Popen(["caddy", "run", "--adapter", "caddyfile", "--config", f.name],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            attendre()
            code, _ = statut(None)
            debit, p50, p99 = wrk()
            print(f"{nom:<28}{code:>12}{debit:>8.0f}{p50:>8.2f} ms{p99:>8.2f} ms")
            if conf is AVEC:
                reponse = statut("jeton-valide")[1]
            caddy.terminate()
            caddy.wait()
            os.unlink(f.name)
        print("réponse de l'API avec jeton, à travers forward_auth :", reponse)
    finally:
        for s in services:
            s.terminate()
            s.wait()


if __name__ == "__main__":
    main()
