"""Chapitre 36 : un verrou etcd, un processus suspendu, et les jetons
d'isolement (fencing tokens).

Un service de stockage (sous-processus, port 11620) reçoit des écritures
qui portent un jeton. Deux travailleurs A et B (sous-processus) prennent un
verrou dans etcd : un bail de 2 s renouvelé toutes les 0,5 s, et une
transaction qui crée la clé du verrou si elle n'existe pas ; le jeton est la
révision etcd de cette création. Le détenteur écrit toutes les 50 ms.
A prend le verrou, puis il est suspendu (SIGSTOP) 4 s ; son bail expire, B
prend le verrou et écrit ; A reprend (SIGCONT). On compte les écritures de A
acceptées alors que B détient le verrou, sans puis avec contrôle du jeton.

    python3 verrou.py
"""
import signal
import subprocess
import sys
import time
from pathlib import Path

import commun

ICI = Path(__file__).parent
PORT_STOCKAGE = 11620

STOCKAGE = f"""
import json, sys
from aiohttp import web
controle = sys.argv[1] == "1"
journal, plus_grand = [], [0]

async def ecrire(r):
    qui, jeton = r.query["qui"], int(r.query["jeton"])
    if controle and jeton < plus_grand[0]:
        journal.append((qui, jeton, "refusée"))
        return web.json_response({{"ok": False}}, status=409)
    plus_grand[0] = max(plus_grand[0], jeton)
    journal.append((qui, jeton, "acceptée"))
    return web.json_response({{"ok": True}})

async def lire(r):
    return web.json_response(journal)

app = web.Application()
app.add_routes([web.post("/ecrire", ecrire), web.get("/journal", lire)])
web.run_app(app, host="127.0.0.1", port={PORT_STOCKAGE}, print=None,
            access_log=None)
"""

TRAVAILLEUR = """
import sys, threading, time, urllib.request
sys.path.insert(0, ".")
import commun
qui = sys.argv[1]
cle = "/verrous/tache"
bail = commun.bail(2)
perdu = threading.Event()

def renouvellement():
    while not perdu.is_set():
        if commun.renouveler(bail) <= 0:
            perdu.set()
            print(f"bail perdu {time.monotonic():.3f}", flush=True)
        time.sleep(0.5)
threading.Thread(target=renouvellement, daemon=True).start()

while True:                                # prendre le verrou
    s, d = commun.etcd("/v3/kv/txn", {
        "compare": [{"key": commun.b64(cle), "target": "CREATE",
                     "result": "EQUAL", "create_revision": "0"}],
        "success": [{"request_put": {"key": commun.b64(cle),
                                     "value": commun.b64(qui),
                                     "lease": bail}}]})
    if d.get("succeeded"):
        jeton = int(d["header"]["revision"])
        break
    time.sleep(0.05)
print(f"verrou {jeton} {time.monotonic():.3f}", flush=True)
while not perdu.is_set():
    try:
        urllib.request.urlopen(urllib.request.Request(
            f"http://127.0.0.1:__PORT__/ecrire?qui={qui}&jeton={jeton}",
            method="POST"), timeout=2)
    except Exception:
        pass
    time.sleep(0.05)
time.sleep(0.5)
"""


def lancer_stockage(controle):
    p = subprocess.Popen([sys.executable, "-c", STOCKAGE,
                          "1" if controle else "0"])
    for _ in range(100):
        s, _ = commun.requete(PORT_STOCKAGE, "GET", "/journal")
        if s == 200:
            return p
        time.sleep(0.05)
    raise RuntimeError("stockage absent")


def travailleur(qui):
    code = TRAVAILLEUR.replace("__PORT__", str(PORT_STOCKAGE))
    return subprocess.Popen([sys.executable, "-c", code, qui],
                            stdout=subprocess.PIPE, text=True, cwd=ICI)


def essai(controle):
    commun.etcd("/v3/kv/deleterange", {"key": commun.b64("/verrous/tache")})
    st = lancer_stockage(controle)
    a = travailleur("A")
    ligne_a = a.stdout.readline().split()        # verrou <jeton> <t>
    b = travailleur("B")
    time.sleep(1.0)
    a.send_signal(signal.SIGSTOP)
    t_pause = time.monotonic()
    ligne_b = b.stdout.readline().split()        # B obtient le verrou
    t_b = float(ligne_b[2])
    time.sleep(max(0.0, 4.0 - (time.monotonic() - t_pause)))
    a.send_signal(signal.SIGCONT)
    perte_a = a.stdout.readline().split()        # bail perdu <t>
    time.sleep(1.0)
    _, journal = commun.requete(PORT_STOCKAGE, "GET", "/journal")
    for p in (a, b, st):
        p.kill()
        p.wait()
    jeton_a, jeton_b = int(ligne_a[1]), int(ligne_b[1])
    # écritures de A arrivées après que B a pris le verrou
    tardives_a = [e for e in journal[[i for i, e in enumerate(journal)
                                      if e[0] == "B"][0]:] if e[0] == "A"]
    acceptees = sum(1 for e in tardives_a if e[2] == "acceptée")
    refusees = sum(1 for e in tardives_a if e[2] == "refusée")
    return {"jetons": (jeton_a, jeton_b),
            "b_apres": t_b - t_pause, "tardives": len(tardives_a),
            "acceptees": acceptees, "refusees": refusees,
            "a_croit": float(perte_a[2]) - t_pause - 4.0
            if perte_a and perte_a[0] == "bail" else None,
            "total": len(journal)}


def main():
    commun.lancer(etcd=True)
    try:
        print("verrou etcd : bail de 2 s renouvelé toutes les 0,5 s ;")
        print("A écrit toutes les 50 ms, puis il est suspendu 4 s ; B attend\n")
        for controle in (False, True):
            r = essai(controle)
            print(f"{'avec' if controle else 'sans'} contrôle du jeton par "
                  f"le stockage :")
            print(f"   jetons : A {r['jetons'][0]}, B {r['jetons'][1]} ; B prend "
                  f"le verrou {r['b_apres']:.2f} s après")
            print(f"   la suspension de A ; à sa reprise, A écrit encore "
                  f"{r['tardives']} fois avant")
            print(f"   de voir son bail perdu, pendant "
                  f"{r['a_croit'] * 1000:.0f} ms")
            print(f"   écritures de A pendant le verrou de B : "
                  f"{r['acceptees']} acceptées, {r['refusees']} refusées")
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
