"""Chapitre 34 : Paxos (décret unique) avec de vrais processus.

Cinq accepteurs tournent chacun dans un processus (UDP, ports 11401 à
11405) et gardent leur état dans un fichier de ~/.cache/cours-sd-ch34,
écrit avec fsync avant chaque réponse (ou sans, pour comparer). Les
proposeurs envoient des messages JSON. Le script mesure :
1. la durée d'une décision, avec et sans fsync ;
2. ce qui se passe quand on arrête (SIGKILL) un, deux puis trois
   accepteurs, puis quand on les relance ;
3. un proposeur A suspendu (SIGSTOP) après avoir fait accepter sa valeur
   par deux accepteurs seulement, pendant qu'un proposeur B passe.

    python3 paxos_reel.py
"""
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

PORTS = [11401 + i for i in range(5)]
MAJORITE = 3
DOSSIER = Path.home() / ".cache" / "cours-sd-ch34"


# --- accepteur ------------------------------------------------------------
def accepteur(ident, port, fichier, durable):
    etat = json.loads(Path(fichier).read_text()) if Path(fichier).exists() \
        else {}
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", port))

    def ecrire():
        with open(fichier, "w") as f:
            json.dump(etat, f)
            f.flush()
            if durable:
                os.fsync(f.fileno())

    while True:
        data, adresse = s.recvfrom(65536)
        m = json.loads(data)
        e = etat.setdefault(m["inst"], {"promis": -1, "accepte": None})
        if m["type"] == "prepare":
            if m["num"] > e["promis"]:
                e["promis"] = m["num"]
                ecrire()
                r = {"type": "promesse", "accepte": e["accepte"]}
            else:
                r = {"type": "refus", "promis": e["promis"]}
        else:                               # accept
            if m["num"] >= e["promis"]:
                e["promis"], e["accepte"] = m["num"], [m["num"], m["valeur"]]
                ecrire()
                r = {"type": "acceptee"}
            else:
                r = {"type": "refus", "promis": e["promis"]}
        r.update(inst=m["inst"], num=m["num"], de=ident)
        s.sendto(json.dumps(r).encode(), adresse)


# --- proposeur ------------------------------------------------------------
def tour(s, inst, num, message, cibles, attendu, delai):
    """Envoie le message aux cibles, attend `attendu` réponses positives."""
    for i in cibles:
        s.sendto(json.dumps(dict(message, inst=inst, num=num)).encode(),
                 ("127.0.0.1", PORTS[i]))
    oui, refus, fin = {}, [], time.monotonic() + delai
    while len(oui) < attendu and time.monotonic() < fin:
        s.settimeout(max(0.001, fin - time.monotonic()))
        try:
            r = json.loads(s.recv(65536))
        except socket.timeout:
            break
        if r["inst"] != inst or r["num"] != num:
            continue
        if r["type"] == "refus":
            refus.append(r["promis"])
        else:
            oui[r["de"]] = r
    return oui, refus


def proposer(inst, valeur, num, cibles1=range(5), cibles2=range(5),
             delai=0.5, pause=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    debut = time.monotonic()
    promesses, refus = tour(s, inst, num, {"type": "prepare"}, cibles1,
                            MAJORITE, delai)
    if len(promesses) < MAJORITE:
        return {"etat": "phase 1 sans majorité", "refus": refus}
    acceptees = [p["accepte"] for p in promesses.values() if p["accepte"]]
    v = max(acceptees)[1] if acceptees else valeur
    if pause:
        pause(v)
    acc, refus = tour(s, inst, num, {"type": "accept", "valeur": v}, cibles2,
                      MAJORITE, delai)
    duree = (time.monotonic() - debut) * 1000
    if len(acc) >= MAJORITE:
        return {"etat": "choisie", "valeur": v, "ms": duree,
                "vus": sorted(promesses), "reprise": bool(acceptees)}
    return {"etat": "phase 2 refusée" if refus else "phase 2 sans majorité",
            "valeur": v, "refus": refus, "acceptee_par": sorted(acc)}


# --- orchestration --------------------------------------------------------
class Grappe:
    def __init__(self, durable):
        self.durable, self.p = durable, {}

    def lancer(self, i):
        self.p[i] = subprocess.Popen(
            [sys.executable, __file__, "accepteur", str(i), str(PORTS[i]),
             str(DOSSIER / f"acc{i}.json"), "1" if self.durable else "0"])

    def tout_lancer(self):
        for i in range(5):
            self.lancer(i)
        time.sleep(0.5)

    def tuer(self, i):
        self.p[i].send_signal(signal.SIGKILL)
        self.p[i].wait()

    def arreter(self):
        for p in self.p.values():
            if p.poll() is None:
                p.kill()
                p.wait()


def duree_decision(durable, n=50):
    shutil.rmtree(DOSSIER, ignore_errors=True)
    DOSSIER.mkdir(parents=True)
    g = Grappe(durable)
    g.tout_lancer()
    try:
        temps = [proposer(f"lat{k}", "v", 1)["ms"] for k in range(n)]
    finally:
        g.arreter()
    return statistics.median(temps), max(temps)


def main():
    print("1. durée d'une décision, 5 accepteurs locaux (50 décisions)")
    for durable in (False, True):
        med, pire = duree_decision(durable)
        print(f"   {'avec' if durable else 'sans'} fsync de l'état : médiane "
              f"{med:6.2f} ms, pire {pire:6.2f} ms")

    shutil.rmtree(DOSSIER, ignore_errors=True)
    DOSSIER.mkdir(parents=True)
    g = Grappe(True)
    g.tout_lancer()
    try:
        print("\n2. accepteurs arrêtés par SIGKILL (une décision à chaque étape)")
        for k, cible in enumerate([None, 0, 1, 2]):
            if cible is not None:
                g.tuer(cible)
            r = proposer(f"panne{k}", f"x{k}", 1)
            morts = k
            detail = (f"choisie en {r['ms']:.1f} ms" if r["etat"] == "choisie"
                      else r["etat"])
            print(f"   {morts} arrêté(s) sur 5 : {detail}")
        for i in (0, 1, 2):
            g.lancer(i)
        time.sleep(0.5)
        r = proposer("panne3", "x3", 11)
        print(f"   accepteurs relancés (état relu sur disque) : "
              f"{r['etat']}, valeur {r.get('valeur')}")

        print("\n3. proposeur A suspendu au milieu de sa phase 2")
        for variante, cibles_b in [("B consulte les accepteurs 2, 3, 4",
                                    [2, 3, 4]),
                                   ("B consulte les accepteurs 0, 1, 2",
                                    [0, 1, 2])]:
            inst = f"duel-{cibles_b[0]}"
            a = subprocess.Popen([sys.executable, __file__, "proposeur-a", inst],
                                 stdout=subprocess.PIPE, text=True)
            ligne = a.stdout.readline().strip()     # A a fait accepter 0 et 1
            a.send_signal(signal.SIGSTOP)
            rb = proposer(inst, "B", 202, cibles1=cibles_b)
            a.send_signal(signal.SIGCONT)
            ra = json.loads(a.stdout.readline())
            a.wait()
            print(f"   {variante} :")
            print(f"      A : {ligne}, puis suspendu ;")
            print(f"          après la reprise : {ra['etat']}")
            print(f"      B voit {rb.get('vus')}, propose « {rb.get('valeur')} »"
                  f"{' (valeur de A reprise)' if rb.get('reprise') else ''},"
                  f" {rb['etat']}")
    finally:
        g.arreter()
        shutil.rmtree(DOSSIER, ignore_errors=True)


def proposeur_a(inst):
    """Fait accepter « A » par les accepteurs 0 et 1 seulement, signale
    qu'il est prêt à être suspendu, puis envoie la suite."""
    def pause(v):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        oui, _ = tour(s, inst, 101, {"type": "accept", "valeur": v}, [0, 1],
                      2, 0.5)
        print(f"accept(101, « {v} ») accepté par {sorted(oui)}", flush=True)
        time.sleep(0.2)                     # suspendu ici par SIGSTOP
    r = proposer(inst, "A", 101, cibles1=[0, 1, 2, 3, 4],
                 cibles2=[2, 3, 4], pause=pause)
    print(json.dumps(r), flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "accepteur":
        accepteur(int(sys.argv[2]), int(sys.argv[3]), sys.argv[4],
                  sys.argv[5] == "1")
    elif len(sys.argv) > 1 and sys.argv[1] == "proposeur-a":
        proposeur_a(sys.argv[2])
    else:
        main()
