"""Chapitre 36 : découverte de services, et ce qui arrive quand une instance
meurt.

Trois instances d'une API (sous-processus, ports 11610 à 11612) s'inscrivent
dans un registre. Un client envoie une requête toutes les 10 ms à tour de
rôle aux instances qu'il connaît, pendant 6 s ; à 1,5 s, une instance est tuée
(SIGKILL). Variantes :
- etcd : chaque instance inscrit sa clé avec un bail de 3 s qu'elle renouvelle
  chaque seconde ; le client suit le préfixe par un watch ;
- Consul : l'agent vérifie /sante de chaque instance toutes les secondes ; le
  client suit /v1/health/service par des requêtes bloquantes ;
- Consul par DNS (enregistrements SRV), résolu avant chaque requête ;
- la même liste DNS gardée 30 s en cache, sans puis avec nouvel essai sur une
  autre instance après un échec.

    python3 decouverte.py
"""
import http.client
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import dns.resolver

import commun

ICI = Path(__file__).parent
PORTS = [11610, 11611, 11612]

INSTANCE = """
import sys, threading, time
sys.path.insert(0, ".")
from aiohttp import web
import commun
port, registre = int(sys.argv[1]), sys.argv[2]
if registre == "etcd":
    bail = commun.bail(3)
    commun.etcd_put(f"/services/api/{port}", str(port), bail)
    def renouveler():
        while True:
            commun.renouveler(bail)
            time.sleep(1)
    threading.Thread(target=renouveler, daemon=True).start()

async def racine(r):
    return web.Response(text=str(port))

app = web.Application()
app.add_routes([web.get("/", racine), web.get("/sante", racine)])
web.run_app(app, host="127.0.0.1", port=port, print=None, access_log=None)
"""


class Liste:
    def __init__(self):
        self.ports, self.verrou = [], threading.Lock()

    def fixer(self, ports):
        with self.verrou:
            self.ports = sorted(ports)

    def lire(self):
        with self.verrou:
            return list(self.ports)


def suivre_etcd(liste, arret):
    s, d = commun.etcd("/v3/kv/range", {"key": commun.b64("/services/api/"),
                                        "range_end": commun.b64("/services/api0")})
    import base64
    liste.fixer(int(base64.b64decode(kv["value"])) for kv in d.get("kvs", []))

    def rappel(t, cle):
        port = int(cle.rsplit("/", 1)[1])
        ports = set(liste.lire())
        (ports.add if t == "PUT" else ports.discard)(port)
        liste.fixer(ports)
        return not arret.is_set()
    commun.surveiller("/services/api/", rappel)


def suivre_consul(liste, arret):
    index = 0
    while not arret.is_set():
        try:
            cx = http.client.HTTPConnection("127.0.0.1", commun.CONSUL_HTTP,
                                            timeout=10)
            cx.request("GET", f"/v1/health/service/api?passing=true"
                              f"&index={index}&wait=2s")
            r = cx.getresponse()
            import json
            d = json.loads(r.read())
            index = int(r.getheader("X-Consul-Index", index))
            liste.fixer(e["Service"]["Port"] for e in d)
            cx.close()
        except OSError:
            time.sleep(0.1)


def resoudre_dns():
    r = dns.resolver.Resolver(configure=False)
    r.nameservers, r.port = ["127.0.0.1"], commun.CONSUL_DNS
    rep = r.resolve("api.service.consul", "SRV")
    return sorted(x.port for x in rep), rep.rrset.ttl


def client(liste, mode, victime, duree=6.0, mort_a=1.5):
    resultats = []                              # (t, port, ok)
    t0, k, ttl_vu = time.monotonic(), 0, None
    cache_jusqu_a, cache = 0.0, []
    tuee = None
    while (t := time.monotonic() - t0) < duree:
        if tuee is None and t >= mort_a:
            victime.send_signal(signal.SIGKILL)
            victime.wait()
            tuee = t
        if mode == "dns":
            ports, ttl_vu = resoudre_dns()
        elif mode.startswith("cache"):
            if time.monotonic() >= cache_jusqu_a:
                cache, ttl_vu = resoudre_dns()
                cache_jusqu_a = time.monotonic() + 30
            ports = cache
        else:
            ports = liste.lire()
        if ports:
            essais = [ports[k % len(ports)]]
            if mode == "cache+essai":
                essais += [p for p in ports if p != essais[0]]
            for port in essais:
                try:
                    cx = http.client.HTTPConnection("127.0.0.1", port,
                                                    timeout=0.5)
                    cx.request("GET", "/")
                    cx.getresponse().read()
                    cx.close()
                    resultats.append((t, port, True))
                    break
                except OSError:
                    resultats.append((t, port, False))
            k += 1
        time.sleep(0.01)
    return resultats, tuee, ttl_vu


def essai(mode):
    registre = "etcd" if mode == "etcd" else "consul"
    instances = {p: subprocess.Popen([sys.executable, "-c", INSTANCE, str(p),
                                      registre], cwd=ICI) for p in PORTS}
    time.sleep(1.0)
    if registre == "consul":
        for p in PORTS:
            commun.requete(commun.CONSUL_HTTP, "PUT",
                           "/v1/agent/service/register",
                           {"ID": f"api-{p}", "Name": "api",
                            "Address": "127.0.0.1", "Port": p,
                            "Check": {"HTTP": f"http://127.0.0.1:{p}/sante",
                                      "Interval": "1s", "Timeout": "500ms",
                                      "DeregisterCriticalServiceAfter": "1m"}})
        time.sleep(3.0)                         # premiers contrôles passés
    liste, arret = Liste(), threading.Event()
    if mode == "etcd":
        threading.Thread(target=suivre_etcd, args=(liste, arret),
                         daemon=True).start()
    elif mode == "consul":
        threading.Thread(target=suivre_consul, args=(liste, arret),
                         daemon=True).start()
    time.sleep(0.5)
    victime_port = PORTS[0]
    res, tuee, ttl = client(liste, mode, instances[victime_port])
    arret.set()
    for p in instances.values():
        if p.poll() is None:
            p.kill()
            p.wait()
    if registre == "consul":
        for p in PORTS:
            commun.requete(commun.CONSUL_HTTP, "PUT",
                           f"/v1/agent/service/deregister/api-{p}")
    else:
        commun.etcd("/v3/kv/deleterange",
                    {"key": commun.b64("/services/api/"),
                     "range_end": commun.b64("/services/api0")})
    vers_morte = [r for r in res if r[1] == victime_port and r[0] >= tuee]
    derniere = max((r[0] for r in vers_morte), default=tuee) - tuee
    # une requête du client échoue si toutes ses tentatives ont échoué
    par_requete, echecs = {}, 0
    for t, port, ok in res:
        par_requete.setdefault(t, []).append(ok)
    echecs = sum(1 for oks in par_requete.values() if not any(oks))
    return len(vers_morte), derniere, echecs, len(par_requete), ttl


def main():
    commun.lancer(etcd=True, consul=True)
    try:
        version = commun.requete(commun.CONSUL_HTTP, "GET",
                                 "/v1/agent/self")[1]["Config"]["Version"]
        print(f"etcd 3.7, Consul {version} ; 3 instances, une requête toutes "
              f"les 10 ms,")
        print("une instance tuée à 1,5 s ; 6 s en tout\n")
        print(f"   {'registre et client':34} {'vers la morte':>13} "
              f"{'dernière':>9} {'échecs vus':>10}")
        for mode, nom in (("etcd", "etcd, watch (bail de 3 s)"),
                          ("consul", "Consul, requêtes bloquantes"),
                          ("dns", "Consul, DNS à chaque requête"),
                          ("cache", "DNS gardé 30 s en cache"),
                          ("cache+essai", "  + nouvel essai sur une autre")):
            n, derniere, echecs, total, ttl = essai(mode)
            print(f"   {nom:34} {n:13} {derniere:8.2f}s {echecs:5} / {total}")
            if mode == "dns":
                print(f"   {'':34} (TTL des réponses DNS : {ttl} s)")
    finally:
        commun.arreter()


if __name__ == "__main__":
    main()
