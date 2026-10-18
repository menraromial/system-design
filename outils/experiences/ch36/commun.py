"""Chapitre 36 : les services de coordination des expériences.

ZooKeeper 3.9 (cours-sd-ch36-zk, port 11601), etcd 3.7 (cours-sd-ch36-etcd,
port 11602) et Consul 1.20 en mode développement (cours-sd-ch36-consul,
réseau de l'hôte, HTTP 11603, DNS 11604). Fonctions d'accès à etcd par sa
passerelle HTTP/JSON. Tout est supprimé par son nom à la fin.
"""
import base64
import http.client
import json
import subprocess
import time

ZK_PORT, ETCD_PORT, CONSUL_HTTP, CONSUL_DNS = 11601, 11602, 11603, 11604
NOMS = ["cours-sd-ch36-zk", "cours-sd-ch36-etcd", "cours-sd-ch36-consul"]


def sh(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True)


def lancer(zk=False, etcd=False, consul=False):
    arreter()
    if zk:
        sh("docker", "run", "-d", "--name", NOMS[0], "-p",
           f"{ZK_PORT}:2181", "zookeeper:3.9")
    if etcd:
        sh("docker", "run", "-d", "--name", NOMS[1], "-p",
           f"{ETCD_PORT}:2379", "--entrypoint", "/usr/local/bin/etcd",
           "registry.k8s.io/etcd:3.7.0-0", "--name", "e1",
           "--data-dir", "/var/lib/etcd",
           "--listen-client-urls", "http://0.0.0.0:2379",
           "--advertise-client-urls", "http://127.0.0.1:2379")
    if consul:
        sh("docker", "run", "-d", "--name", NOMS[2], "--network", "host",
           "hashicorp/consul:1.20", "agent", "-dev", "-client=127.0.0.1",
           "-bind=127.0.0.1", f"-http-port={CONSUL_HTTP}",
           f"-dns-port={CONSUL_DNS}", "-serf-lan-port=11605",
           "-serf-wan-port=11606", "-server-port=11607")
    fin = time.monotonic() + 90
    while time.monotonic() < fin:
        ok = True
        if zk:
            from kazoo.client import KazooClient
            try:
                c = KazooClient(hosts=f"127.0.0.1:{ZK_PORT}", timeout=2)
                c.start(timeout=2)
                c.stop()
                c.close()
            except Exception:
                ok = False
        if etcd and etcd_put("pret", "1")[0] != 200:
            ok = False
        if consul:
            s, _ = requete(CONSUL_HTTP, "GET", "/v1/status/leader")
            ok = ok and s == 200
        if ok:
            return
        time.sleep(0.5)
    raise RuntimeError("services non prêts")


def arreter():
    for n in NOMS:
        sh("docker", "rm", "-f", "-v", n, check=False)


def requete(port, methode, chemin, corps=None, delai=5.0):
    try:
        cx = http.client.HTTPConnection("127.0.0.1", port, timeout=delai)
        cx.request(methode, chemin,
                   json.dumps(corps) if corps is not None else None,
                   {"Content-Type": "application/json"})
        r = cx.getresponse()
        data = r.read()
        cx.close()
        return r.status, (json.loads(data) if data else {})
    except (OSError, http.client.HTTPException, ValueError) as e:
        return None, {"erreur": type(e).__name__}


def b64(s):
    return base64.b64encode(s.encode()).decode()


def etcd(chemin, corps, delai=5.0):
    return requete(ETCD_PORT, "POST", chemin, corps, delai)


def etcd_put(k, v, bail=None):
    corps = {"key": b64(k), "value": b64(v)}
    if bail:
        corps["lease"] = bail
    return etcd("/v3/kv/put", corps)


def etcd_get(k):
    s, d = etcd("/v3/kv/range", {"key": b64(k)})
    kvs = d.get("kvs", []) if s == 200 else []
    return base64.b64decode(kvs[0]["value"]).decode() if kvs else None


def bail(ttl):
    s, d = etcd("/v3/lease/grant", {"TTL": ttl})
    return d["ID"]


def renouveler(ident):
    s, d = etcd("/v3/lease/keepalive", {"ID": ident}, 2.0)
    return int(d.get("result", {}).get("TTL", 0)) if s == 200 else 0


def surveiller(prefixe, rappel):
    """Watch etcd sur un préfixe, dans le fil courant : appelle
    rappel(type, clé) pour chaque événement (type PUT ou DELETE)."""
    fin = prefixe[:-1] + chr(ord(prefixe[-1]) + 1)
    cx = http.client.HTTPConnection("127.0.0.1", ETCD_PORT, timeout=None)
    cx.request("POST", "/v3/watch",
               json.dumps({"create_request": {"key": b64(prefixe),
                                              "range_end": b64(fin)}}),
               {"Content-Type": "application/json"})
    r = cx.getresponse()
    while True:
        ligne = r.readline()
        if not ligne:
            return
        d = json.loads(ligne).get("result", {})
        for ev in d.get("events", []):
            cle = base64.b64decode(ev["kv"]["key"]).decode()
            if rappel(ev.get("type", "PUT"), cle) is False:
                cx.close()
                return
