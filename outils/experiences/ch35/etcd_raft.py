"""Chapitre 35 : Raft mesuré sur une vraie grappe etcd 3.7.

Les membres tournent dans des conteneurs cours-sd-ch35-e1 à e5 (réseau
cours-sd-ch35-reseau, ports clients 11501 à 11505 sur l'hôte). Pour chacun,
un conteneur auxiliaire qui partage son espace réseau (image construite
cours-sd-ch35-outils, avec tc et iptables) ajoute un retard à tout ce que le
membre émet, ou coupe le trafic entre pairs (port 2380) sans couper les
clients. Le script parle à etcd par sa passerelle HTTP/JSON (/v3/...).

1. durée d'une écriture selon la taille de la grappe et le retard réseau ;
2. panne du leader : durée sans écriture possible, avec les délais par
   défaut puis réduits ;
3. leader coupé de ses pairs : ce qu'il répond, ce que lit un client ;
4. membre isolé puis réintégré, avec et sans pré-vote ;
5. retard réseau croissant et changements de leader ;
6. connectivité partielle (un membre ne joint plus que l'un des deux autres),
   avec et sans pré-vote.

    python3 etcd_raft.py
"""
import base64
import http.client
import json
import statistics
import subprocess
import threading
import time

IMAGE = "registry.k8s.io/etcd:3.7.0-0"
OUTILS = "cours-sd-ch35-outils"
RESEAU = "cours-sd-ch35-reseau"
PORT0 = 11501


def sh(*args, check=True):
    return subprocess.run(args, check=check, capture_output=True, text=True)


def b64(s):
    return base64.b64encode(s.encode()).decode()


class Membre:
    def __init__(self, i):
        self.i = i
        self.nom = f"cours-sd-ch35-e{i}"
        self.port = PORT0 + i - 1
        self.cx = None

    def requete(self, chemin, corps, delai=3.0):
        try:
            if self.cx is None:
                self.cx = http.client.HTTPConnection("127.0.0.1", self.port,
                                                     timeout=delai)
            self.cx.timeout = delai
            if self.cx.sock:
                self.cx.sock.settimeout(delai)
            self.cx.request("POST", chemin, json.dumps(corps),
                            {"Content-Type": "application/json"})
            r = self.cx.getresponse()
            data = json.loads(r.read() or b"{}")
            return r.status, data
        except (OSError, http.client.HTTPException, ValueError) as e:
            self.cx = None
            return None, {"erreur": type(e).__name__}

    def put(self, k, v, delai=3.0):
        return self.requete("/v3/kv/put", {"key": b64(k), "value": b64(v)},
                            delai)

    def get(self, k, serialisable=False, delai=3.0):
        s, d = self.requete("/v3/kv/range", {"key": b64(k),
                                             "serializable": serialisable},
                            delai)
        if s == 200:
            kvs = d.get("kvs", [])
            return base64.b64decode(kvs[0]["value"]).decode() if kvs else None
        return d.get("message") or d.get("error") or d.get("erreur")

    def statut(self):
        """État Raft réel du membre, lu dans /debug/vars (raft.status) ; le
        raftTerm de /v3/maintenance/status est celui de la dernière entrée
        appliquée, pas le terme courant."""
        try:
            cx = http.client.HTTPConnection("127.0.0.1", self.port, timeout=1)
            cx.request("GET", "/debug/vars")
            d = json.loads(cx.getresponse().read())["raft.status"]
            cx.close()
            return {"id": d["id"], "leader": d["lead"], "terme": d["term"],
                    "etat": d["raftState"]}
        except (OSError, http.client.HTTPException, ValueError, KeyError):
            return None


class Grappe:
    def __init__(self, n, battement=100, election=1000, pre_vote=True):
        self.membres = [Membre(i) for i in range(1, n + 1)]
        jeton = f"ch35-{time.monotonic_ns()}"
        initial = ",".join(f"e{m.i}=http://{m.nom}:2380"
                           for m in self.membres)
        for m in self.membres:
            sh("docker", "run", "-d", "--name", m.nom, "--network", RESEAU,
               "-p", f"{m.port}:2379", "--entrypoint", "/usr/local/bin/etcd",
               IMAGE, "--name", f"e{m.i}", "--data-dir", "/var/lib/etcd",
               "--listen-client-urls", "http://0.0.0.0:2379",
               "--advertise-client-urls", f"http://{m.nom}:2379",
               "--listen-peer-urls", "http://0.0.0.0:2380",
               "--initial-advertise-peer-urls", f"http://{m.nom}:2380",
               "--initial-cluster", initial, "--initial-cluster-token", jeton,
               "--initial-cluster-state", "new",
               "--heartbeat-interval", str(battement),
               "--election-timeout", str(election),
               f"--pre-vote={'true' if pre_vote else 'false'}")
            sh("docker", "run", "-d", "--name", f"{m.nom}-outils",
               "--network", f"container:{m.nom}", "--cap-add", "NET_ADMIN",
               OUTILS, "sleep", "1d")
        self.attendre_leader()

    def outil(self, m, *cmd):
        return sh("docker", "exec", f"{m.nom}-outils", *cmd, check=False)

    def retard(self, ms, gigue=0, perte=0):
        for m in self.membres:
            self.outil(m, "tc", "qdisc", "del", "dev", "eth0", "root")
            if ms or perte:
                self.outil(m, "tc", "qdisc", "add", "dev", "eth0", "root",
                           "netem", "delay", f"{ms}ms",
                           *([f"{gigue}ms"] if gigue else []),
                           *(["loss", f"{perte}%"] if perte else []))

    def isoler(self, m, oui=True):
        """Coupe (ou rétablit) le trafic entre m et ses pairs, port 2380."""
        op = "-A" if oui else "-D"
        for chaine, sens in (("INPUT", "--dport"), ("INPUT", "--sport"),
                             ("OUTPUT", "--dport"), ("OUTPUT", "--sport")):
            self.outil(m, "iptables", op, chaine, "-p", "tcp", sens, "2380",
                       "-j", "DROP")

    def ip(self, m):
        return sh("docker", "inspect", "-f",
                  "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                  m.nom).stdout.strip()

    def couper_entre(self, a, b, oui=True):
        """Coupe tout le trafic entre les membres a et b (pas les autres)."""
        op = "-A" if oui else "-D"
        ip_b = self.ip(b)
        self.outil(a, "iptables", op, "INPUT", "-s", ip_b, "-j", "DROP")
        self.outil(a, "iptables", op, "OUTPUT", "-d", ip_b, "-j", "DROP")

    def leader(self):
        """Le membre dans l'état StateLeader au terme le plus élevé."""
        meilleur = (None, None)
        for m in self.membres:
            st = m.statut()
            if st and st["etat"] == "StateLeader" and (
                    meilleur[1] is None or st["terme"] > meilleur[1]):
                meilleur = (m, st["terme"])
        return meilleur

    def attendre_leader(self, delai=30):
        fin = time.monotonic() + delai
        while time.monotonic() < fin:
            chef, terme = self.leader()
            if chef and chef.put("pret", "1", 1.0)[0] == 200:
                return chef, terme
            time.sleep(0.2)
        raise RuntimeError("pas de leader")

    def arreter(self):
        for m in self.membres:
            sh("docker", "rm", "-f", "-v", f"{m.nom}-outils", m.nom,
               check=False)


def mesurer_ecritures(m, n=300):
    t = []
    for k in range(n):
        debut = time.perf_counter()
        s, _ = m.put(f"k{k % 50}", "x" * 100)
        if s == 200:
            t.append((time.perf_counter() - debut) * 1000)
    t.sort()
    return statistics.median(t), t[int(0.99 * len(t))]


def debit(m, clients=32, duree=5):
    fin, total, verrou = time.monotonic() + duree, [0], threading.Lock()

    def boucle(j):
        c = Membre(m.i)
        while time.monotonic() < fin:
            if c.put(f"d{j}", "x" * 100)[0] == 200:
                with verrou:
                    total[0] += 1
    fils = [threading.Thread(target=boucle, args=(j,)) for j in range(clients)]
    for f in fils:
        f.start()
    for f in fils:
        f.join()
    return total[0] / duree


def partie_latence():
    print("1. durée d'une écriture (300 écritures de 100 o à la suite)")
    print(f"   {'grappe':34} {'médiane':>8} {'99e c.':>8} {'32 clients':>11}")
    for n, ms in ((1, 0), (3, 0), (5, 0), (3, 5), (5, 5)):
        g = Grappe(n)
        try:
            g.retard(ms)
            chef, _ = g.attendre_leader()
            med, p99 = mesurer_ecritures(chef)
            d = debit(chef)
            suiveur = next(m for m in g.membres if m is not chef) \
                if n > 1 else None
            nom = (f"{n} membre{'s' if n > 1 else ''}, "
                   + (f"retard de {ms} ms" if ms else "sans retard"))
            print(f"   {nom:34} {med:6.2f}ms {p99:6.2f}ms {d:8.0f}/s")
            if suiveur and ms == 0 and n == 3:
                med2, p992 = mesurer_ecritures(suiveur)
                print(f"   {'  la même, écrite par un suiveur':34} "
                      f"{med2:6.2f}ms {p992:6.2f}ms")
                t = time.perf_counter()
                for _ in range(200):
                    chef.get("k1")
                lin = (time.perf_counter() - t) / 200 * 1000
                t = time.perf_counter()
                for _ in range(200):
                    chef.get("k1", serialisable=True)
                ser = (time.perf_counter() - t) / 200 * 1000
                print(f"   {'  lecture linéarisable / sérialisable':34} "
                      f"{lin:6.2f}ms {ser:6.2f}ms")
        finally:
            g.arreter()


def ecrivain(m, arret, succes):
    while not arret.is_set():
        if m.put("compteur", str(time.monotonic()), 0.5)[0] == 200:
            succes.append(time.monotonic())
        else:
            time.sleep(0.005)
        time.sleep(0.005)


def partie_panne():
    print("\n2. panne (SIGKILL) du leader, 3 membres, une écriture toutes les"
          " 5 ms")
    for battement, election in ((100, 1000), (25, 250)):
        g = Grappe(3, battement, election)
        try:
            trous = []
            for essai in range(3):
                chef, terme = g.attendre_leader()
                autre = next(m for m in g.membres if m is not chef)
                arret, succes = threading.Event(), []
                f = threading.Thread(target=ecrivain,
                                     args=(autre, arret, succes))
                f.start()
                time.sleep(1.0)
                sh("docker", "kill", "-s", "KILL", chef.nom)
                time.sleep(4.0)
                arret.set()
                f.join()
                ecarts = [b - a for a, b in zip(succes, succes[1:])]
                nouveau, terme2 = g.leader()
                trous.append(max(ecarts) * 1000)
                sh("docker", "start", chef.nom)
                chef.cx = None
                g.attendre_leader()
            print(f"   battement {battement} ms, élection {election} ms :")
            print("      écritures impossibles pendant "
                  + ", ".join(f"{t:.0f}" for t in trous) + " ms")
        finally:
            g.arreter()


def partie_partition():
    print("\n3. leader d'une grappe de 5 coupé de ses pairs (port 2380)")
    g = Grappe(5)
    try:
        chef, terme = g.attendre_leader()
        chef.put("cle", "v1")
        g.isoler(chef)
        debut = time.monotonic()
        s, d = chef.put("cle", "v-minoritaire", 15.0)
        print(f"   écriture sur l'ancien leader, au bout de "
              f"{time.monotonic() - debut:.1f} s :")
        print(f"      « {d.get('message') or d} »")
        st = chef.statut()
        print(f"   l'ancien leader est passé en {st['etat']} (terme "
              f"{st['terme']})")
        autre = next(m for m in g.membres if m is not chef)
        s, _ = autre.put("cle", "v2")
        nouveau, terme2 = g.leader()
        print(f"   la majorité écrit v2 : {'acceptée' if s == 200 else s}"
              f" ; nouveau leader e{nouveau.i}, terme {terme} → {terme2}")
        print(f"   lecture sur l'ancien leader : sérialisable « "
              f"{chef.get('cle', True)} »,")
        print(f"      linéarisable « {chef.get('cle', False, 3.0)} »")
        debut = time.monotonic()
        g.isoler(chef, False)
        while chef.get("cle", True) != "v2" and time.monotonic() - debut < 60:
            time.sleep(0.1)
        print(f"   réintégré : sérialisable « {chef.get('cle', True)} » au bout"
              f" de {time.monotonic() - debut:.1f} s, linéarisable « "
              f"{chef.get('cle')} »")
    finally:
        g.arreter()


def partie_prevote():
    print("\n4. suiveur isolé 10 s puis réintégré (grappe de 3)")
    for pre_vote in (True, False):
        g = Grappe(3, pre_vote=pre_vote)
        try:
            chef, terme = g.attendre_leader()
            suiveur = next(m for m in g.membres if m is not chef)
            autre = next(m for m in g.membres
                         if m is not chef and m is not suiveur)
            g.isoler(suiveur)
            time.sleep(10)
            terme_isole = suiveur.statut()["terme"]
            arret, succes = threading.Event(), []
            f = threading.Thread(target=ecrivain, args=(autre, arret, succes))
            f.start()
            time.sleep(1.0)
            g.isoler(suiveur, False)
            time.sleep(5.0)
            arret.set()
            f.join()
            chef2, terme2 = g.leader()
            ecarts = [b - a for a, b in zip(succes, succes[1:])]
            print(f"   pré-vote {'activé' if pre_vote else 'désactivé'} : "
                  f"terme du suiveur isolé {terme} → {terme_isole}")
            print(f"      après son retour : leader e{chef.i} → e{chef2.i}, "
                  f"terme {terme} → {terme2},")
            print(f"      écritures bloquées au plus "
                  f"{max(ecarts) * 1000:.0f} ms")
        finally:
            g.arreter()


def partie_retard():
    print("\n5. retard ajouté à l'émission de chaque membre pendant 30 s")
    print("   (3 membres, battement 100 ms, élection 1 000 ms, gigue ± g)")
    print(f"   {'retard':>8} {'gigue':>7} {'changements de leader':>22} "
          f"{'écriture méd.':>14}")
    for ms, gigue, perte in ((0, 0, 0), (100, 0, 0), (400, 0, 0),
                             (100, 100, 0), (500, 500, 0)):
        g = Grappe(3)
        try:
            g.retard(ms, gigue, perte)
            debut = time.monotonic()
            changements, precedent, durees = 0, None, []
            while time.monotonic() - debut < 30:
                chef, terme = g.leader()
                if chef and (chef.nom, terme) != precedent:
                    if precedent is not None:
                        changements += 1
                    precedent = (chef.nom, terme)
                if chef:
                    t = time.perf_counter()
                    if chef.put("r", "x", 5.0)[0] == 200:
                        durees.append((time.perf_counter() - t) * 1000)
                time.sleep(0.3)
            med = f"{statistics.median(durees):9.0f} ms" if durees \
                else "    aucune"
            print(f"   {ms:6} ms {gigue:5} ms {changements:22} {med:>14}")
        finally:
            g.arreter()


def partie_partielle():
    print("\n6. connectivité partielle (3 membres, 20 s) : un suiveur ne joint")
    print("   plus le leader, mais joint encore le troisième membre")
    for pre_vote in (True, False):
        g = Grappe(3, pre_vote=pre_vote)
        try:
            chef, terme = g.attendre_leader()
            coupe = next(m for m in g.membres if m is not chef)
            tiers = next(m for m in g.membres
                         if m is not chef and m is not coupe)
            arret, succes = threading.Event(), []
            f = threading.Thread(target=ecrivain, args=(tiers, arret, succes))
            f.start()
            time.sleep(1.0)
            g.couper_entre(coupe, chef)
            debut, changements, precedent = time.monotonic(), 0, (chef.nom,
                                                                 terme)
            while time.monotonic() - debut < 20:
                c, t = g.leader()
                if c and (c.nom, t) != precedent:
                    changements += 1
                    precedent = (c.nom, t)
                time.sleep(0.2)
            st_coupe = coupe.statut()
            arret.set()
            f.join()
            ecarts = [b - a for a, b in zip(succes, succes[1:])]
            perdu = sum(e for e in ecarts if e > 0.2)
            print(f"   pré-vote {'activé' if pre_vote else 'désactivé'} : "
                  f"{changements} changement(s) de leader, terme {terme} → "
                  f"{precedent[1]}")
            print(f"      membre coupé : {st_coupe['etat']}, terme "
                  f"{st_coupe['terme']}")
            print(f"      écritures bloquées {perdu:.1f} s sur 20")
        finally:
            g.arreter()


def main(parties):
    sh("docker", "network", "create", RESEAU, check=False)
    subprocess.run(["docker", "build", "-q", "-t", OUTILS, "-"], check=True,
                   input="FROM python:3.14-alpine\nRUN apk add --no-cache "
                         "iproute2-tc iptables\n", text=True,
                   capture_output=True)
    version = sh("docker", "run", "--rm", "--entrypoint",
                 "/usr/local/bin/etcd", IMAGE, "--version").stdout.split()[2]
    print(f"etcd {version} ; un membre par conteneur\n")
    try:
        for p in parties:
            p()
    finally:
        for i in range(1, 6):
            sh("docker", "rm", "-f", "-v", f"cours-sd-ch35-e{i}-outils",
               f"cours-sd-ch35-e{i}", check=False)
        sh("docker", "network", "rm", RESEAU, check=False)
        sh("docker", "rmi", OUTILS, check=False)


if __name__ == "__main__":
    import sys
    toutes = [partie_latence, partie_panne, partie_partition, partie_prevote,
              partie_retard, partie_partielle]
    choix = [toutes[int(x) - 1] for x in sys.argv[1:]] or toutes
    main(choix)
