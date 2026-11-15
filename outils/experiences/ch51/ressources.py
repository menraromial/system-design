"""Chapitre 51 : requêtes et limites de ressources sur un vrai Kubernetes 1.36
(cluster kind cours-sd-ch51, deux nœuds de travail à 2 cœurs allouables).

  1. ordonnancement : 10 répliques qui demandent chacune 500m de processeur ;
     où elles vont, combien restent en attente, et pourquoi ;
  2. limite de processeur : un pod dont on abaisse la limite de 2 cœurs à 100m
     par redimensionnement en place (sous-ressource resize, sans redémarrage) ;
     quota CFS lu dans le cgroup, latence de requêtes de 50 ms et de 5 ms de
     calcul envoyées une à une, périodes où le pod a été bridé ;
  3. limite de mémoire : un pod limité à 128 Mio qui en alloue 300 ; arrêts,
     raison, code de sortie, délais entre redémarrages ; classes de QoS.

    python3 ressources.py        (environ 5 minutes)
"""
import time

from commun import (IMAGE, NS, appliquer, attendre, centile, get, kubectl, mediane, nb, objet, pods,
                    preparer, supprimer)


def ordonnancement():
    appliquer(f"""
apiVersion: apps/v1
kind: Deployment
metadata: {{name: ordo}}
spec:
  replicas: 10
  selector: {{matchLabels: {{app: ordo}}}}
  template:
    metadata: {{labels: {{app: ordo}}}}
    spec:
      containers:
        - name: appli
          image: {IMAGE}
          resources: {{requests: {{cpu: 500m, memory: 64Mi}}}}
""")
    attendre(lambda: sum(p["status"]["phase"] == "Running" for p in pods("app=ordo")) >= 6, 120, 1, "ordo")
    time.sleep(5)
    par_noeud, attente = {}, 0
    for p in pods("app=ordo"):
        n = p["spec"].get("nodeName")
        if n:
            par_noeud[n] = par_noeud.get(n, 0) + 1
        else:
            attente += 1
    # requêtes de processeur de tous les pods, par nœud (y compris kube-proxy, kindnet...)
    tous = __import__("json").loads(kubectl("get", "pods", "-A", "-o", "json"))["items"]
    demande = {}
    for p in tous:
        n = p["spec"].get("nodeName")
        if not n or p["status"]["phase"] not in ("Running", "Pending"):
            continue
        for c in p["spec"]["containers"]:
            v = c.get("resources", {}).get("requests", {}).get("cpu", "0")
            demande[n] = demande.get(n, 0) + (float(v[:-1]) if v.endswith("m") else 1000 * float(v))
    ev = objet("events", "--field-selector", "reason=FailedScheduling")["items"]
    msg = ev[-1]["message"] if ev else "-"
    print("1. 10 répliques demandant 500m de processeur, nœuds à 2 cœurs allouables")
    print(f"   {'nœud':<28}{'répliques':>10}{'processeur demandé':>20}")
    for n in sorted(par_noeud):
        print(f"   {n:<28}{par_noeud[n]:>10}{nb(demande.get(n, 0)):>17} m")
    print(f"   en attente (Pending) : {attente}")
    print("   message de l'ordonnanceur :")
    for k in range(0, len(msg), 68):
        print(f"     {msg[k:k + 68]}")
    supprimer("deploy/ordo")


def lire_cgroup(pod, fichier):
    return kubectl("exec", "-n", NS, pod, "--", "cat", f"/sys/fs/cgroup/{fichier}").strip()


def stat(pod):
    return dict(l.split() for l in lire_cgroup(pod, "cpu.stat").splitlines())


def limite_cpu():
    appliquer(f"""
apiVersion: v1
kind: Pod
metadata: {{name: plafond, labels: {{app: plafond}}}}
spec:
  containers:
    - name: appli
      image: {IMAGE}
      resizePolicy: [{{resourceName: cpu, restartPolicy: NotRequired}}]
      resources:
        requests: {{cpu: 100m, memory: 64Mi}}
        limits: {{cpu: "2", memory: 256Mi}}
      readinessProbe: {{httpGet: {{path: /pret, port: 8080}}, periodSeconds: 1}}
---
apiVersion: v1
kind: Service
metadata: {{name: plafond}}
spec:
  type: NodePort
  selector: {{app: plafond}}
  ports: [{{port: 8080, targetPort: 8080, nodePort: 30080}}]
""")
    attendre(lambda: get("/travail?ms=1"), 120, 1, "plafond")
    print("\n2. limite de processeur abaissée en place ; requêtes envoyées une à une")
    print(f"   {'limite':>6}{'quota CFS':>12}{'50 ms p50':>10}{'p99':>7}{'5 ms p50':>10}{'p99':>7}"
          f"{'bridé':>7}{'redém.':>7}")
    for lim in ("2", "1", "500m", "200m", "100m"):
        kubectl("patch", "pod", "-n", NS, "plafond", "--subresource", "resize", "-p",
                f'{{"spec":{{"containers":[{{"name":"appli","resources":{{"limits":{{"cpu":"{lim}"}}}}}}]}}}}')
        cible = {"2": "2", "1": "1", "500m": "500m", "200m": "200m", "100m": "100m"}[lim]
        attendre(lambda: objet("pod", "plafond")["status"]["containerStatuses"][0]["resources"]["limits"]["cpu"]
                 == cible, 60, 0.5, "resize")
        for _ in range(5):                                   # chauffe
            get("/travail?ms=5")
        avant = stat("plafond")
        longues, courtes = [], []
        for _ in range(30):
            t = time.perf_counter()
            get("/travail?ms=50", 30)
            longues.append(1000 * (time.perf_counter() - t))
        for _ in range(200):
            t = time.perf_counter()
            get("/travail?ms=5", 30)
            courtes.append(1000 * (time.perf_counter() - t))
        apres = stat("plafond")
        periodes = int(apres["nr_periods"]) - int(avant["nr_periods"])
        bride = int(apres["nr_throttled"]) - int(avant["nr_throttled"])
        p = objet("pod", "plafond")
        redem = p["status"]["containerStatuses"][0]["restartCount"]
        q, per = lire_cgroup("plafond", "cpu.max").split()
        cpumax = f"{int(q) // 1000}/{int(per) // 1000} ms"
        part = f"{100 * bride / periodes:.0f} %" if periodes else "-"
        print(f"   {lim:>6}{cpumax:>12}{mediane(longues):>7.0f} ms{centile(longues, 99):>4.0f} ms"
              f"{mediane(courtes):>7.1f} ms{centile(courtes, 99):>4.0f} ms{part:>7}{redem:>7}")
    print(f"   classe de QoS du pod : {objet('pod', 'plafond')['status']['qosClass']}")
    supprimer("pod/plafond", "svc/plafond")


def limite_memoire():
    appliquer(f"""
apiVersion: v1
kind: Pod
metadata: {{name: gourmand}}
spec:
  containers:
    - name: appli
      image: {IMAGE}
      env: [{{name: MEMOIRE_MO, value: "300"}}]
      resources:
        requests: {{cpu: 50m, memory: 128Mi}}
        limits: {{cpu: 500m, memory: 128Mi}}
""")
    from datetime import datetime
    def dt(x):
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    t0 = time.monotonic()
    vies, alloue = {}, 0                     # début du conteneur -> (fin, raison, code)
    while time.monotonic() - t0 < 100:
        s = objet("pod", "gourmand")["status"].get("containerStatuses", [{}])[0]
        for etat in (s.get("lastState", {}), s.get("state", {})):
            fin = etat.get("terminated")
            if fin and fin.get("startedAt") and fin["startedAt"] not in vies:
                vies[fin["startedAt"]] = (fin["finishedAt"], fin["reason"], fin["exitCode"])
                for l in kubectl("logs", "-n", NS, "gourmand", "--previous", check=False).splitlines():
                    if l.endswith("Mo alloués"):
                        alloue = max(alloue, int(l.split()[0]))
        time.sleep(0.5)
    s = objet("pod", "gourmand")["status"]["containerStatuses"][0]
    print("\n3. limite de mémoire de 128 Mio, le programme en alloue 300 (100 s)")
    print(f"   {'vie':>4}{'début':>8}{'durée':>8}{'raison':>12}{'code':>6}{'attente avant redémarrage':>27}")
    debut0 = dt(min(vies))
    prec = None
    for i, d in enumerate(sorted(vies), 1):
        fin, raison, code = vies[d]
        att = f"{(dt(d) - prec).total_seconds():.0f} s" if prec else "-"
        prec = dt(fin)
        print(f"   {i:>4}{(dt(d) - debut0).total_seconds():>6.0f} s{(dt(fin) - dt(d)).total_seconds():>6.0f} s"
              f"{raison:>12}{code:>6}{att:>27}")
    etat = s["state"].get("waiting", {}).get("reason") or list(s["state"])[0]
    print(f"   redémarrages : {s['restartCount']} ; état à 100 s : {etat} ;")
    print(f"   allocation la plus haute relevée dans le journal avant l'arrêt : {alloue} Mo")
    supprimer("pod/gourmand")

    print("\n   classes de QoS selon les ressources déclarées :")
    cas = {"rien": "{}",
           "requêtes < limites": "{requests: {cpu: 100m, memory: 64Mi}, limits: {cpu: 500m, memory: 128Mi}}",
           "requêtes = limites": "{requests: {cpu: 100m, memory: 64Mi}, limits: {cpu: 100m, memory: 64Mi}}"}
    for i, (nom, res) in enumerate(cas.items()):
        appliquer(f"""
apiVersion: v1
kind: Pod
metadata: {{name: qos{i}}}
spec:
  containers: [{{name: appli, image: {IMAGE}, resources: {res}}}]
""")
        print(f"     {nom:<22}{objet('pod', f'qos{i}')['status']['qosClass']}")
    supprimer("pod/qos0", "pod/qos1", "pod/qos2")


def main():
    preparer()
    ordonnancement()
    limite_cpu()
    limite_memoire()


if __name__ == "__main__":
    main()
