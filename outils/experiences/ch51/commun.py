"""Outils communs aux scripts du chapitre 51 : kubectl sur le cluster kind
cours-sd-ch51 (kubeconfig séparé, le contexte courant de la machine n'est pas
touché), application de manifestes, attentes, installation des composants.

Le cluster se crée une fois avec :
    kind create cluster --config cluster.yaml --kubeconfig ~/.cache/cours-sd-ch51/kubeconfig
    docker build -t cours-sd-ch51-appli:1 appli
    kind load docker-image cours-sd-ch51-appli:1 redis:8.8-alpine --name cours-sd-ch51
"""
import json
import os
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

DOSSIER = Path.home() / ".cache" / "cours-sd-ch51"
KUBECONFIG = str(DOSSIER / "kubeconfig")
NS = "ch51"
IMAGE = "cours-sd-ch51-appli:1"
URL = "http://127.0.0.1:13101"                 # NodePort 30080 du cluster kind
REDIS_PORT = 13102                             # NodePort 30379


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def kubectl(*a, entree=None, check=True):
    r = subprocess.run(["kubectl", "--kubeconfig", KUBECONFIG, *a], input=entree, capture_output=True,
                       text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(a)} : {r.stderr.strip()}")
    return r.stdout


def appliquer(yaml):
    kubectl("apply", "-n", NS, "-f", "-", entree=yaml)


def objet(*a):
    return json.loads(kubectl("get", "-n", NS, *a, "-o", "json"))


def pods(selecteur):
    return objet("pods", "-l", selecteur)["items"]


def supprimer(*a):
    kubectl("delete", "-n", NS, *a, "--ignore-not-found", "--wait=true", "--timeout=120s", check=False)


def attendre(cond, delai=180, pas=0.5, quoi="condition"):
    t = time.monotonic()
    while time.monotonic() - t < delai:
        try:
            if cond():
                return time.monotonic() - t
        except (RuntimeError, KeyError, IndexError, OSError):
            pass
        time.sleep(pas)
    raise TimeoutError(quoi)


def prets(selecteur):
    n = 0
    for p in pods(selecteur):
        if p["metadata"].get("deletionTimestamp"):
            continue
        cond = {c["type"]: c["status"] for c in p["status"].get("conditions", [])}
        n += cond.get("Ready") == "True"
    return n


def get(chemin, delai=5):
    with urllib.request.urlopen(URL + chemin, timeout=delai) as r:
        return json.loads(r.read())


def centile(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, max(0, int(round(p / 100 * len(v))) - 1))] if v else float("nan")


def mediane(v):
    return statistics.median(v) if v else float("nan")


def preparer(metriques=False, redis=False, keda=False):
    """Espace de noms, et au besoin metrics-server, Redis et KEDA."""
    kubectl("create", "namespace", NS, check=False)
    if metriques and "metrics-server" not in kubectl("get", "deploy", "-n", "kube-system", check=False):
        f = DOSSIER / "metrics-server.yaml"
        if not f.exists():
            urllib.request.urlretrieve(
                "https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.1/components.yaml", f)
        y = f.read_text().replace("- --secure-port=10250", "- --secure-port=10250\n        - --kubelet-insecure-tls")
        kubectl("apply", "-f", "-", entree=y)
        kubectl("rollout", "status", "-n", "kube-system", "deploy/metrics-server", "--timeout=180s")
        attendre(lambda: "cours-sd-ch51-worker" in kubectl("top", "nodes"), 180, 3, "metrics-server")
    if redis:
        appliquer(f"""
apiVersion: apps/v1
kind: Deployment
metadata: {{name: redis}}
spec:
  selector: {{matchLabels: {{app: redis}}}}
  template:
    metadata: {{labels: {{app: redis}}}}
    spec:
      containers:
        - name: redis
          image: redis:8.8-alpine
          resources: {{requests: {{cpu: 100m, memory: 64Mi}}}}
---
apiVersion: v1
kind: Service
metadata: {{name: redis}}
spec:
  type: NodePort
  selector: {{app: redis}}
  ports: [{{port: 6379, targetPort: 6379, nodePort: 30379}}]
""")
        kubectl("rollout", "status", "-n", NS, "deploy/redis", "--timeout=120s")
    if keda and "keda-operator" not in kubectl("get", "deploy", "-n", "keda", check=False):
        subprocess.run(["helm", "--kubeconfig", KUBECONFIG, "install", "keda", "kedacore/keda", "--version",
                        "2.21.0", "--namespace", "keda", "--create-namespace", "--wait", "--timeout", "300s"],
                       check=True, capture_output=True)
