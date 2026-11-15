"""Chapitre 51 : démarrage d'un pod et sondes (readiness, liveness, startup).

L'application met DEMARRAGE_S secondes à s'initialiser avant d'écouter.

  1. étapes du démarrage de 3 nouveaux pods (image déjà présente sur le nœud) :
     création, affectation à un nœud, conteneur lancé, pod prêt ;
  2. passage de 1 à 4 répliques sous charge (20 requêtes par seconde), sans
     sonde de disponibilité puis avec : requêtes en erreur ;
  3. initialisation de 30 s, sonde de vie seule (toutes les 5 s, 3 échecs) puis
     sonde de vie précédée d'une sonde de démarrage : redémarrages en 120 s.

    python3 sondes.py            (environ 7 minutes)
"""
import asyncio
import time
from datetime import datetime

import aiohttp

from commun import IMAGE, URL, appliquer, attendre, kubectl, objet, pods, preparer, prets, supprimer


def deploiement(nom, demarrage, repliques=1, sondes=""):
    appliquer(f"""
apiVersion: apps/v1
kind: Deployment
metadata: {{name: {nom}}}
spec:
  replicas: {repliques}
  selector: {{matchLabels: {{app: {nom}}}}}
  template:
    metadata: {{labels: {{app: {nom}}}}}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: appli
          image: {IMAGE}
          env: [{{name: DEMARRAGE_S, value: "{demarrage}"}}]
          resources: {{requests: {{cpu: 100m, memory: 64Mi}}, limits: {{cpu: "1", memory: 256Mi}}}}
{sondes}
---
apiVersion: v1
kind: Service
metadata: {{name: {nom}}}
spec:
  type: NodePort
  selector: {{app: {nom}}}
  ports: [{{port: 8080, targetPort: 8080, nodePort: 30080}}]
""")


PRET = """          readinessProbe: {httpGet: {path: /pret, port: 8080}, periodSeconds: 1}"""


def etapes():
    deploiement("lent", 8, 1, PRET)
    attendre(lambda: prets("app=lent") == 1, 120, 0.5, "lent")
    anciens = {p["metadata"]["name"] for p in pods("app=lent")}
    vus = {}                                     # pod -> {étape: secondes depuis t0}
    t0 = time.monotonic()
    kubectl("scale", "-n", "ch51", "deploy/lent", "--replicas=4")
    while time.monotonic() - t0 < 60:
        for p in pods("app=lent"):
            n = p["metadata"]["name"]
            if n in anciens:
                continue
            e = vus.setdefault(n, {})
            t = time.monotonic() - t0
            e.setdefault("créé", t)
            if p["spec"].get("nodeName"):
                e.setdefault("affecté", t)
            cs = p["status"].get("containerStatuses", [{}])[0]
            if "running" in cs.get("state", {}):
                e.setdefault("lancé", t)
            if any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", [])):
                e.setdefault("prêt", t)
        if len(vus) == 3 and all("prêt" in e for e in vus.values()):
            break
        time.sleep(0.2)
    print("1. passage de 1 à 4 répliques ; initialisation de 8 s ; secondes depuis")
    print("   la commande kubectl scale (relevés en boucle, 0,3 s d'écart environ)")
    print(f"   {'pod':<26}{'créé':>7}{'affecté':>9}{'lancé':>8}{'prêt':>8}")
    for n, e in sorted(vus.items()):
        print(f"   {n[-26:]:<26}" + "".join(f"{e.get(k, float('nan')):>{w}.1f}" for k, w in
                                           (("créé", 7), ("affecté", 9), ("lancé", 8), ("prêt", 8))))
    supprimer("deploy/lent", "svc/lent")


async def sous_charge(duree, a_t, action, debit=20):
    """Envoie debit requêtes/s pendant duree s ; lance action() à a_t s ; renvoie
    les erreurs par seconde et leur type."""
    erreurs, ok, types = {}, {}, {}
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True)) as s:
        async def une(sec):
            try:
                async with s.get(f"{URL}/travail?ms=1", timeout=aiohttp.ClientTimeout(total=3)) as r:
                    await r.read()
                    if r.status != 200:
                        raise RuntimeError(f"HTTP {r.status}")
                ok[sec] = ok.get(sec, 0) + 1
            except Exception as e:
                erreurs[sec] = erreurs.get(sec, 0) + 1
                k = type(e).__name__
                types[k] = types.get(k, 0) + 1
        taches, t0, fait = [], time.monotonic(), False
        for i in range(duree * debit):
            cible = t0 + i / debit
            await asyncio.sleep(max(0, cible - time.monotonic()))
            if not fait and time.monotonic() - t0 >= a_t:
                await asyncio.to_thread(action)
                fait = True
            taches.append(asyncio.create_task(une(int(time.monotonic() - t0))))
        await asyncio.gather(*taches)
    return erreurs, ok, types


def disponibilite():
    print("\n2. de 1 à 4 répliques à t = 5 s, 20 requêtes par seconde pendant 30 s,")
    print("   initialisation de 8 s, une connexion par requête")
    print(f"   {'sonde de disponibilité':<24}{'envoyées':>9}{'erreurs':>9}{'secondes touchées':>19}")
    for nom, sondes in (("aucune", ""), ("httpGet /pret, 1 s", PRET)):
        deploiement("dispo", 8, 1, sondes)
        attendre(lambda: prets("app=dispo") == 1, 120, 0.5, "dispo")
        attendre(lambda: __import__("commun").get("/travail?ms=1"), 60, 0.5, "service")
        erreurs, ok, types = asyncio.run(sous_charge(
            30, 5, lambda: kubectl("scale", "-n", "ch51", "deploy/dispo", "--replicas=4")))
        n_err = sum(erreurs.values())
        secs = sorted(erreurs)
        plage = f"{secs[0]} à {secs[-1]} s" if secs else "-"
        print(f"   {nom:<24}{n_err + sum(ok.values()):>9}{n_err:>9}{plage:>19}")
        if types:
            print("     " + ", ".join(f"{k} {v}" for k, v in types.items()))
        supprimer("deploy/dispo", "svc/dispo")
        attendre(lambda: not pods("app=dispo"), 120, 1, "suppression")


def demarrage_long():
    VIE = """          livenessProbe: {httpGet: {path: /sante, port: 8080}, periodSeconds: 5, failureThreshold: 3}"""
    DEM = """          startupProbe: {httpGet: {path: /sante, port: 8080}, periodSeconds: 5, failureThreshold: 12}"""
    print("\n3. initialisation de 30 s ; sonde de vie toutes les 5 s, 3 échecs tolérés")
    print(f"   {'sondes':<26}{'redémarrages':>13}{'prêt après':>12}{'état à 120 s':>18}")
    for nom, sondes in (("vie seule", VIE + "\n" + PRET), ("démarrage 60 s, puis vie", DEM + "\n" + VIE + "\n" + PRET)):
        deploiement("long", 30, 1, sondes)
        t0 = time.monotonic()
        pret = None
        while time.monotonic() - t0 < 120:
            if pret is None and prets("app=long") == 1:
                pret = time.monotonic() - t0
            time.sleep(0.5)
        p = pods("app=long")[0]
        cs = p["status"]["containerStatuses"][0]
        etat = cs["state"].get("waiting", {}).get("reason") or list(cs["state"])[0]
        print(f"   {nom:<26}{cs['restartCount']:>13}{(f'{pret:.0f} s' if pret else 'jamais'):>12}{etat:>18}")
        supprimer("deploy/long", "svc/long")
        attendre(lambda: not pods("app=long"), 120, 1, "suppression")


def main():
    preparer()
    etapes()
    disponibilite()
    demarrage_long()


if __name__ == "__main__":
    main()
