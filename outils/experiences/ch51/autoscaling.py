"""Chapitre 51 : autoscaling horizontal (HorizontalPodAutoscaler) sur le processeur.

Le service « web » demande 200m de processeur et il est limité à 500m ; chaque
requête /travail?ms=10 coûte 10 ms de calcul, si bien qu'un pod en sert au plus
50 par seconde. HPA : de 1 à 10 répliques, cible de 50 % d'utilisation (100m par
pod), comportement par défaut. metrics-server collecte toutes les 15 s.

Scénario : 30 s à 5 requêtes par seconde, puis 240 s à 80 requêtes par seconde
(0,8 cœur de calcul, donc 8 pods à la cible), puis plus rien. Deux variantes :
une initialisation de 1 s, puis de 20 s (DEMARRAGE_S). Pour la première, on
observe aussi le retour à 1 réplique (fenêtre de stabilisation de 300 s).
Le premier pod a plus de 5 minutes quand la charge arrive, et le client ouvre une
connexion par requête (voir les commentaires de scenario et de main).

    python3 autoscaling.py [--figure fichier.tex]   (environ 30 minutes)
"""
import asyncio
import sys
import time

import aiohttp

from commun import (IMAGE, URL, appliquer, attendre, centile, kubectl, mediane, nb, objet, preparer,
                    prets, supprimer)

BASE, CHARGE, REPOS = 30, 240, 360
DEBIT_BAS, DEBIT_HAUT = 5, 80


def deployer(demarrage):
    appliquer(f"""
apiVersion: apps/v1
kind: Deployment
metadata: {{name: web}}
spec:
  replicas: 1
  selector: {{matchLabels: {{app: web}}}}
  template:
    metadata: {{labels: {{app: web}}}}
    spec:
      terminationGracePeriodSeconds: 5
      containers:
        - name: appli
          image: {IMAGE}
          env: [{{name: DEMARRAGE_S, value: "{demarrage}"}}]
          resources: {{requests: {{cpu: 200m, memory: 64Mi}}, limits: {{cpu: 500m, memory: 256Mi}}}}
          readinessProbe: {{httpGet: {{path: /pret, port: 8080}}, periodSeconds: 1}}
---
apiVersion: v1
kind: Service
metadata: {{name: web}}
spec:
  type: NodePort
  selector: {{app: web}}
  ports: [{{port: 8080, targetPort: 8080, nodePort: 30080}}]
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {{name: web}}
spec:
  scaleTargetRef: {{apiVersion: apps/v1, kind: Deployment, name: web}}
  minReplicas: 1
  maxReplicas: 10
  metrics:
    - type: Resource
      resource: {{name: cpu, target: {{type: Utilization, averageUtilization: 50}}}}
""")


def age_premier():
    from datetime import datetime, timezone
    p = objet("pods", "-l", "app=web")["items"][0]
    debut = datetime.fromisoformat(p["status"]["startTime"].replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - debut).total_seconds()


def etat_hpa():
    h = objet("hpa", "web")["status"]
    util = None
    for m in h.get("currentMetrics") or []:
        util = m.get("resource", {}).get("current", {}).get("averageUtilization")
    d = objet("deploy", "web")["status"]
    return h.get("desiredReplicas", 0), d.get("readyReplicas", 0), util


async def scenario(repos):
    lat, err, envoyees = {}, {}, {}           # seconde -> liste / compte
    hpa = []                                  # (t, voulues, prêtes, utilisation)
    duree = BASE + CHARGE + repos
    t0 = time.monotonic()
    fin = asyncio.Event()

    async def echantillonner():
        while not fin.is_set():
            try:
                v, p, u = await asyncio.to_thread(etat_hpa)
                hpa.append((time.monotonic() - t0, v, p, u))
            except Exception:
                pass
            await asyncio.sleep(2)
            if repos and time.monotonic() - t0 > BASE + CHARGE + 30 and hpa and hpa[-1][1] == 1 and hpa[-1][2] == 1:
                fin.set()

    # une connexion par requête : kube-proxy répartit les connexions, pas les requêtes ;
    # des connexions gardées ouvertes resteraient sur les premiers pods
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0, force_close=True)) as s:
        async def une(sec):
            t = time.perf_counter()
            try:
                async with s.get(f"{URL}/travail?ms=10", timeout=aiohttp.ClientTimeout(total=2)) as r:
                    await r.read()
                lat.setdefault(sec, []).append(1000 * (time.perf_counter() - t))
            except Exception:
                err[sec] = err.get(sec, 0) + 1

        ech = asyncio.create_task(echantillonner())
        taches, i = [], 0
        while not fin.is_set() and time.monotonic() - t0 < duree:
            now = time.monotonic() - t0
            if BASE <= now < BASE + CHARGE:
                debit = DEBIT_HAUT
            elif now < BASE:
                debit = DEBIT_BAS
            else:
                await asyncio.sleep(0.5)
                continue
            sec = int(now)
            envoyees[sec] = envoyees.get(sec, 0) + 1
            taches.append(asyncio.create_task(une(sec)))
            await asyncio.sleep(1 / debit)
        await asyncio.gather(*taches)
        if not repos:
            fin.set()
        await ech
    return lat, err, envoyees, hpa


def premier(hpa, cond):
    for t, v, p, u in hpa:
        if cond(v, p, u):
            return t - BASE
    return None


def fmt(x):
    return "-" if x is None else f"{x:.0f} s"


def main():
    preparer(metriques=True)
    figure = []
    for demarrage, repos in ((1, REPOS), (20, 0)):
        supprimer("hpa/web", "deploy/web", "svc/web")
        attendre(lambda: not objet("pods", "-l", "app=web")["items"], 120, 1, "nettoyage")
        deployer(demarrage)
        attendre(lambda: prets("app=web") == 1, 120, 1, "web")
        attendre(lambda: isinstance(etat_hpa()[2], int), 300, 2, "métriques du HPA")
        # pendant les 5 min qui suivent son démarrage, le HPA ignore le processeur d'un pod
        # qui n'est pas prêt (--horizontal-pod-autoscaler-cpu-initialization-period) ; le
        # premier pod, surchargé, échouerait à sa sonde et rendrait le HPA aveugle : on attend
        attendre(lambda: age_premier() > 310, 400, 5, "âge du premier pod")
        attendre(lambda: isinstance(etat_hpa()[2], int), 300, 2, "métriques du HPA")
        lat, err, envoyees, hpa = asyncio.run(scenario(repos))

        print(f"\ninitialisation de {demarrage} s ; {DEBIT_HAUT} requêtes/s à partir de t = 0")
        print(f"   {'t':>6}{'envoyées':>10}{'erreurs':>9}{'p50':>9}{'p99':>9}{'voulues':>9}{'prêtes':>8}{'util.':>7}")
        for t in list(range(0, 151, 15)) + list(range(180, CHARGE + 1, 60)):
            secs = range(BASE + t - 15, BASE + t) if t else range(BASE - 15, BASE)
            l = [x for s in secs for x in lat.get(s, [])]
            e = sum(err.get(s, 0) for s in secs)
            n = sum(envoyees.get(s, 0) for s in secs)
            h = min(hpa, key=lambda x: abs(x[0] - BASE - t))
            u = f"{h[3]} %" if h[3] is not None else "-"
            print(f"   {t:>4} s{n:>10}{e:>9}{mediane(l):>6.0f} ms{centile(l, 99):>6.0f} ms{h[1]:>9}{h[2]:>8}{u:>7}")
        tot_err = sum(e for s, e in err.items() if BASE <= s < BASE + CHARGE)
        tot = sum(n for s, n in envoyees.items() if BASE <= s < BASE + CHARGE)
        fin_l = [x for s in range(BASE + CHARGE - 60, BASE + CHARGE) for x in lat.get(s, [])]
        print(f"   premier ajout de répliques : {fmt(premier(hpa, lambda v, p, u: v > 1))} ;"
              f" 5 prêtes : {fmt(premier(hpa, lambda v, p, u: p >= 5))} ;"
              f" 8 prêtes : {fmt(premier(hpa, lambda v, p, u: p >= 8))}")
        print(f"   erreurs pendant la charge : {nb(tot_err)} sur {nb(tot)} ({100 * tot_err / tot:.1f} %),")
        print("   délai de 2 s dépassé ou connexion refusée")
        print(f"   dernière minute de charge : p50 {mediane(fin_l):.0f} ms, p99 {centile(fin_l, 99):.0f} ms,"
              f" {hpa[-1][2] if not repos else max(p for t, v, p, u in hpa if t < BASE + CHARGE)} répliques")
        if repos:
            arret = BASE + CHARGE
            baisse = next((t - arret for t, v, p, u in hpa if t > arret and v < max(x[1] for x in hpa)), None)
            un = next((t - arret for t, v, p, u in hpa if t > arret and p == 1), None)
            print(f"   après l'arrêt de la charge : moins de répliques voulues à {fmt(baisse)} ;")
            print(f"   une seule réplique prête à {fmt(un)}")
        # figure : répliques prêtes et p99 par tranche de 5 s, de -30 s à la fin de la charge
        pts_r = " ".join(f"({t - BASE:.0f},{p})" for t, v, p, u in hpa if t <= BASE + CHARGE)
        pts_v = " ".join(f"({t - BASE:.0f},{v})" for t, v, p, u in hpa if t <= BASE + CHARGE)
        pts_l = []
        for s in range(0, BASE + CHARGE, 5):
            l = [x for k in range(s, s + 5) for x in lat.get(k, [])]
            e = sum(err.get(k, 0) for k in range(s, s + 5))
            p99 = 2000 if e else (centile(l, 99) if l else 0)
            pts_l.append(f"({s - BASE + 2.5:.1f},{min(p99, 2000):.0f})")
        nom = "rapide" if demarrage == 1 else "lent"
        figure += [f"\\def\\pretes{nom}{{{pts_r}}}", f"\\def\\voulues{nom}{{{pts_v}}}",
                   f"\\def\\latence{nom}{{{' '.join(pts_l)}}}"]
    supprimer("hpa/web", "deploy/web", "svc/web")
    if len(sys.argv) > 2 and sys.argv[1] == "--figure":
        open(sys.argv[2], "w").write("% produit par outils/experiences/ch51/autoscaling.py\n" + "\n".join(figure) + "\n")


if __name__ == "__main__":
    main()
