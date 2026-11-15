"""Chapitre 51 : mettre à l'échelle des travailleurs sur la longueur d'une file.

Des travailleurs prennent des tâches dans une liste Redis ; chaque tâche attend
200 ms un service lent (sans calcul), si bien qu'un travailleur en traite
5 par seconde en n'utilisant presque pas de processeur. Un producteur, sur la
machine, pousse 25 tâches par seconde pendant 180 s : il faut 5 travailleurs.

  A. HorizontalPodAutoscaler sur le processeur (cible 50 % de 50m demandés),
     de 1 à 10 travailleurs ;
  B. KEDA 2.21, déclencheur « redis » sur la longueur de la liste (10 tâches
     par travailleur), de 0 à 10 travailleurs, interrogation toutes les 5 s,
     retour à zéro 30 s après que la file a été trouvée vide (cooldownPeriod) ;
  C. le même déclencheur, avec au moins un travailleur (pas de retour à zéro).

    python3 file.py [--figure fichier.tex]     (environ 20 minutes)
"""
import statistics
from math import log10
import sys
import threading
import time

import redis

from commun import IMAGE, REDIS_PORT, appliquer, attendre, centile, kubectl, nb, objet, preparer, supprimer

DEBIT, DUREE, TACHE = 25, 180, 0.2

TRAVAILLEUR = f"""
apiVersion: apps/v1
kind: Deployment
metadata: {{name: travailleur}}
spec:
  replicas: REPLIQUES
  selector: {{matchLabels: {{app: travailleur}}}}
  template:
    metadata: {{labels: {{app: travailleur}}}}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: appli
          image: {IMAGE}
          env: [{{name: MODE, value: travailleur}}, {{name: TACHE_MS, value: "200"}}, {{name: REDIS, value: redis}}]
          resources: {{requests: {{cpu: 50m, memory: 32Mi}}, limits: {{cpu: 200m, memory: 128Mi}}}}
"""

HPA = """
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: {name: travailleur}
spec:
  scaleTargetRef: {apiVersion: apps/v1, kind: Deployment, name: travailleur}
  minReplicas: 1
  maxReplicas: 10
  metrics:
    - type: Resource
      resource: {name: cpu, target: {type: Utilization, averageUtilization: 50}}
"""

KEDA = """
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: {name: travailleur}
spec:
  scaleTargetRef: {name: travailleur}
  minReplicaCount: 0
  maxReplicaCount: 10
  pollingInterval: 5
  cooldownPeriod: 30
  triggers:
    - type: redis
      metadata: {address: "redis.ch51.svc.cluster.local:6379", listName: taches, listLength: "10"}
"""


def repliques():
    d = objet("deploy", "travailleur")
    return d["spec"].get("replicas", 0), d["status"].get("readyReplicas", 0)


def scenario(r, observer_apres):
    r.delete("taches", "faites")
    serie = []                                  # (t, file, faites, voulues, prêtes)
    t0 = time.monotonic()
    fin_prod = threading.Event()

    def produire():
        for i in range(DEBIT * DUREE):
            cible = t0 + i / DEBIT
            time.sleep(max(0, cible - time.monotonic()))
            r.lpush("taches", f"{time.time():.3f}")
        fin_prod.set()

    th = threading.Thread(target=produire)
    th.start()
    while True:
        t = time.monotonic() - t0
        v, p = repliques()
        serie.append((t, r.llen("taches"), r.llen("faites"), v, p))
        if fin_prod.is_set() and observer_apres(t, serie):
            break
        time.sleep(2)
    th.join()
    attentes = []
    for x in r.lrange("faites", 0, -1):
        e, f = x.decode().split(",")
        attentes.append(float(f) - float(e))
    return serie, attentes


def afficher(nom, serie, attentes):
    print(f"\n{nom}")
    print(f"   {'t':>6}{'en file':>9}{'faites':>9}{'voulues':>9}{'prêtes':>8}")
    montres = list(range(0, 181, 20)) + list(range(240, int(serie[-1][0]) + 1, 60))
    for m in montres:
        t, f, d, v, p = min(serie, key=lambda x: abs(x[0] - m))
        if abs(t - m) < 3:
            print(f"   {m:>4} s{nb(f):>9}{nb(d):>9}{v:>9}{p:>8}")
    print(f"   file la plus longue : {nb(max(x[1] for x in serie))} tâches")
    print(f"   attente d'une tâche : p50 {statistics.median(attentes):.1f} s,"
          f" p99 {centile(attentes, 99):.1f} s")


def main():
    preparer(metriques=True, redis=True, keda=True)
    r = redis.Redis(port=REDIS_PORT)
    attendre(lambda: r.ping(), 60, 1, "redis")
    figure = []

    # A. HPA sur le processeur
    supprimer("scaledobject/travailleur", "hpa/travailleur", "deploy/travailleur")
    appliquer(TRAVAILLEUR.replace("REPLIQUES", "1"))
    appliquer(HPA)
    attendre(lambda: repliques()[1] == 1, 120, 1, "travailleur")
    attendre(lambda: objet("hpa", "travailleur")["status"].get("currentMetrics"), 180, 2, "métriques")
    serie, attentes = scenario(r, lambda t, s: t >= DUREE + 60)
    afficher("A. HPA sur le processeur (50 % de 50m), de 1 à 10 travailleurs", serie, attentes)
    h = objet("hpa", "travailleur")["status"]["currentMetrics"][0]["resource"]["current"]
    reste = serie[-1][1]
    print(f"   utilisation vue par le HPA à la fin : {h.get('averageUtilization')} %"
          f" ({h.get('averageValue')}) ; il reste {nb(reste)} tâches,")
    print(f"   soit {reste / (5 * max(1, serie[-1][4])) / 60:.0f} min de travail au rythme de"
          f" {serie[-1][4]} travailleur(s)")
    figure.append("\\def\\fileA{" + " ".join(f"({t:.0f},{log10(f + 1):.3f})" for t, f, d, v, p in serie) + "}")
    figure.append("\\def\\prA{" + " ".join(f"({t:.0f},{p})" for t, f, d, v, p in serie) + "}")
    supprimer("hpa/travailleur", "deploy/travailleur")
    attendre(lambda: not objet("pods", "-l", "app=travailleur")["items"], 120, 1, "nettoyage")

    # B et C. KEDA sur la longueur de la file, avec ou sans retour à zéro
    for lettre, mini in (("B", 0), ("C", 1)):
        r.delete("taches", "faites")
        appliquer(TRAVAILLEUR.replace("REPLIQUES", str(mini)))
        appliquer(KEDA.replace("minReplicaCount: 0", f"minReplicaCount: {mini}"))
        attendre(lambda: "True" in kubectl("get", "scaledobject", "-n", "ch51", "travailleur", "-o",
                                           "jsonpath={.status.conditions[?(@.type=='Ready')].status}"),
                 120, 2, "keda")
        # départ propre : le minimum de travailleurs, sans changement pendant 30 s
        stable = [time.monotonic()]
        def depart():
            if repliques() != (mini, mini):
                stable[0] = time.monotonic()
            return time.monotonic() - stable[0] >= 30
        attendre(depart, 600, 2, "état de départ")
        serie, attentes = scenario(r, lambda t, s: (s[-1][1] == 0 and s[-1][3] == mini and s[-1][4] == mini
                                                    and t > DUREE + 40) or t > DUREE + 480)
        afficher(f"{lettre}. KEDA sur la longueur de la file (10 tâches par travailleur), de {mini} à 10",
                 serie, attentes)
        prem = next((t for t, f, d, v, p in serie if p >= 1 and (mini or t > 0)), None)
        cinq = next((t for t, f, d, v, p in serie if p >= 5), None)
        regime = [p for t, f, d, v, p in serie if 60 <= t < DUREE]
        changements = sum(1 for x, y in zip(serie, serie[1:]) if x[3] != y[3] and x[0] < DUREE)
        vide = next((t for t, f, d, v, p in serie if t > DUREE and f == 0), None)
        bas = next((t for t, f, d, v, p in serie if t > DUREE and p == mini and v == mini), None)
        if not mini:
            print(f"   premier travailleur prêt à {prem:.0f} s (parti de zéro), cinq à {cinq:.0f} s")
        else:
            print(f"   cinq travailleurs prêts à {cinq:.0f} s")
        print(f"   de 60 à 180 s : {min(regime)} à {max(regime)} travailleurs prêts ;")
        print(f"   {changements} changements de la consigne pendant la production")
        print(f"   producteur arrêté à {DUREE} s : file vide à {vide:.0f} s, {mini} travailleur"
              f"{'s' if mini > 1 else ''} à {bas:.0f} s" if bas else "   pas de retour au minimum observé")
        figure.append(f"\\def\\file{lettre}{{" + " ".join(f"({t:.0f},{log10(f + 1):.3f})" for t, f, d, v, p in serie)
                      + "}")
        figure.append(f"\\def\\pr{lettre}{{" + " ".join(f"({t:.0f},{p})" for t, f, d, v, p in serie) + "}")
        supprimer("scaledobject/travailleur", "deploy/travailleur")
        attendre(lambda: not objet("pods", "-l", "app=travailleur")["items"], 120, 1, "nettoyage")
    r.delete("taches", "faites")
    if len(sys.argv) > 2 and sys.argv[1] == "--figure":
        open(sys.argv[2], "w").write("% produit par outils/experiences/ch51/file.py\n" + "\n".join(figure) + "\n")


if __name__ == "__main__":
    main()
