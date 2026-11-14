"""Chapitre 44 : démarrage à froid, mémoire et processeur, limites, sur le runtime
Python officiel d'AWS Lambda (image public.ecr.aws/lambda/python:3.13 et son
émulateur d'interface d'exécution, RIE), dans des conteneurs Docker.

  1. démarrage à froid : dix fois par fonction, on crée un environnement neuf
     (un conteneur) et on chronomètre le premier appel, puis 200 appels chauds ;
     fonction « vide » et fonction « lourde » (numpy et pandas importés à
     l'initialisation), à 1 769 Mo (un vCPU) et à 512 Mo ;
  2. mémoire et processeur : Lambda donne du processeur en proportion de la
     mémoire (un vCPU à 1 769 Mo) ; on le reproduit avec --cpus = mémoire / 1 769
     et l'on mesure une fonction qui hache 64 Mo, avec son coût au tarif x86 ;
  3. limites : délai d'exécution dépassé, mémoire dépassée, et ce qu'il advient
     de l'environnement ensuite.

Construire l'image d'abord :  docker build -t cours-sd-ch44-fonction .
    python3 froid.py            (environ 4 minutes)
"""
import http.client
import json
import re
import statistics
import subprocess
import time

IMAGE = "cours-sd-ch44-fonction"
PORT = 12491
PRIX_GO_S = 0.0000166667          # x86, us-east-1, premier palier
PRIX_REQ = 0.20 / 1e6


def nb(x, d=0):
    """Nombre avec espace pour les milliers (sortie à la française, point décimal)."""
    return f"{x:,.{d}f}".replace(",", " ")


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def demarrer(nom, gestionnaire, memoire, delai=None):
    cpus = memoire / 1769
    a = ["run", "-d", "--name", nom, "-p", f"{PORT}:8080", f"--cpus={cpus:.3f}",
         f"--memory={memoire}m", "-e", f"AWS_LAMBDA_FUNCTION_MEMORY_SIZE={memoire}"]
    if delai:
        a += ["-e", f"AWS_LAMBDA_FUNCTION_TIMEOUT={delai}"]
    r = docker(*a, IMAGE, gestionnaire)
    if r.returncode:
        raise SystemExit(r.stderr)


def arreter(nom):
    docker("rm", "-f", nom)


def appeler(evt=None, attendre=False):
    corps = json.dumps(evt or {})
    while True:
        try:
            c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=60)
            t = time.perf_counter()
            c.request("POST", "/2015-03-31/functions/function/invocations", corps)
            r = c.getresponse().read()
            d = (time.perf_counter() - t) * 1000
            c.close()
            try:
                return json.loads(r), d
            except json.JSONDecodeError:
                return {"errorMessage": r.decode().strip()}, d
        except (ConnectionError, http.client.RemoteDisconnected, OSError):
            if not attendre:
                raise
            time.sleep(0.005)


def init_rapportee(nom):
    """Durée de la phase Init telle que le runtime la rapporte (INIT REPORT)."""
    journal = docker("logs", nom).stdout + docker("logs", nom).stderr
    m = re.findall(r"INIT REPORT\(durationMs: ([0-9.]+)\)", journal)
    return float(m[-1]) if m else float("nan")


def centile(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))]


def froid():
    print("1. démarrage à froid (médianes de 10 environnements neufs, en ms)")
    print(f"   {'fonction':<8}{'mémoire':>8}{'1er appel':>11}{'dont Init':>11}"
          f"{'dont code':>11}{'chaud méd.':>12}{'99e':>7}")
    for fonction in ("vide", "lourde"):
        for memoire in (1769, 512):
            premiers, inits, codes, chauds = [], [], [], []
            for i in range(10):
                nom = f"cours-sd-ch44-f-{fonction}"
                t = time.perf_counter()
                demarrer(nom, f"{fonction}.handler", memoire)
                rep, _ = appeler(attendre=True)
                premiers.append((time.perf_counter() - t) * 1000)
                codes.append(rep["init_ms"])
                for _ in range(20):
                    chauds.append(appeler()[1])
                inits.append(init_rapportee(nom))
                arreter(nom)
            print(f"   {fonction:<8}{nb(memoire):>8}{statistics.median(premiers):>11.0f}"
                  f"{statistics.median(inits):>11.0f}{statistics.median(codes):>11.0f}"
                  f"{statistics.median(chauds):>12.2f}{centile(chauds, .99):>7.2f}")
    print("   1er appel : de la création du conteneur à la première réponse ;")
    print("   Init : phase Init rapportée par le runtime ; code : imports du module")


def memoire_cpu():
    print("\n2. mémoire et processeur : hacher 64 Mo (médiane de 5 appels chauds)")
    print(f"   {'mémoire':>8}{'vCPU':>7}{'durée ms':>10}{'Go-s':>9}{'$ / million':>13}")
    for memoire in (128, 256, 512, 1024, 1769, 3008):
        nom = "cours-sd-ch44-f-calcul"
        demarrer(nom, "calcul.handler", memoire)
        appeler({"mo": 1}, attendre=True)
        durees = [appeler({"mo": 64})[1] for _ in range(5)]
        arreter(nom)
        d = statistics.median(durees)
        go_s = d / 1000 * memoire / 1024
        cout = 1e6 * (go_s * PRIX_GO_S + PRIX_REQ)
        print(f"   {nb(memoire):>8}{memoire / 1769:>7.2f}{d:>10.0f}{go_s:>9.3f}{cout:>13.2f}")


def limites():
    print("\n3. limites (128 Mo, délai d'exécution réglé à 2 s)")
    nom = "cours-sd-ch44-f-limites"
    demarrer(nom, "limites.handler", 128, delai=2)
    essais = [("appel normal", {}), ("dormir 4 s", {"dormir": 4}), ("appel normal", {}),
              ("allouer 300 Mo", {"allouer_mo": 300}), ("appel normal", {})]
    rep, _ = appeler({}, attendre=True)
    print(f"   {'appel':<16}{'durée':>9}  réponse")
    print(f"   {'premier appel':<16}{'':>9}  appels = {rep['appels']}, pid = {rep['pid']}")
    for libelle, evt in essais:
        rep, d = appeler(evt)
        if "errorMessage" in rep:
            m = rep["errorMessage"]
            texte = rep.get("errorType", "") + (" (signal: killed)" if "killed" in m else "")
            texte = texte if texte else m[:44]
        else:
            texte = f"appels = {rep['appels']}, pid = {rep['pid']}"
        print(f"   {libelle:<16}{nb(d) + ' ms':>9}  {texte}")
    arreter(nom)


def main():
    for n in ("vide", "lourde", "calcul", "limites"):
        arreter(f"cours-sd-ch44-f-{n}")
    try:
        froid()
        memoire_cpu()
        limites()
    finally:
        for n in ("vide", "lourde", "calcul", "limites"):
            arreter(f"cours-sd-ch44-f-{n}")


if __name__ == "__main__":
    main()
