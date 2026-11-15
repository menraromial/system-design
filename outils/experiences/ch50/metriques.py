"""Chapitre 50 : métriques avec prometheus_client et un vrai Prometheus 3.16
(conteneur cours-sd-ch50-prom, réseau de l'hôte, port 13090).

  1. coût de l'instrumentation dans le code : incrémenter un compteur, observer
     une valeur dans un histogramme, avec ou sans recherche des étiquettes ;
  2. quantiles estimés par histogram_quantile selon les bornes des seaux,
     comparés aux quantiles exacts des mêmes observations ; deux instances,
     l'une rapide, l'autre lente : moyenne des p99 et p99 de l'ensemble ;
  3. cardinalité : un histogramme par route (5 routes) auquel on ajoute une
     étiquette « client » à 1, 100, 1 000 ou 5 000 valeurs ; séries actives,
     mémoire de Prometheus, durée de la collecte, durée d'une requête.

    python3 metriques.py         (environ 7 minutes)
    python3 metriques.py --figure fichier.tex   (données de la figure, sans Prometheus)
"""
import asyncio
import math
import os
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from aiohttp import web

DOSSIER = Path.home() / ".cache" / "cours-sd-ch50"
PROM = "cours-sd-ch50-prom"
PORT_PROM, PORT_SERVICE = 13090, 13001
ROUTES = ["/produits", "/panier", "/commande", "/paiement", "/compte"]


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def signe(x):
    return "0" if round(x) == 0 else f"{x:+.0f}"


def docker(*a, check=True):
    return subprocess.run(["docker", *a], capture_output=True, text=True, check=check)


# ---- 1. coût de l'instrumentation ------------------------------------------------------------
def cout():
    from prometheus_client import CollectorRegistry, Counter, Histogram
    reg = CollectorRegistry()
    c = Counter("req", "requêtes", registry=reg)
    cl = Counter("req_l", "requêtes", ["route", "statut"], registry=reg)
    h = Histogram("lat", "latence", registry=reg)
    hl = Histogram("lat_l", "latence", ["route"], registry=reg)
    enfant = cl.labels("/panier", "200")
    n = 1_000_000

    def mesure(f):
        d = []
        for _ in range(5):
            t = time.perf_counter()
            for _ in range(n):
                f()
            d.append((time.perf_counter() - t) / n * 1e9)
        return statistics.median(d)

    vide = mesure(lambda: None)
    print("1. coût d'une opération (médiane de 5 × 1 million, ns, boucle vide déduite)")
    for nom, f in (("compteur.inc()", c.inc),
                   ("compteur.labels(route, statut).inc()", lambda: cl.labels("/panier", "200").inc()),
                   ("enfant déjà résolu .inc()", enfant.inc),
                   ("histogramme.observe(0.042)", lambda: h.observe(0.042)),
                   ("histogramme.labels(route).observe()", lambda: hl.labels("/panier").observe(0.042))):
        print(f"   {nom:<40}{mesure(f) - vide:>8.0f}")


# ---- service : expose des métriques écrites à la main ----------------------------------------
BORNES = {
    "défaut": [.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10],
    "autour du SLO": [.025, .05, .1, .15, .2, .25, .3, .35, .4, .5, .75, 1, 2],
    "exponentiel ×1,1": [round(0.001 * 1.1 ** k, 6) for k in range(0, 100) if 0.001 * 1.1 ** k <= 10],
}


def latences(instance, n, graine):
    """Instance a : log-normale de médiane 40 ms. Instance b : médiane 120 ms,
    et 2 % de requêtes qui attendent une ressource (+ 300 à 600 ms)."""
    r = random.Random(graine)
    if instance == "a":
        return [r.lognormvariate(math.log(0.040), 0.5) for _ in range(n)]
    return [r.lognormvariate(math.log(0.120), 0.4) + (r.uniform(0.3, 0.6) if r.random() < 0.02 else 0)
            for _ in range(n)]


def texte_quantiles(graine):
    lignes = []
    for nom, bornes in BORNES.items():
        m = {"défaut": "lat_defaut", "autour du SLO": "lat_slo", "exponentiel ×1,1": "lat_exp"}[nom]
        lignes.append(f"# TYPE {m}_seconds histogram")
        for inst in ("a", "b"):
            v = sorted(latences(inst, 200_000, graine + (inst == "b")))
            j = 0
            for b in bornes:
                while j < len(v) and v[j] <= b:
                    j += 1
                lignes.append(f'{m}_seconds_bucket{{inst="{inst}",le="{b}"}} {j}')
            lignes.append(f'{m}_seconds_bucket{{inst="{inst}",le="+Inf"}} {len(v)}')
            lignes.append(f'{m}_seconds_count{{inst="{inst}"}} {len(v)}')
            lignes.append(f'{m}_seconds_sum{{inst="{inst}"}} {sum(v)}')
    return "\n".join(lignes) + "\n"


def servir(mode, arg):
    """Processus séparé : sert /metrics sur PORT_SERVICE."""
    if mode == "quantiles":
        corps = texte_quantiles(int(arg)).encode()

        async def metrics(_):
            return web.Response(body=corps, content_type="text/plain")
    else:
        clients = int(arg)
        bornes = [.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5]
        etiq = [(r, f"c{k:05d}") for r in ROUTES for k in range(clients)]
        compte = [0]

        async def metrics(_):
            compte[0] += 1
            n = compte[0]
            l = ["# TYPE req_duree_seconds histogram"]
            for i, (r, c) in enumerate(etiq):
                lab = f'route="{r}"' if clients == 1 else f'route="{r}",client="{c}"'
                tot = n * (i % 7 + 3)
                for k, b in enumerate(bornes):
                    l.append(f'req_duree_seconds_bucket{{{lab},le="{b}"}} {tot * (k + 1) // 11}')
                l.append(f'req_duree_seconds_bucket{{{lab},le="+Inf"}} {tot}')
                l.append(f'req_duree_seconds_count{{{lab}}} {tot}')
                l.append(f'req_duree_seconds_sum{{{lab}}} {tot * 0.05:.3f}')
            return web.Response(text="\n".join(l) + "\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    web.run_app(app, host="127.0.0.1", port=PORT_SERVICE, print=None)


# ---- Prometheus ----------------------------------------------------------------------------
def demarrer_prom(intervalle):
    (DOSSIER / "prometheus.yml").write_text(f"""global:
  scrape_interval: {intervalle}
  scrape_timeout: {intervalle}
scrape_configs:
  - job_name: service
    static_configs: [{{targets: ['127.0.0.1:{PORT_SERVICE}']}}]
  - job_name: prometheus
    static_configs: [{{targets: ['127.0.0.1:{PORT_PROM}']}}]
""")
    docker("rm", "-f", "-v", PROM, check=False)
    docker("run", "-d", "--name", PROM, "--network", "host", "--memory", "3g",
           "-v", f"{DOSSIER / 'prometheus.yml'}:/etc/prometheus/prometheus.yml:ro",
           "prom/prometheus:latest", "--config.file=/etc/prometheus/prometheus.yml",
           f"--web.listen-address=127.0.0.1:{PORT_PROM}", "--storage.tsdb.path=/prometheus")


async def requete(s, q):
    async with s.get(f"http://127.0.0.1:{PORT_PROM}/api/v1/query", params={"query": q}) as r:
        d = await r.json()
    if d.get("status") != "success":
        raise RuntimeError(d)
    return d["data"]["result"]


async def valeur(s, q, defaut=None):
    try:
        res = await requete(s, q)
    except (aiohttp.ClientError, RuntimeError):
        return defaut
    return float(res[0]["value"][1]) if res else defaut


async def attendre(s, q, cond, delai=180):
    t = time.monotonic()
    while time.monotonic() - t < delai:
        v = await valeur(s, q)
        if v is not None and cond(v):
            return v
        await asyncio.sleep(1)
    raise TimeoutError(q)


def lancer_service(mode, arg):
    p = subprocess.Popen([sys.executable, __file__, "--servir", mode, str(arg)])
    import urllib.request
    for _ in range(600):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{PORT_SERVICE}/metrics", timeout=60).read()
            return p
        except OSError:
            time.sleep(0.1)
    raise TimeoutError("service")


def quantile_exact(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, math.ceil(q * len(v)) - 1)]


async def quantiles(graine):
    service = lancer_service("quantiles", graine)
    demarrer_prom("2s")
    try:
        async with aiohttp.ClientSession() as s:
            await attendre(s, 'count(lat_exp_seconds_count)', lambda v: v == 2)
            a, b = latences("a", 200_000, graine), latences("b", 200_000, graine + 1)
            print("\n2. quantiles : 200 000 observations par instance, a rapide, b lente")
            vrai = {"a": quantile_exact(a, .99), "b": quantile_exact(b, .99), "a+b": quantile_exact(a + b, .99)}
            print(f"   p99 exacts : a {1000 * vrai['a']:.0f} ms, b {1000 * vrai['b']:.0f} ms,"
                  f" a+b {1000 * vrai['a+b']:.0f} ms ;")
            print(f"   moyenne des p99 de a et de b : {500 * (vrai['a'] + vrai['b']):.0f} ms"
                  f" (faux, un p99 ne se moyenne pas)")
            print(f"   {'seaux':<18}{'séries':>7}{'p99 a+b estimé':>16}{'erreur':>8}{'seau qui le contient':>22}")
            for nom, bornes in BORNES.items():
                m = {"défaut": "lat_defaut", "autour du SLO": "lat_slo", "exponentiel ×1,1": "lat_exp"}[nom]
                est = await valeur(s, f"histogram_quantile(0.99, sum by (le) ({m}_seconds_bucket))")
                bas = max([0] + [x for x in bornes if x < est])
                haut = min([x for x in bornes if x >= est] + [math.inf])
                seau = f"{1000 * bas:.0f} à {1000 * haut:.0f} ms"
                print(f"   {nom:<18}{len(bornes) + 3:>7}{1000 * est:>13.0f} ms"
                      f"{signe(100 * (est - vrai['a+b']) / vrai['a+b']):>7} %{seau:>22}")
            print("   part des requêtes sous un seuil (a+b) :")
            for seuil in (0.25, 0.3):
                ex = sum(1 for x in a + b if x <= seuil) / len(a + b)
                line = f"     ≤ {1000 * seuil:.0f} ms : exacte {100 * ex:.2f} %"
                for nom, m in (("défaut", "lat_defaut"), ("SLO", "lat_slo")):
                    q = (f'histogram_fraction(0, {seuil}, sum by (le) ({m}_seconds_bucket))')
                    line += f", {nom} {100 * await valeur(s, q):.2f} %"
                print(line)
    finally:
        service.terminate()
        service.wait()
        docker("rm", "-f", "-v", PROM, check=False)


async def cardinalite():
    print("\n3. cardinalité : histogramme de 10 seaux par route, 5 routes, étiquette")
    print("   client ; collecte toutes les 5 s pendant 60 s, Prometheus limité à 3 Go")
    print(f"   {'clients':>8}{'séries':>10}{'exposition':>12}{'collecte':>10}{'mémoire':>10}"
          f"{'par série':>11}{'requête':>9}")
    base = None
    for clients in (1, 100, 1000, 5000):
        service = lancer_service("cardinalite", clients)
        import urllib.request
        taille = len(urllib.request.urlopen(f"http://127.0.0.1:{PORT_SERVICE}/metrics", timeout=60).read())
        demarrer_prom("5s")
        try:
            async with aiohttp.ClientSession() as s:
                await attendre(s, 'up{job="service"}', lambda v: v == 1)
                await asyncio.sleep(60)
                series = await valeur(s, "prometheus_tsdb_head_series")
                collecte = await valeur(s, 'scrape_duration_seconds{job="service"}')
                rss = await valeur(s, 'process_resident_memory_bytes{job="prometheus"}')
                q = "histogram_quantile(0.99, sum by (le, route) (rate(req_duree_seconds_bucket[1m])))"
                d = []
                for _ in range(5):
                    t = time.perf_counter()
                    await requete(s, q)
                    d.append(time.perf_counter() - t)
                if base is None:
                    base = (rss, series)
                par = f"{(rss - base[0]) / (series - base[1]) / 1000:.1f} Ko" if series > base[1] * 2 else "-"
                print(f"   {nb(clients):>8}{nb(series):>10}{taille / 1e6:>9.1f} Mo{1000 * collecte:>7.0f} ms"
                      f"{rss / 1e6:>7.0f} Mo{par:>11}{1000 * statistics.median(d):>6.0f} ms")
        finally:
            service.terminate()
            service.wait()
            docker("rm", "-f", "-v", PROM, check=False)


def main():
    DOSSIER.mkdir(parents=True, exist_ok=True)
    graine = int(os.environ.get("GRAINE", "50"))
    parties = os.environ.get("PARTIES", "123")             # PARTIES=2 : quantiles seulement
    if "1" in parties:
        cout()
    if "2" in parties:
        asyncio.run(quantiles(graine))
    if "3" in parties:
        asyncio.run(cardinalite())
    (DOSSIER / "prometheus.yml").unlink()


def figure(chemin, graine):
    """Données de la figure : répartition exacte des latences a+b et points des
    seaux, avec le p99 interpolé comme le fait histogram_quantile."""
    v = sorted(latences("a", 200_000, graine) + latences("b", 200_000, graine + 1))
    n = len(v)

    def F(x):
        import bisect
        return bisect.bisect_right(v, x) / n

    def estime(bornes):
        rang = 0.99 * n
        bas = 0.0
        for b in bornes:
            if F(b) * n >= rang:
                return bas + (b - bas) * (rang - F(bas) * n) / ((F(b) - F(bas)) * n)
            bas = b

    cdf = " ".join(f"({x},{F(x / 1000):.5f})" for x in range(150, 665, 5))
    pts = lambda bs: " ".join(f"({1000 * b:.0f},{F(b):.5f})" for b in bs if 0.1 <= b <= 1)
    with open(chemin, "w") as f:
        f.write("% produit par outils/experiences/ch50/metriques.py --figure\n")
        f.write(f"\\def\\cdfexacte{{{cdf}}}\n")
        f.write(f"\\def\\seauxdefaut{{{pts(BORNES['défaut'])}}}\n")
        f.write(f"\\def\\seauxslo{{{pts(BORNES['autour du SLO'])}}}\n")
        f.write(f"\\def\\pvrai{{{1000 * quantile_exact(v, .99):.0f}}}\n")
        f.write(f"\\def\\pdefaut{{{1000 * estime(BORNES['défaut']):.0f}}}\n")
        f.write(f"\\def\\pslo{{{1000 * estime(BORNES['autour du SLO']):.0f}}}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--servir":
        servir(sys.argv[2], sys.argv[3])
    elif len(sys.argv) > 1 and sys.argv[1] == "--figure":
        figure(sys.argv[2], int(os.environ.get("GRAINE", "50")))
    else:
        main()
