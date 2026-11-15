"""Chapitre 50 : traces distribuées avec OpenTelemetry (SDK Python 1.45) et Jaeger 2
(conteneur cours-sd-ch50-jaeger : OTLP sur le port 13318, API sur le port 13686).

Quatre services aiohttp, chacun dans son processus : passerelle (13011) appelle
commandes (13012), qui lit sa base (5 ms simulées) puis appelle stock (13013) et
paiement (13014). Le contexte passe d'un service à l'autre dans l'en-tête W3C
traceparent, injecté et extrait à la main pour qu'on le voie.

  1. une requête : en-têtes reçus par chaque service, arbre de la trace relu
     dans l'API de Jaeger ;
  2. trouver la cause d'une lenteur : stock attend un verrou (250 ms) dans 5 %
     des cas ; 400 requêtes, puis recherche des traces de plus de 200 ms ;
  3. coût : 3 000 requêtes (16 en parallèle) sans SDK, puis avec échantillonnage
     de 0 %, 1 % et 100 % ; temps CPU des services par requête ;
  4. échantillonnage cohérent ou non : 2 000 requêtes, décision à la racine
     propagée (ParentBased, 10 %) ou décision prise par chaque service (10 %) ;
  5. un appel qui ne propage pas l'en-tête : la trace se coupe en deux.

    python3 traces.py            (environ 4 minutes)
"""
import asyncio
import json
import os
import random
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import aiohttp
from aiohttp import web

JAEGER = "cours-sd-ch50-jaeger"
PORT_OTLP, PORT_API = 13318, 13686
PORTS = {"passerelle": 13011, "commandes": 13012, "stock": 13013, "paiement": 13014}


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def docker(*a, check=True):
    return subprocess.run(["docker", *a], capture_output=True, text=True, check=check)


# ---- un service --------------------------------------------------------------------------------
def configurer(nom, mode):
    """mode : 'sans-sdk', un ratio ('0', '0.01', '1') appliqué à la racine et suivi
    par les enfants, ou 'indep:0.1' (chaque service décide seul)."""
    from opentelemetry import trace
    if mode == "sans-sdk":
        return trace.get_tracer(nom)
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import (Decision, ParentBased, Sampler, SamplingResult,
                                                  TraceIdRatioBased)

    class Independant(Sampler):
        """Tire au sort chaque fois qu'une requête entre dans le service (parent
        distant ou absent) ; suit le parent local à l'intérieur du service."""
        def __init__(self, p):
            self.p, self.r = p, random.Random()

        def should_sample(self, parent_context, trace_id, name, kind=None, attributes=None, links=None,
                          trace_state=None):
            parent = trace.get_current_span(parent_context).get_span_context()
            if parent.is_valid and not parent.is_remote:
                ok = parent.trace_flags.sampled
            else:
                ok = self.r.random() < self.p
            return SamplingResult(Decision.RECORD_AND_SAMPLE if ok else Decision.DROP, attributes)

        def get_description(self):
            return "Independant"

    if mode.startswith("indep:"):
        echant = Independant(float(mode[6:]))
    else:
        echant = ParentBased(TraceIdRatioBased(float(mode)))
    fournisseur = TracerProvider(resource=Resource.create({"service.name": nom}), sampler=echant)
    fournisseur.add_span_processor(BatchSpanProcessor(
        OTLPSpanExporter(endpoint=f"http://127.0.0.1:{PORT_OTLP}/v1/traces"),
        schedule_delay_millis=500, max_queue_size=20000, max_export_batch_size=2000))
    trace.set_tracer_provider(fournisseur)
    return trace.get_tracer(nom)


def servir(nom, mode, p_lent, propager):
    from opentelemetry import trace
    from opentelemetry.propagate import extract, inject
    from opentelemetry.trace import SpanKind
    tracer = configurer(nom, mode)
    p_lent, propager = float(p_lent), propager == "1"
    hasard = random.Random()
    session = {}

    async def appeler(service, chemin):
        entetes = {}
        with tracer.start_as_current_span(f"GET {chemin}", kind=SpanKind.CLIENT) as span:
            if propager or service != "paiement":
                inject(entetes)
            span.set_attribute("server.address", service)
            async with session["s"].get(f"http://127.0.0.1:{PORTS[service]}{chemin}", headers=entetes) as r:
                span.set_attribute("http.response.status_code", r.status)
                return await r.json()

    async def traiter(request):
        ctx = extract(request.headers)
        with tracer.start_as_current_span(f"GET {request.path}", context=ctx, kind=SpanKind.SERVER) as span:
            span.set_attribute("http.request.method", "GET")
            span.set_attribute("http.route", request.path)
            appels = []
            if nom == "passerelle":
                appels.append(await appeler("commandes", "/commandes"))
            elif nom == "commandes":
                with tracer.start_as_current_span("SELECT commandes") as db:
                    db.set_attribute("db.system.name", "postgresql")
                    await asyncio.sleep(0.005)
                appels.append(await appeler("stock", "/stock"))
                appels.append(await appeler("paiement", "/paiement"))
            elif nom == "stock":
                if hasard.random() < p_lent:
                    with tracer.start_as_current_span("attente du verrou") as v:
                        v.set_attribute("verrou", "article:4217")
                        await asyncio.sleep(0.250)
                await asyncio.sleep(0.003)
            else:
                await asyncio.sleep(0.008)
            span.set_attribute("http.response.status_code", 200)
            return web.json_response({"service": nom, "traceparent": request.headers.get("traceparent"),
                                      "appels": appels})

    async def debut(app):
        session["s"] = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0))

    async def fin(app):
        await session["s"].close()
        p = trace.get_tracer_provider()
        if hasattr(p, "shutdown"):
            p.shutdown()

    async def sante(_):
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/sante", sante)
    app.router.add_get("/{x}", traiter)
    app.on_startup.append(debut)
    app.on_cleanup.append(fin)
    web.run_app(app, host="127.0.0.1", port=PORTS[nom], print=None)


# ---- pilotage ------------------------------------------------------------------------------------
def demarrer_jaeger():
    docker("rm", "-f", "-v", JAEGER, check=False)
    docker("run", "-d", "--name", JAEGER, "--memory", "2g", "-p", f"127.0.0.1:{PORT_OTLP}:4318",
           "-p", f"127.0.0.1:{PORT_API}:16686", "jaegertracing/jaeger:latest")
    for _ in range(120):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{PORT_API}/api/v3/services", timeout=2).read()
            return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError("jaeger")


class Services:
    def __init__(self, mode, p_lent=0.0, propager=True):
        self.procs = {n: subprocess.Popen([sys.executable, __file__, "--servir", n, mode, str(p_lent),
                                           "1" if propager else "0"]) for n in PORTS}
        for n, port in PORTS.items():
            for _ in range(200):
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{port}/sante", timeout=5).read()
                    break
                except OSError:
                    time.sleep(0.1)

    def cpu(self):
        """Temps CPU cumulé (utilisateur + système) des quatre processus, en s."""
        t = 0
        for p in self.procs.values():
            champs = open(f"/proc/{p.pid}/stat").read().rsplit(")", 1)[1].split()
            t += int(champs[11]) + int(champs[12])
        return t / os.sysconf("SC_CLK_TCK")

    def arreter(self):
        for p in self.procs.values():
            p.terminate()
        for p in self.procs.values():
            p.wait()


async def charge(n, parallele=16):
    lat = []
    sem = asyncio.Semaphore(parallele)
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0)) as s:
        async def une():
            async with sem:
                t = time.perf_counter()
                async with s.get(f"http://127.0.0.1:{PORTS['passerelle']}/passerelle") as r:
                    await r.read()
                lat.append(time.perf_counter() - t)
        t = time.perf_counter()
        await asyncio.gather(*(une() for _ in range(n)))
        return lat, time.perf_counter() - t


def _spans(d):
    """Réponse OTLP JSON de l'API v3 de Jaeger -> {trace_id: [span, ...]}."""
    traces = {}
    for rs in d.get("result", {}).get("resourceSpans", []):
        svc = next(a["value"]["stringValue"] for a in rs["resource"]["attributes"] if a["key"] == "service.name")
        for ss in rs["scopeSpans"]:
            for sp in ss["spans"]:
                debut, fin = int(sp["startTimeUnixNano"]), int(sp["endTimeUnixNano"])
                traces.setdefault(sp["traceId"], []).append(
                    {"id": sp["spanId"], "parent": sp.get("parentSpanId") or None, "nom": sp["name"],
                     "debut": debut, "duree": (fin - debut) / 1000, "service": svc,
                     "kind": {2: "server", 3: "client"}.get(sp.get("kind"), "internal")})
    return traces


def api(chemin, **params):
    q = urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{PORT_API}/api/v3/{chemin}?{q}", timeout=60) as r:
            return _spans(json.loads(r.read()))
    except urllib.error.HTTPError as e:
        if e.code == 404:                                   # aucune trace ne correspond
            return {}
        raise


def chercher(service, duree_min=None):
    maintenant = time.time()
    p = {"query.service_name": service, "query.num_traces": 10000,
         "query.start_time_min": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(maintenant - 3600)),
         "query.start_time_max": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(maintenant + 60))}
    if duree_min:
        p["query.duration_min"] = duree_min
    return api("traces", **p)


def toutes_traces():
    """Toutes les traces stockées, par identifiant, en interrogeant chaque service."""
    vues = {}
    for n in PORTS:
        vues.update(chercher(n))
    return vues


def temps_propre(spans):
    """Durée de chaque span moins celle de ses enfants, cumulée par service."""
    propre = {s["id"]: s["duree"] for s in spans}
    for s in spans:
        if s["parent"] in propre:
            propre[s["parent"]] -= s["duree"]
    par = {}
    for s in spans:
        par[s["service"]] = par.get(s["service"], 0) + propre[s["id"]]
    return par


async def principal():
    demarrer_jaeger()
    lignes_fig = []

    # 1. une requête
    sv = Services("1")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"http://127.0.0.1:{PORTS['passerelle']}/passerelle") as r:
                rep = await r.json()
        print("1. une requête ; en-tête traceparent reçu par chaque service :")
        def montrer(r, d=0):
            print(f"   {r['service']:<11}{r['traceparent'] or '(aucun)'}")
            for a in r["appels"]:
                montrer(a, d + 1)
        montrer(rep)
        await asyncio.sleep(2)
        _, tid, pid, _ = rep["appels"][0]["traceparent"].split("-")
        spans = api(f"traces/{tid}")[tid]
        par_id = {sp["id"]: sp for sp in spans}
        t0 = min(sp["debut"] for sp in spans)
        def prof(sp):
            return 0 if sp["parent"] not in par_id else 1 + prof(par_id[sp["parent"]])
        print(f"   trace {tid[:8]}… dans Jaeger : {len(spans)} spans")
        print(f"   {'service':<11}{'opération':<25}{'début':>8}{'durée':>9}")
        for sp in sorted(spans, key=lambda x: (x["debut"], prof(x))):
            op = "  " * prof(sp) + sp["nom"]
            print(f"   {sp['service']:<11}{op:<25}{(sp['debut'] - t0) / 1e6:>5.1f} ms"
                  f"{sp['duree'] / 1000:>6.1f} ms")
            lignes_fig.append(f"\\spantrace{{{sp['service']}}}{{{sp['nom']}}}{{{prof(sp)}}}"
                              f"{{{(sp['debut'] - t0) / 1e6:.2f}}}{{{sp['duree'] / 1000:.2f}}}{{{sp['kind']}}}")
    finally:
        sv.arreter()

    # 2. lenteur
    demarrer_jaeger()
    sv = Services("1", p_lent=0.05)
    try:
        lat, _ = await charge(400, 8)
        await asyncio.sleep(2)
        lents = chercher("passerelle", "200ms")
        coupable = {}
        for spans in lents.values():
            p = temps_propre(spans)
            c = max(p, key=p.get)
            coupable[c] = coupable.get(c, 0) + 1
        lat.sort()
        op = {}
        for spans in lents.values():
            for sp in spans:
                if sp["duree"] > 200_000 and sp["kind"] == "internal":
                    op[sp["nom"]] = op.get(sp["nom"], 0) + 1
        print("\n2. stock attend un verrou dans 5 % des cas ; 400 requêtes")
        print(f"   latence vue du client : p50 {1000 * lat[199]:.0f} ms, p95 {1000 * lat[379]:.0f} ms,"
              f" p99 {1000 * lat[395]:.0f} ms")
        print(f"   traces de plus de 200 ms dans Jaeger : {len(lents)}")
        print(f"   service au plus grand temps propre : "
              + ", ".join(f"{k} {v}" for k, v in sorted(coupable.items(), key=lambda x: -x[1])))
        print(f"   spans de plus de 200 ms dans ces traces : "
              + ", ".join(f"« {k} » {v}" for k, v in sorted(op.items(), key=lambda x: -x[1])))
    finally:
        sv.arreter()

    # 3. coût
    print("\n3. coût du traçage : 3 000 requêtes, 16 en parallèle")
    print(f"   {'configuration':<26}{'req/s':>7}{'p50':>8}{'CPU/requête':>13}{'traces':>8}")
    for nom, mode in (("API seule, sans SDK", "sans-sdk"), ("SDK, échantillon 0 %", "0"),
                      ("SDK, échantillon 1 %", "0.01"), ("SDK, échantillon 100 %", "1")):
        demarrer_jaeger()
        sv = Services(mode)
        try:
            await charge(300)                                        # chauffe
            await asyncio.sleep(2)
            avant = len(toutes_traces()) if mode != "sans-sdk" else 0
            c0 = sv.cpu()
            lat, duree = await charge(3000)
            cpu = sv.cpu() - c0
            await asyncio.sleep(3)
            n = len(toutes_traces()) - avant if mode != "sans-sdk" else 0
            lat.sort()
            print(f"   {nom:<26}{3000 / duree:>7.0f}{1000 * lat[1500]:>5.1f} ms"
                  f"{1e6 * cpu / 3000:>10.0f} µs{nb(n) if mode != 'sans-sdk' else '-':>8}")
        finally:
            sv.arreter()

    # 4. échantillonnage cohérent
    print("\n4. 2 000 requêtes, 10 % des traces gardées ; 8 spans par requête")
    print(f"   {'décision':<34}{'traces':>8}{'complètes':>11}{'spans':>8}")
    for nom, mode in (("à la racine, propagée", "0.1"), ("par chaque service", "indep:0.1")):
        demarrer_jaeger()
        sv = Services(mode)
        try:
            await charge(2000)
            await asyncio.sleep(3)
            ts = toutes_traces()
            completes = sum(1 for t in ts.values() if len(t) == 8)
            print(f"   {nom:<34}{nb(len(ts)):>8}{nb(completes):>11}"
                  f"{nb(sum(len(t) for t in ts.values())):>8}")
        finally:
            sv.arreter()

    # 5. propagation oubliée
    demarrer_jaeger()
    sv = Services("1", propager=False)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"http://127.0.0.1:{PORTS['passerelle']}/passerelle") as r:
                await r.read()
        await asyncio.sleep(2)
        ts = toutes_traces()
        print("\n5. commandes appelle paiement sans injecter traceparent : une requête")
        for t5, spans in ts.items():
            svcs = sorted({sp["service"] for sp in spans}, key=list(PORTS).index)
            print(f"   trace {t5[:8]}… : {len(spans)} span{'s' if len(spans) > 1 else ''}, services {', '.join(svcs)}")
    finally:
        sv.arreter()
        docker("rm", "-f", "-v", JAEGER, check=False)
    return tid, pid, lignes_fig


def main():
    lignes = asyncio.run(principal())
    if len(sys.argv) > 1 and sys.argv[1] == "--figure":
        tid, pid, spans = lignes
        open(sys.argv[2], "w").write("% produit par outils/experiences/ch50/traces.py\n"
                                     f"\\def\\tracetid{{{tid}}}\n\\def\\traceparentid{{{pid}}}\n"
                                     "\\def\\traceentiere{%\n" + "%\n".join(spans) + "}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--servir":
        servir(*sys.argv[2:6])
    else:
        main()
