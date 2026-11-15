"""Chapitre 50 : journaux en texte libre et journaux structurés (JSON).

Un service fictif « commandes » écrit 1 million d'événements (route, statut,
durée, client, identifiant de trace ; le client 4217 fait 2 % du trafic) par le module logging de Python, une fois
en texte libre, une fois en JSON, dans ~/.cache/cours-sd-ch50/journaux.

  1. coût d'écriture d'une ligne, taille brute et compressée ; coût d'un appel
     de niveau DEBUG désactivé, selon qu'on passe les arguments ou une f-chaîne ;
  2. question d'exploitation : les erreurs 5xx du client 4217, par route ;
     expression régulière sur le texte, json.loads ligne à ligne, DuckDB ;
  3. à mi-parcours, un développeur ajoute un champ au message (second fichier
     texte, et champ ajouté au JSON) : ce que la même expression régulière
     retrouve encore, et ce que retrouve la requête JSON.

    python3 journaux.py          (environ 1 minute)
"""
import gzip
import json
import logging
import random
import re
import shutil
import statistics
import time
from pathlib import Path

import duckdb

DOSSIER = Path.home() / ".cache" / "cours-sd-ch50" / "journaux"
N = 1_000_000
ROUTES = ["/produits", "/panier", "/commande", "/paiement", "/compte"]


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def evenements():
    r = random.Random(50)
    t0 = 1_794_700_000.0
    for i in range(N):
        statut = r.choices([200, 404, 500, 503], [96, 1, 2, 1])[0]
        yield {"t": t0 + i * 0.001, "route": r.choice(ROUTES), "statut": statut,
               "duree_ms": round(r.lognormvariate(3.5, 0.6), 1), "client": 4217 if r.random() < 0.02 else r.randrange(10_000),
               "trace_id": f"{r.getrandbits(128):032x}", "change": i >= N // 2}


class Json(logging.Formatter):
    def format(self, rec):
        d = {"ts": self.formatTime(rec, "%Y-%m-%dT%H:%M:%S") + f".{int(rec.msecs):03d}Z",
             "niveau": rec.levelname, "service": "commandes", "msg": rec.getMessage()}
        d.update(rec.champs)
        return json.dumps(d, ensure_ascii=False)


def journal(nom, formateur):
    log = logging.getLogger(nom)
    log.handlers.clear()
    log.propagate = False
    h = logging.FileHandler(DOSSIER / f"{nom}.log", mode="w", encoding="utf-8")
    h.setFormatter(formateur)
    log.addHandler(h)
    log.setLevel(logging.INFO)
    return log, h


def ecrire():
    fmt = logging.Formatter("%(asctime)s %(levelname)s [commandes] %(message)s")
    texte, ht = journal("texte", fmt)
    modifie, hm = journal("texte-modifie", fmt)
    js, hj = journal("json", Json())
    jsm, hjm = journal("json-modifie", Json())
    dt = dj = 0.0
    for e in evenements():
        niveau = logging.ERROR if e["statut"] >= 500 else logging.INFO
        # texte : le développeur ajoute « region=eu-west » au milieu du message à mi-parcours
        t = time.perf_counter()
        texte.log(niveau, "GET %s -> %d en %.1f ms client=%d trace=%s", e["route"], e["statut"],
                  e["duree_ms"], e["client"], e["trace_id"])
        dt += time.perf_counter() - t
        reg = " region=eu-west" if e["change"] else ""
        modifie.log(niveau, "GET %s -> %d%s en %.1f ms client=%d trace=%s", e["route"], e["statut"], reg,
                    e["duree_ms"], e["client"], e["trace_id"])
        champs = {"http.route": e["route"], "http.status": e["statut"], "duree_ms": e["duree_ms"],
                  "client": e["client"], "trace_id": e["trace_id"]}
        t = time.perf_counter()
        js.log(niveau, "requête traitée", extra={"champs": champs})
        dj += time.perf_counter() - t
        if e["change"]:
            champs["region"] = "eu-west"
        jsm.log(niveau, "requête traitée", extra={"champs": champs})
    for h in (ht, hm, hj, hjm):
        h.close()
    return dt, dj


def cout_debug():
    log = logging.getLogger("silence")
    log.setLevel(logging.INFO)
    log.addHandler(logging.NullHandler())
    log.propagate = False
    panier = {"articles": list(range(50)), "client": 4217}
    n = 200_000

    def mesure(f):
        d = []
        for _ in range(5):
            t = time.perf_counter()
            for _ in range(n):
                f()
            d.append((time.perf_counter() - t) / n * 1e9)
        return statistics.median(d)

    vide = mesure(lambda: None)
    args = mesure(lambda: log.debug("panier %s", panier)) - vide
    fch = mesure(lambda: log.debug(f"panier {panier}")) - vide
    return args, fch


def main():
    shutil.rmtree(DOSSIER, ignore_errors=True)
    DOSSIER.mkdir(parents=True)
    dt, dj = ecrire()
    print(f"1. {nb(N)} événements écrits par logging (fichier local)")
    print(f"   {'format':<14}{'µs par ligne':>13}{'taille':>10}{'gzip':>9}{'octets/ligne':>14}")
    for nom, d in (("texte", dt), ("json", dj)):
        f = DOSSIER / f"{nom}.log"
        brut = f.read_bytes()
        gz = len(gzip.compress(brut, 6))
        print(f"   {nom:<14}{1e6 * d / N:>13.1f}{len(brut) / 1e6:>7.0f} Mo{gz / 1e6:>6.0f} Mo{len(brut) / N:>14.0f}")
    print(f"   exemple texte : {(DOSSIER / 'texte.log').open().readline()[:54]}…")
    print(f"   exemple JSON  : {(DOSSIER / 'json.log').open().readline()[:54]}…")
    premiere = (DOSSIER / "json.log").open(encoding="utf-8").readline().strip()
    print("   première ligne JSON en entier :")
    for k in range(0, len(premiere), 68):
        print(f"     {premiere[k:k + 68]}")
    a, f = cout_debug()
    print(f"   log.debug désactivé, (\"panier %s\", panier) : {a:>6.0f} ns")
    print(f"   log.debug désactivé, f\"panier {{panier}}\"   : {f:>6.0f} ns")

    # vérité
    vrai = {}
    for e in evenements():
        if e["client"] == 4217 and e["statut"] >= 500:
            vrai[e["route"]] = vrai.get(e["route"], 0) + 1
    total = sum(vrai.values())

    print("\n2. erreurs 5xx du client 4217, par route (médiane de 3)")
    motif = re.compile(r"GET (\S+) -> (5\d\d) en [\d.]+ ms client=4217 ")

    def regex(fichier="texte.log"):
        res = {}
        with open(DOSSIER / fichier, encoding="utf-8") as fh:
            for ligne in fh:
                m = motif.search(ligne)
                if m:
                    res[m[1]] = res.get(m[1], 0) + 1
        return res

    def charger():
        res = {}
        with open(DOSSIER / "json.log", encoding="utf-8") as fh:
            for ligne in fh:
                d = json.loads(ligne)
                if d["client"] == 4217 and d["http.status"] >= 500:
                    res[d["http.route"]] = res.get(d["http.route"], 0) + 1
        return res

    def sql(fichier="json.log"):
        q = f"""select "http.route", count(*) from read_json('{DOSSIER / fichier}', format = 'newline_delimited')
                where client = 4217 and "http.status" >= 500 group by 1"""
        return dict(duckdb.execute(q).fetchall())

    print(f"   {'méthode':<34}{'durée':>8}{'trouvées':>10}")
    for nom, f in (("regex sur le texte", regex), ("json.loads ligne à ligne", charger),
                   ("DuckDB read_json", sql)):
        d = []
        for _ in range(3):
            t = time.perf_counter()
            res = f()
            d.append(time.perf_counter() - t)
        print(f"   {nom:<34}{statistics.median(d):>6.2f} s{sum(res.values()):>6} / {total}")

    print("\n3. au 500 000e événement, le message texte gagne « region=eu-west »")
    avant = sum(1 for e in evenements() if e["client"] == 4217 and e["statut"] >= 500 and not e["change"])
    res = regex("texte-modifie.log")
    print(f"   même regex sur le texte : {sum(res.values()):>4} / {total}"
          f" ({avant} dans la première moitié)")
    print(f"   même requête sur le JSON : {sum(sql("json-modifie.log").values()):>3} / {total}"
          " (le champ region s'ajoute à côté)")
    shutil.rmtree(DOSSIER)


if __name__ == "__main__":
    main()
