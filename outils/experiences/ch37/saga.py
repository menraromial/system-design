"""Chapitre 37 : une saga orchestrée, ses compensations et ses pannes.

Trois services, chacun avec sa propre base PostgreSQL (aucune transaction
commune) : stock (port 11721), paiement (11722) et livraison (11723). Une
commande est une saga en trois étapes : réserver l'article, débiter le client,
expédier. Si le débit est refusé (solde insuffisant, un client sur quatre) ou si
l'expédition l'est (adresse invalide, une commande sur dix), l'orchestrateur
compense les étapes faites, dans l'ordre inverse : rembourser, libérer.

L'orchestrateur traite 1 000 commandes avec 8 fils, sur un article dont il n'y a que
300 exemplaires ; on le tue par SIGKILL toutes les 0,5 à 1,5 s et on le relance.
Variantes :
- l'orchestrateur écrit l'état de chaque saga dans sa base après chaque étape
  et reprend les sagas inachevées ; les services sont idempotents (une seconde
  demande pour la même saga renvoie le résultat de la première) ;
- le même orchestrateur, avec des services qui ne sont pas idempotents ;
- un orchestrateur qui garde l'état des sagas en mémoire : relancé, il ne
  reprend que les commandes qu'il n'avait pas commencées.
À la fin, on vérifie chaque saga dans les trois bases.

    python3 saga.py
"""
import random
import signal
import subprocess
import sys
import time
from pathlib import Path

import commun

ICI = Path(__file__).parent
P1 = commun.PG["pg1"]
N, EXEMPLAIRES, FILS = 1000, 300, 8

SERVICES = r"""
import asyncio, json, sys
sys.path.insert(0, ".")
import psycopg, commun
from aiohttp import web
idempotent = sys.argv[1] == "1"

async def service(base, port, routes):
    cx = await psycopg.AsyncConnection.connect(commun.dsn(commun.PG["pg1"], base),
                                               autocommit=True)
    verrou = asyncio.Lock()
    app = web.Application()
    for chemin, f in routes.items():
        async def h(r, f=f):
            corps = await r.json()
            async with verrou:
                code, rep = await f(cx, corps)
            return web.json_response(rep, status=code)
        app.router.add_post(chemin, h)
    run = web.AppRunner(app, access_log=None)
    await run.setup()
    await web.TCPSite(run, "127.0.0.1", port).start()

async def deja(cx, table, saga):
    if not idempotent:
        return None
    r = await (await cx.execute(f"select etat from {table} where saga = %s", (saga,))).fetchone()
    return r[0] if r else None

# ---- stock
async def reserver(cx, c):
    etat = await deja(cx, "reservations", c["saga"])
    if etat:
        return (200 if etat == "reservee" else 409), {"etat": etat}
    async with cx.transaction():
        r = await cx.execute("update stock set disponible = disponible - 1"
                             " where article = 1 and disponible >= 1")
        if r.rowcount == 0:
            await cx.execute("insert into refus select %s, array_agg(saga) from reservations"
                             " where etat = 'reservee'", (c["saga"],))
            return 409, {"etat": "epuise"}
        await cx.execute("insert into reservations (saga, etat) values (%s, 'reservee')",
                         (c["saga"],))
    return 200, {"etat": "reservee"}

async def liberer(cx, c):
    async with cx.transaction():
        r = await cx.execute("update reservations set etat = 'liberee'"
                             " where saga = %s and etat = 'reservee'", (c["saga"],))
        if r.rowcount:
            await cx.execute("update stock set disponible = disponible + %s where article = 1",
                             (r.rowcount,))
        elif idempotent:          # pierre tombale : une réservation tardive sera refusée
            await cx.execute("insert into reservations (saga, etat) values (%s, 'liberee')"
                             " on conflict do nothing", (c["saga"],))
    return 200, {"etat": "liberee"}

# ---- paiement
async def debiter(cx, c):
    etat = await deja(cx, "paiements", c["saga"])
    if etat:
        return (200 if etat == "debite" else 409), {"etat": etat}
    async with cx.transaction():
        r = await cx.execute("update comptes set solde = solde - %s"
                             " where client = %s and solde >= %s",
                             (c["montant"], c["client"], c["montant"]))
        if r.rowcount == 0:
            await cx.execute("insert into paiements (saga, etat) values (%s, 'refuse')",
                             (c["saga"],))
            return 409, {"etat": "refuse"}
        await cx.execute("insert into paiements (saga, client, montant, etat)"
                         " values (%s, %s, %s, 'debite')", (c["saga"], c["client"], c["montant"]))
    return 200, {"etat": "debite"}

async def rembourser(cx, c):
    async with cx.transaction():
        r = await cx.execute("update paiements set etat = 'rembourse'"
                             " where saga = %s and etat = 'debite'"
                             " returning client, montant", (c["saga"],))
        for client, montant in await r.fetchall():
            await cx.execute("update comptes set solde = solde + %s where client = %s",
                             (montant, client))
    return 200, {"etat": "rembourse"}

# ---- livraison
async def expedier(cx, c):
    etat = await deja(cx, "expeditions", c["saga"])
    if etat:
        return (200 if etat == "expediee" else 422), {"etat": etat}
    etat = "refusee" if c["adresse"] == "invalide" else "expediee"
    await cx.execute("insert into expeditions (saga, etat) values (%s, %s)", (c["saga"], etat))
    return (200 if etat == "expediee" else 422), {"etat": etat}

async def principal():
    await service("stock", 11721, {"/reserver": reserver, "/liberer": liberer})
    await service("paiement", 11722, {"/debiter": debiter, "/rembourser": rembourser})
    await service("livraison", 11723, {"/expedier": expedier})
    print("pret", flush=True)
    await asyncio.Event().wait()
asyncio.run(principal())
"""

ORCHESTRATEUR = r"""
import http.client, json, sys, threading, time
sys.path.insert(0, ".")
import commun
journal, fils = sys.argv[1] == "1", int(sys.argv[2])

def appeler(port, chemin, corps):
    cx = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    cx.request("POST", chemin, json.dumps(corps), {"Content-Type": "application/json"})
    r = cx.getresponse()
    r.read()
    cx.close()
    time.sleep(0.02)                       # traitement et réseau d'un vrai service
    return r.status

def noter(c, saga, etat, etape):
    if journal:
        c.execute("update sagas set etat = %s, etape = %s where id = %s", (etat, etape, saga))

def traiter(c, s):
    saga, client, montant, adresse, etat, etape = s
    corps = {"saga": saga, "client": client, "montant": montant, "adresse": adresse}
    while etat in ("nouvelle", "en_cours"):
        if etape == 0:
            if appeler(11721, "/reserver", corps) != 200:
                etat = "refusee"
                break
        elif etape == 1:
            if appeler(11722, "/debiter", corps) != 200:
                etat = "compensation"
        elif etape == 2:
            if appeler(11723, "/expedier", corps) != 200:
                etat = "compensation"
            else:
                etat = "terminee"
                break
        if etat != "compensation":
            etape += 1
            etat = "en_cours"
        noter(c, saga, etat, etape)
    while etat == "compensation":
        # etape = nombre d'étapes faites, à défaire dans l'ordre inverse
        if etape == 2:
            appeler(11722, "/rembourser", corps)
        elif etape == 1:
            appeler(11721, "/liberer", corps)
        etape -= 1
        if etape == 0:
            etat = "annulee"
        noter(c, saga, etat, etape)
    noter(c, saga, etat, etape)
    if not journal:
        c.execute("update sagas set etat = %s where id = %s", (etat, saga))

def fil(i):
    c = commun.connecter(commun.PG["pg1"], "orchestre")
    if journal:          # toutes les sagas inachevées, y compris celles en cours
        q = ("select id, client, montant, adresse, etat, etape from sagas"
             " where etat in ('nouvelle', 'en_cours', 'compensation') and num %% %s = %s"
             " order by num")
    else:                # seulement celles qui n'ont pas été prises
        q = ("select id, client, montant, adresse, etat, etape from sagas"
             " where etat = 'nouvelle' and num %% %s = %s order by num")
    for s in c.execute(q, (fils, i)).fetchall():
        if not journal:
            c.execute("update sagas set etat = 'prise' where id = %s", (s[0],))
        traiter(c, s)

print("pret", flush=True)
ts = [threading.Thread(target=fil, args=(i,)) for i in range(fils)]
for t in ts:
    t.start()
for t in ts:
    t.join()
"""


def preparer(idempotent):
    with commun.connecter(P1) as c:
        for base in ("stock", "paiement", "livraison", "orchestre"):
            c.execute(f"drop database if exists {base} with (force)")
            c.execute(f"create database {base}")
    cle = "primary key" if idempotent else ""
    with commun.connecter(P1, "stock") as c:
        c.execute("create table stock (article int primary key,"
                  " disponible int not null check (disponible >= 0))")
        c.execute("insert into stock values (1, %s)", (EXEMPLAIRES,))
        c.execute(f"create table reservations (saga text {cle}, etat text)")
        c.execute("create table refus (saga text, detenteurs text[])")
    with commun.connecter(P1, "paiement") as c:
        c.execute("create table comptes (client int primary key, solde int not null)")
        c.execute(f"create table paiements (saga text {cle}, client int, montant int,"
                  " etat text)")
    with commun.connecter(P1, "livraison") as c:
        c.execute(f"create table expeditions (saga text {cle}, etat text)")
    rng = random.Random(37)
    with commun.connecter(P1, "orchestre") as c:
        c.execute("create table sagas (num int, id text primary key, client int, montant int,"
                  " adresse text, etat text, etape int)")
        for k in range(1, N + 1):
            adresse = "invalide" if k % 10 == 0 else "valide"
            c.execute("insert into sagas values (%s, %s, %s, 50, %s, 'nouvelle', 0)",
                      (k, f"s{k}", k, adresse))
    with commun.connecter(P1, "paiement") as c:
        for k in range(1, N + 1):          # un client sur quatre n'a pas assez
            c.execute("insert into comptes values (%s, %s)",
                      (k, 20 if rng.random() < 0.25 else 500))


def lancer(code, *args):
    p = subprocess.Popen([sys.executable, "-c", code, *map(str, args)],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, cwd=ICI)
    p.stdout.readline()
    return p


def essai(journal, idempotent):
    preparer(idempotent)
    services = lancer(SERVICES, "1" if idempotent else "0")
    pannes, debut = 0, time.monotonic()
    orch = lancer(ORCHESTRATEUR, "1" if journal else "0", FILS)
    prochaine = time.monotonic() + random.uniform(0.5, 1.5)
    while True:
        if orch.poll() is not None:
            if orch.returncode == 0:
                break
            raise RuntimeError(f"l'orchestrateur s'est arrêté seul ({orch.returncode})")
        if time.monotonic() >= prochaine:
            orch.send_signal(signal.SIGKILL)
            orch.wait()
            pannes += 1
            orch = lancer(ORCHESTRATEUR, "1" if journal else "0", FILS)
            prochaine = time.monotonic() + random.uniform(0.5, 1.5)
        time.sleep(0.02)
    duree = time.monotonic() - debut
    services.kill()
    services.wait()
    return pannes, duree, verifier()


def verifier():
    with commun.connecter(P1, "orchestre") as c:
        etats = dict(c.execute("select id, etat from sagas").fetchall())
    with commun.connecter(P1, "stock") as c:
        tenues = {}
        for saga, n in c.execute("select saga, count(*) from reservations"
                                 " where etat = 'reservee' group by saga"):
            tenues[saga] = n
        disponible = c.execute("select disponible from stock").fetchone()[0]
        refus = c.execute("select saga, detenteurs from refus").fetchall()
    with commun.connecter(P1, "paiement") as c:
        debits = dict(c.execute("select saga, count(*) from paiements where etat = 'debite'"
                                " group by saga").fetchall())
    with commun.connecter(P1, "livraison") as c:
        envois = dict(c.execute("select saga, count(*) from expeditions"
                                " where etat = 'expediee' group by saga").fetchall())
    r = {"terminee": 0, "annulee": 0, "refusee": 0, "incoherentes": 0, "doubles": 0,
         "oubliees": 0}
    for saga, etat in etats.items():
        faits = (tenues.get(saga, 0), debits.get(saga, 0), envois.get(saga, 0))
        if max(faits) > 1:
            r["doubles"] += 1
        if etat == "prise" and faits == (0, 0, 0):
            r["oubliees"] += 1             # commencée, abandonnée sans effet
        elif etat in r and faits in ((1, 1, 1), (0, 0, 0)) and \
                (faits == (1, 1, 1)) == (etat == "terminee"):
            r[etat] += 1
        else:
            r["incoherentes"] += 1
    r["perdus"] = EXEMPLAIRES - disponible - sum(tenues.values())
    # refus pendant qu'un exemplaire était tenu par une saga finalement annulée
    r["refus_evitables"] = sum(1 for _, d in refus
                               if any(etats.get(s) == "annulee" for s in (d or [])))
    r["refus"] = len(refus)
    return r


def main():
    commun.lancer_pg("pg1")
    try:
        print(f"{N} commandes, {EXEMPLAIRES} exemplaires, {FILS} fils ; orchestrateur tué"
              f" toutes les")
        print("0,5 à 1,5 s\n")
        print(f"   {'orchestrateur et services':31} {'pannes':>6} {'fin.':>5} {'ann.':>5}"
              f" {'ref.':>5} {'incoh.':>6} {'doubl.':>6}")
        for journal, idempotent, nom in ((True, True, "journal, idempotents"),
                                         (True, False, "journal, non idempotents"),
                                         (False, True, "en mémoire, idempotents")):
            pannes, duree, r = essai(journal, idempotent)
            print(f"   {nom:31} {pannes:6} {r['terminee']:5} {r['annulee']:5}"
                  f" {r['refusee']:5} {r['incoherentes']:6} {r['doubles']:6}")
            print(f"      {duree:.1f} s ; commandes oubliées : {r['oubliees']} ;"
                  f" exemplaires perdus : {r['perdus']}")
            print(f"      refus de stock : {r['refus']}, dont {r['refus_evitables']}"
                  f" pendant une réservation annulée ensuite")
    finally:
        commun.supprimer("pg1")


if __name__ == "__main__":
    main()
