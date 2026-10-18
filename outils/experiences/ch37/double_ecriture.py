"""Chapitre 37 : la double écriture (base + messages) et la boîte d'envoi (outbox).

Un service de commandes enregistre 2 000 commandes dans PostgreSQL et doit
annoncer chacune par un message Kafka. Une commande sur 50 a un montant négatif,
que la base refuse (contrainte CHECK). On tue le service par SIGKILL toutes les
0,5 à 1,5 s ; il redémarre et reprend à la commande suivante, la commande en
cours au moment de la panne étant abandonnée, comme une requête dont le client
a reçu une erreur. Variantes :
- valider la transaction, puis publier le message ;
- publier, puis valider ;
- boîte d'envoi : la commande et son événement sont écrits dans la même
  transaction ; un relais, tué lui aussi, lit la table par scrutation, publie et
  marque les lignes envoyées ;
- boîte d'envoi lue par capture des changements (décodage logique de PostgreSQL,
  test_decoding) : le relais confirme sa position après chaque publication.
On compare ensuite les commandes de la base et les messages du sujet.

2. délai entre la validation et l'arrivée du message, sans panne ;
3. journal (WAL) retenu par un emplacement de réplication quand le lecteur de
   la capture est arrêté.

    python3 double_ecriture.py
"""
import json
import random
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import commun

ICI = Path(__file__).parent
CACHE = Path.home() / ".cache" / "cours-sd-ch37"
P1 = commun.PG["pg1"]
N = 2000

SERVICE = """
import json, sys, time
from pathlib import Path
sys.path.insert(0, ".")
import psycopg, commun
from confluent_kafka import Producer
mode, sujet, n, rythme = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
marque = Path(sys.argv[5])
c = commun.connecter(commun.PG["pg1"])
p = Producer({"bootstrap.servers": commun.KAFKA, "acks": "all", "linger.ms": 0})
debut = int(marque.read_text()) + 1 if marque.exists() else 1
print("pret", flush=True)

def publier(i, charge):
    p.produce(sujet, key=f"commande-{i}", value=charge)
    p.flush()

def marquer(i):                          # écriture atomique : une panne ne laisse
    tmp = marque.with_suffix(".tmp")     # jamais un fichier vide
    tmp.write_text(str(i))
    tmp.replace(marque)

for i in range(debut, n + 1):
    marquer(i)                           # la commande en cours ; abandonnée si panne
    montant = -1 if i % 50 == 0 else 10 + i % 90
    t = time.time()
    try:
        charge = json.dumps({"id": i, "t": t})     # t : juste avant la validation
        if mode == "valider puis publier":
            c.execute("insert into commandes values (%s, %s)", (i, montant))
            publier(i, charge)
        elif mode == "publier puis valider":
            publier(i, charge)
            c.execute("insert into commandes values (%s, %s)", (i, montant))
        else:
            with c.transaction():
                c.execute("insert into commandes values (%s, %s)", (i, montant))
                c.execute("insert into outbox (evenement_id, charge) values (%s, %s)",
                          (f"commande-{i}", charge))
    except psycopg.errors.CheckViolation:
        pass                                 # commande refusée par la base
    if rythme:
        time.sleep(max(0.0, rythme - (time.time() - t)))
"""

RELAIS_SCRUTATION = """
import sys, time
sys.path.insert(0, ".")
import commun
from confluent_kafka import Producer
sujet, intervalle = sys.argv[1], float(sys.argv[2])
c = commun.connecter(commun.PG["pg1"])
p = Producer({"bootstrap.servers": commun.KAFKA, "acks": "all", "linger.ms": 0})
print("pret", flush=True)
while True:
    lignes = c.execute("select id, evenement_id, charge from outbox where not envoye"
                       " order by id limit 100").fetchall()
    if not lignes:
        time.sleep(intervalle)
        continue
    for _, ev, charge in lignes:
        p.produce(sujet, key=ev, value=charge)
    p.flush()
    c.execute("update outbox set envoye = true where id = any(%s)", ([l[0] for l in lignes],))
"""

RELAIS_CAPTURE = r"""
import re, sys
sys.path.insert(0, ".")
import psycopg2, psycopg2.extras, commun
from confluent_kafka import Producer
sujet = sys.argv[1]
from pathlib import Path
dernier = Path(sys.argv[2])
p = Producer({"bootstrap.servers": commun.KAFKA, "acks": "all", "linger.ms": 0})
cx = psycopg2.connect(commun.dsn(commun.PG["pg1"]),
                      connection_factory=psycopg2.extras.LogicalReplicationConnection)
cur = cx.cursor()
cur.start_replication(slot_name="relais", decode=True)
motif = re.compile(r"^table public\.outbox: INSERT: .*evenement_id\[text\]:'([^']*)'"
                   r" charge\[text\]:'((?:[^']|'')*)'")
print("pret", flush=True)

def recevoir(m):
    r = motif.match(m.payload)
    if r:
        p.produce(sujet, key=r.group(1), value=r.group(2).replace("''", "'"))
        p.flush()
        dernier.write_text(r.group(1))
    m.cursor.send_feedback(flush_lsn=m.data_start, force=True)
cur.consume_stream(recevoir)
"""


def preparer():
    with commun.connecter(P1) as c:
        c.execute("select pg_drop_replication_slot(slot_name) from pg_replication_slots")
        c.execute("drop table if exists commandes, outbox")
        c.execute("create table commandes (id int primary key,"
                  " montant int not null check (montant > 0))")
        c.execute("create table outbox (id bigserial primary key, evenement_id text not null,"
                  " charge text not null, envoye boolean not null default false)")


def lancer(code, *args):
    p = subprocess.Popen([sys.executable, "-c", code, *map(str, args)],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, cwd=ICI)
    p.stdout.readline()
    return p


DERNIER = CACHE / "dernier-publie"


def relais_a_jour(mode):
    with commun.connecter(P1) as c:
        if mode == "scrutation":
            return c.execute("select count(*) = 0 from outbox where not envoye").fetchone()[0]
        dernier = c.execute("select evenement_id from outbox order by id desc limit 1"
                            ).fetchone()
    return dernier is None or (DERNIER.exists() and DERNIER.read_text() == dernier[0])


def essai(mode):
    preparer()
    sujet = f"commandes-{re.sub('[^a-z]+', '-', mode)}-{time.time_ns()}"
    commun.creer_sujet(sujet)
    marque = CACHE / "marque"
    marque.unlink(missing_ok=True)
    relais_mode = {"outbox, scrutation": "scrutation", "outbox, capture": "capture"}.get(mode)
    if relais_mode == "capture":
        with commun.connecter(P1) as c:
            c.execute("select pg_create_logical_replication_slot('relais', 'test_decoding')")
    service_mode = "outbox" if relais_mode else mode
    pannes = {"service": 0, "relais": 0}

    def demarrer_relais():
        if relais_mode == "scrutation":
            return lancer(RELAIS_SCRUTATION, sujet, 0.1)
        return lancer(RELAIS_CAPTURE, sujet, DERNIER)
    DERNIER.unlink(missing_ok=True)
    relais = demarrer_relais() if relais_mode else None
    service = lancer(SERVICE, service_mode, sujet, N, 0, marque)
    prochaine = {"service": time.monotonic() + random.uniform(0.5, 1.5),
                 "relais": time.monotonic() + random.uniform(0.5, 1.5)}
    while service.poll() is None or (relais and not relais_a_jour(relais_mode)):
        maintenant = time.monotonic()
        if service.poll() not in (None, 0):
            raise RuntimeError(f"le service s'est arrêté seul (code {service.returncode})")
        if service.poll() is None and maintenant >= prochaine["service"]:
            service.send_signal(signal.SIGKILL)
            service.wait()
            pannes["service"] += 1
            service = lancer(SERVICE, service_mode, sujet, N, 0, marque)
            prochaine["service"] = time.monotonic() + random.uniform(0.5, 1.5)
        if relais and maintenant >= prochaine["relais"]:
            relais.send_signal(signal.SIGKILL)
            relais.wait()
            pannes["relais"] += 1
            relais = demarrer_relais()
            prochaine["relais"] = time.monotonic() + random.uniform(0.5, 1.5)
        time.sleep(0.02)
    if relais:
        relais.kill()
        relais.wait()
    with commun.connecter(P1) as c:
        commandes = {r[0] for r in c.execute("select id from commandes")}
        c.execute("select pg_drop_replication_slot(slot_name) from pg_replication_slots")
    messages = [int(cle.removeprefix("commande-")) for cle, _, _ in commun.lire_sujet(sujet)]
    vus = set(messages)
    return {"pannes": pannes, "commandes": len(commandes),
            "perdus": len(commandes - vus), "fantomes": len(vus - commandes),
            "doublons": len(messages) - len(vus), "messages": len(messages)}


def partie_pannes():
    print(f"1. {N} commandes, service tué toutes les 0,5 à 1,5 s ;"
          f" une sur 50 refusée")
    print(f"   {'mode':22} {'pannes':>8} {'commandes':>9} {'perdus':>7}"
          f" {'fantômes':>8} {'doublons':>8}")
    for mode in ("valider puis publier", "publier puis valider",
                 "outbox, scrutation", "outbox, capture"):
        r = essai(mode)
        pannes = f"{r['pannes']['service']}+{r['pannes']['relais']}"
        print(f"   {mode:22} {pannes:>8} {r['commandes']:9} {r['perdus']:7}"
              f" {r['fantomes']:8} {r['doublons']:8}")
    print("   (pannes : du service + du relais)")


def mesurer_delai(mode, intervalle=0.1):
    """500 commandes à 50 par seconde ; délai entre la validation, notée dans la
    charge, et la réception par un consommateur."""
    from confluent_kafka import Consumer
    preparer()
    sujet = f"delai-{time.time_ns()}"
    commun.creer_sujet(sujet)
    marque = CACHE / "marque"
    marque.unlink(missing_ok=True)
    cons = Consumer({"bootstrap.servers": commun.KAFKA, "group.id": sujet, "log_level": 0,
                     "auto.offset.reset": "earliest", "fetch.wait.max.ms": 10})
    cons.subscribe([sujet])
    for _ in range(20):                         # attendre l'affectation de la partition
        cons.poll(0.2)
    relais = None
    if mode == "capture":
        with commun.connecter(P1) as c:
            c.execute("select pg_create_logical_replication_slot('relais', 'test_decoding')")
        relais = lancer(RELAIS_CAPTURE, sujet, DERNIER)
    elif mode == "scrutation":
        relais = lancer(RELAIS_SCRUTATION, sujet, intervalle)
    service = lancer(SERVICE, "outbox" if relais else "valider puis publier", sujet, 500,
                     0.02, marque)
    delais = []
    fin = time.monotonic() + 30
    while len(delais) < 490 and time.monotonic() < fin:
        m = cons.poll(0.1)
        if m is not None and not m.error():
            delais.append((time.time() - json.loads(m.value())["t"]) * 1000)
    cons.close()
    for p in (service, relais):
        if p:
            p.kill()
            p.wait()
    with commun.connecter(P1) as c:
        c.execute("select pg_drop_replication_slot(slot_name) from pg_replication_slots")
    return commun.centiles(delais)


def partie_delai():
    print("\n2. validation puis réception, 500 commandes à 50 par seconde")
    print(f"   {'mode':36} {'médiane':>9} {'99e c.':>9}")
    for mode, intervalle, nom in (("direct", 0, "publication après la validation"),
                                  ("scrutation", 0.1, "outbox, scrutation toutes les 100 ms"),
                                  ("scrutation", 1.0, "outbox, scrutation chaque seconde"),
                                  ("capture", 0, "outbox, capture des changements")):
        med, p99 = mesurer_delai(mode, intervalle)
        print(f"   {nom:36} {med:7.1f}ms {p99:7.1f}ms")


def partie_retention():
    print("\n3. emplacement de capture dont le lecteur est arrêté")
    preparer()
    with commun.connecter(P1) as c:
        def etat():
            ligne = c.execute(
                "select pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), wal_status"
                " from pg_replication_slots where slot_name = 'relais'").fetchone()
            taille = c.execute("select sum(size) from pg_ls_waldir()").fetchone()[0] / 2**20
            if ligne is None or ligne[0] is None:
                return None, ligne[1] if ligne else None, taille
            return ligne[0] / 2**20, ligne[1], taille

        def ecrire(lots, suffixe=""):
            for lot in range(1, lots + 1):
                c.execute("insert into outbox (evenement_id, charge) select 'e-' || i,"
                          " repeat('x', 200) from generate_series(1, 250000) i")
                c.execute("checkpoint")
                ret, statut, taille = etat()
                retenu = f"{ret:6.1f} Mo" if ret is not None else "   ---   "
                print(f"   {lot * 250000:>9} événements : retenu {retenu},"
                      f" pg_wal {taille:4.0f} Mo, {statut}{suffixe}")

        for reglage in ("-1", "200MB"):
            c.execute(f"alter system set max_slot_wal_keep_size = '{reglage}'")
            c.execute("select pg_reload_conf()")
            time.sleep(0.5)
            c.execute("select pg_create_logical_replication_slot('relais', 'test_decoding')")
            print(f"   max_slot_wal_keep_size = {reglage}")
            ecrire(4)
            try:
                # le point de reprise d'un emplacement logique n'avance qu'à un
                # instantané des transactions en cours, confirmé par l'avance suivante
                for _ in range(3):
                    c.execute("select pg_log_standby_snapshot()")
                    c.execute("select pg_replication_slot_advance('relais',"
                              " pg_current_wal_lsn())")
                for _ in range(2):
                    c.execute("checkpoint")
                ret, statut, taille = etat()
                print(f"   lecteur rattrapé : retenu {ret:5.1f} Mo, pg_wal {taille:4.0f} Mo")
            except Exception as e:
                print(f"   lecteur relancé : {str(e).splitlines()[0][:60]}")
            c.execute("select pg_drop_replication_slot('relais')")
            c.execute("truncate outbox")
            c.execute("checkpoint")
        c.execute("alter system reset max_slot_wal_keep_size")
        c.execute("select pg_reload_conf()")


def main():
    CACHE.mkdir(parents=True, exist_ok=True)
    commun.lancer_pg("pg1")
    version = commun.lancer_kafka()
    try:
        with commun.connecter(P1) as c:
            pg = c.execute("show server_version").fetchone()[0]
        print(f"PostgreSQL {pg}, Kafka {version} (un courtier, acks=all)\n")
        partie_pannes()
        partie_delai()
        partie_retention()
    finally:
        commun.supprimer("pg1", "kafka")
        (CACHE / "marque").unlink(missing_ok=True)
        DERNIER.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
