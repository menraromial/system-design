"""Chapitre 48 : Apache Flink 2.1 et Kafka 4.3 en conteneurs, sur le flux de clics
d'evenements.py. Une tâche Flink SQL compte les clics par page sur des fenêtres
basculantes de 10 s en temps d'événement, avec un filigrane (watermark) et des
points de contrôle toutes les 2 s, et écrit ses résultats dans Kafka.

  1. en direct (90 s à 1 000 événements par seconde) : filigrane de 5 s en
     exactly-once, en at-least-once, puis filigrane de 30 s ; événements perdus
     car arrivés après la fermeture de leur fenêtre, et attente des résultats ;
  2. relecture d'un sujet déjà rempli (2 millions d'événements) : deux fois
     avec un filigrane de 5 s, puis avec un filigrane de 60 s (plus que le
     retard maximal) sans panne, et avec une panne du TaskManager (SIGKILL quand la
     source a lu 30 % des événements, d'après ses métriques rafraîchies toutes
     les 500 ms, relancé 3 s plus tard) en exactly-once et en
     at-least-once ; résultats en
     double et fenêtres dont le compte diffère du compte exact.

Les résultats sont lus en read_committed. Prérequis : images flink:2.1-java17 et
apache/kafka:latest, connecteur Kafka de Flink dans ~/.cache/cours-sd-ch48/lib
(voir CONNECTEUR ci-dessous) ; confluent-kafka dans l'environnement Python.

    python3 flux_flink.py        (environ 15 minutes)
"""
import json
import statistics
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

from confluent_kafka import Consumer, Producer

from evenements import generer, verite

ICI = Path(__file__).resolve().parent
CACHE = Path.home() / ".cache" / "cours-sd-ch48"
CONNECTEUR = ("https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/"
              "5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar")
JAR = CACHE / "lib" / "flink-sql-connector-kafka-5.0.0-2.1.jar"
RESEAU = "cours-sd-ch48-reseau"
KAFKA_HOTE = "localhost:12892"
REST = "http://localhost:12881"
FENETRE = 10


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def docker(*a, check=True):
    r = subprocess.run(["docker", *a], capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(f"docker {' '.join(a[:3])} : {r.stderr.strip()[:300]}")
    return r


def rest(chemin, methode="GET"):
    req = urllib.request.Request(REST + chemin, method=methode)
    with urllib.request.urlopen(req, timeout=10) as r:
        corps = r.read()
        return json.loads(corps) if corps else None


# ---- infrastructure ------------------------------------------------------------------------
def nettoyer():
    for n in ("tm", "jm", "kafka"):
        docker("rm", "-f", "-v", f"cours-sd-ch48-{n}", check=False)
    docker("network", "rm", RESEAU, check=False)
    docker("run", "--rm", "-v", f"{CACHE}:/c", "--entrypoint", "sh", "flink:2.1-java17", "-c",
           "rm -rf /c/ckpt", check=False)


def preparer():
    nettoyer()
    if not JAR.exists():
        JAR.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(CONNECTEUR, JAR)
    (CACHE / "ckpt").mkdir(parents=True, exist_ok=True)
    (CACHE / "ckpt").chmod(0o777)
    docker("network", "create", RESEAU)
    env = {"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
           "KAFKA_LISTENERS": "INTERNE://:9092,EXTERNE://:12892,CONTROLLER://:9093",
           "KAFKA_ADVERTISED_LISTENERS": "INTERNE://cours-sd-ch48-kafka:9092,EXTERNE://localhost:12892",
           "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNE:PLAINTEXT,EXTERNE:PLAINTEXT,CONTROLLER:PLAINTEXT",
           "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNE", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
           "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
           "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
           "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
           "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
           "KAFKA_LOG_DIRS": "/tmp/kraft-logs"}
    a = []
    for k, v in env.items():
        a += ["-e", f"{k}={v}"]
    docker("run", "-d", "--name", "cours-sd-ch48-kafka", "--network", RESEAU, "-p", "12892:12892",
           "--memory=3g", *a, "apache/kafka:latest")
    props = ("jobmanager.rpc.address: cours-sd-ch48-jm\ntaskmanager.numberOfTaskSlots: 2\n"
             "execution.checkpointing.dir: file:///ckpt\n"
             "heartbeat.interval: 1000\nheartbeat.timeout: 5000\n"
             "metrics.fetcher.update-interval: 500\n")
    for nom, role, port in (("jm", "jobmanager", ["-p", "12881:8081"]), ("tm", "taskmanager", [])):
        docker("run", "-d", "--name", f"cours-sd-ch48-{nom}", "--network", RESEAU, *port, "--memory=2g",
               "-v", f"{JAR}:/opt/flink/lib/flink-sql-connector-kafka.jar:ro",
               "-v", f"{CACHE / 'ckpt'}:/ckpt", "-v", f"{ICI}:/job:ro",
               "-e", f"FLINK_PROPERTIES={props}", "flink:2.1-java17", role)
    for _ in range(120):
        try:
            if rest("/overview")["slots-available"] >= 2:
                break
        except OSError:
            pass
        time.sleep(1)
    for _ in range(60):
        if docker("exec", "cours-sd-ch48-kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                  "localhost:9092", "--list", check=False).returncode == 0:
            break
        time.sleep(1)


def sujet(nom):
    docker("exec", "cours-sd-ch48-kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
           "localhost:9092", "--create", "--topic", nom, "--partitions", "1", "--replication-factor", "1")


def soumettre(nom, entree, sortie, filigrane, garantie):
    sql = f"""SET 'execution.checkpointing.interval' = '2s';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = '{nom}';
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '20';
SET 'restart-strategy.fixed-delay.delay' = '1s';
CREATE TABLE clics (page STRING, ts_ms BIGINT, ts AS TO_TIMESTAMP_LTZ(ts_ms, 3),
  WATERMARK FOR ts AS ts - INTERVAL '{filigrane}' SECOND)
WITH ('connector'='kafka','topic'='{entree}','properties.bootstrap.servers'='cours-sd-ch48-kafka:9092',
  'properties.group.id'='{nom}','scan.startup.mode'='earliest-offset','format'='json');
CREATE TABLE comptes (page STRING, debut TIMESTAMP(3), n BIGINT)
WITH ('connector'='kafka','topic'='{sortie}','properties.bootstrap.servers'='cours-sd-ch48-kafka:9092',
  'format'='json','sink.delivery-guarantee'='{garantie}','sink.transactional-id-prefix'='{nom}',
  'properties.transaction.timeout.ms'='600000');
INSERT INTO comptes SELECT page, window_start, COUNT(*)
FROM TABLE(TUMBLE(TABLE clics, DESCRIPTOR(ts), INTERVAL '{FENETRE}' SECOND))
GROUP BY page, window_start, window_end;
"""
    (CACHE / f"{nom}.sql").write_text(sql)
    docker("cp", str(CACHE / f"{nom}.sql"), f"cours-sd-ch48-jm:/tmp/{nom}.sql")
    r = docker("exec", "cours-sd-ch48-jm", "./bin/sql-client.sh", "-f", f"/tmp/{nom}.sql")
    return r.stdout.split("Job ID:")[1].split()[0]


def lus_par_la_source(job):
    """Événements lus par la source de la tâche (métriques de l'API REST)."""
    try:
        for v in rest(f"/jobs/{job}")["vertices"]:
            if v["name"].startswith("Source"):
                return v["metrics"]["read-records"]
    except (OSError, KeyError):
        pass
    return 0


def annuler(job):
    try:
        rest(f"/jobs/{job}?mode=cancel", "PATCH")
    except OSError:
        pass


class Lecteur(threading.Thread):
    """Lit les résultats validés et note leur heure d'arrivée."""
    def __init__(self, sortie):
        super().__init__(daemon=True)
        self.c = Consumer({"bootstrap.servers": KAFKA_HOTE, "group.id": f"lecteur-{sortie}",
                           "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
        self.c.subscribe([sortie])
        self.res, self.fin = [], False

    def run(self):
        while not self.fin:
            m = self.c.poll(0.2)
            if m is not None and not m.error():
                d = json.loads(m.value())
                self.res.append((time.time(), d["page"], d["debut"], d["n"]))
        self.c.close()


def debut_ms(texte):
    """« 2026-11-15 07:10:20 » (UTC, fuseau de la tâche) -> millisecondes."""
    import datetime
    return int(datetime.datetime.strptime(texte[:19], "%Y-%m-%d %H:%M:%S")
               .replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def producteur():
    return Producer({"bootstrap.servers": KAFKA_HOTE, "linger.ms": 5, "enable.idempotence": True})


# ---- 1. en direct ---------------------------------------------------------------------------
def direct():
    print("1. en direct : 90 s à 1 000 clics par seconde, fenêtres de 10 s")
    print(f"   {'filigrane, garantie':<26}{'produits':>9}{'comptés':>9}{'perdus':>8}{'attente méd.':>13}{'99e':>7}")
    duree = 90
    evts = generer(duree, 1000, graine=481)
    for k, (fil, gar) in enumerate(((5, "exactly-once"), (5, "at-least-once"), (30, "exactly-once"))):
        nom = f"direct{k}"
        sujet(f"clics-{nom}")
        sujet(f"comptes-{nom}")
        job = soumettre(nom, f"clics-{nom}", f"comptes-{nom}", fil, gar)
        lecteur = Lecteur(f"comptes-{nom}")
        lecteur.start()
        time.sleep(3)
        p = producteur()
        t0 = time.time()
        t0_ms = int(t0 * 1000) // 10000 * 10000 + 10000      # fenêtres alignées sur l'horloge
        t0 = t0_ms / 1000
        while time.time() < t0:
            time.sleep(0.001)
        for arr, t, page in evts:
            attente = t0 + arr - time.time()
            if attente > 0:
                time.sleep(attente)
            p.produce(f"clics-{nom}", json.dumps({"page": page, "ts_ms": int(t0_ms + t * 1000)}))
            p.poll(0)
        p.produce(f"clics-{nom}", json.dumps({"page": "fin", "ts_ms": int(t0_ms + (duree + 200) * 1000)}))
        p.flush()
        time.sleep(8)
        lecteur.fin = True
        lecteur.join()
        annuler(job)
        comptes = sum(n for _, page, _, n in lecteur.res if page != "fin")
        # attente des fenêtres fermées naturellement, avant la fin du flux
        attentes = [(arrivee * 1000 - (debut_ms(d) + FENETRE * 1000)) / 1000
                    for arrivee, page, d, _ in lecteur.res
                    if page != "fin" and debut_ms(d) + FENETRE * 1000 <= t0_ms + (duree - fil - 5) * 1000]
        attentes.sort()
        print(f"   {f'{fil} s, {gar}':<26}{nb(len(evts)):>9}{nb(comptes):>9}{nb(len(evts) - comptes):>8}"
              f"{statistics.median(attentes):>11.1f} s{attentes[int(.99 * len(attentes))]:>5.1f} s")


# ---- 2. relecture et pannes ------------------------------------------------------------------
def remplir(nom, evts, t0_ms):
    p = producteur()
    for i, (_, t, page) in enumerate(evts):
        while True:
            try:
                p.produce(nom, json.dumps({"page": page, "ts_ms": int(t0_ms + t * 1000)}))
                break
            except BufferError:                       # file locale pleine : attendre l'envoi
                p.poll(0.1)
        if i % 10000 == 0:
            p.poll(0)
    p.produce(nom, json.dumps({"page": "fin", "ts_ms": int(t0_ms + 10 ** 7)}))
    p.produce(nom, json.dumps({"page": "fin2", "ts_ms": int(t0_ms + 10 ** 7 + 600_000)}))  # ferme « fin »
    p.flush()


def relecture():
    print("\n2. relecture de 2 millions de clics déjà dans Kafka (retard maximal 40 s)")
    evts = generer(2000, 1000, graine=482)
    t0_ms = 1_767_225_600_000
    sujet("clics-relecture")
    remplir("clics-relecture", evts, t0_ms)
    vrai = verite(evts, FENETRE)
    print(f"   {nb(len(evts))} événements, {nb(len(vrai))} fenêtres (page, 10 s)")
    print(f"   {'filigrane, garantie, panne':<30}{'durée':>7}{'résultats':>10}{'doubles':>8}{'≠ vrai':>8}"
          f"{'reprise':>8}")
    cas = [(5, "exactly-once", False), (5, "exactly-once", False), (60, "exactly-once", False),
           (60, "exactly-once", True), (60, "at-least-once", True)]
    duree_ref = None
    for k, (fil, gar, panne) in enumerate(cas):
        nom = f"relecture{k}"
        sujet(f"comptes-{nom}")
        lecteur = Lecteur(f"comptes-{nom}")
        lecteur.start()
        t = time.time()
        job = soumettre(nom, "clics-relecture", f"comptes-{nom}", fil, gar)
        reprise = None
        if panne:
            while lus_par_la_source(job) < 0.3 * len(evts):  # panne vers le milieu du calcul
                time.sleep(0.05)
            avant = lus_par_la_source(job)
            docker("kill", "cours-sd-ch48-tm")
            tk = time.time()
            time.sleep(3)
            docker("start", "cours-sd-ch48-tm")
            while rest(f"/jobs/{job}")["state"] != "RUNNING" or not any(
                    v["status"] == "RUNNING" for v in rest(f"/jobs/{job}")["vertices"]):
                time.sleep(0.2)
            reprise = time.time() - tk
            print(f"   (panne après {nb(avant)} événements lus par la source)")
        while not any(page == "fin" for _, page, _, _ in lecteur.res):    # dernière fenêtre
            time.sleep(0.2)
        duree = time.time() - t
        if fil == 60 and not panne:
            for _ in range(50):                           # métriques rafraîchies toutes les 500 ms
                src = [v for v in rest(f"/jobs/{job}")["vertices"] if v["name"].startswith("Source")][0]
                phases = (src["metrics"]["read-records"], src["metrics"]["write-records"])
                if phases[0] >= len(evts):
                    break
                time.sleep(0.2)
        time.sleep(3)
        lecteur.fin = True
        lecteur.join()
        if fil == 60 and not panne:
            duree_ref = duree
        annuler(job)
        cles = {}
        for _, page, d, n in lecteur.res:
            if not page.startswith("fin"):
                cles.setdefault((page, (debut_ms(d) - t0_ms) // 1000), []).append(n)
        doubles = sum(len(v) - 1 for v in cles.values())
        differents = sum(1 for c, n in vrai.items() if cles.get(c, [None])[-1] != n)
        libelle = f"{fil} s, {gar}{', panne' if panne else ''}"
        print(f"   {libelle:<30}{duree:>6.1f}s{nb(sum(len(v) for v in cles.values())):>10}{nb(doubles):>8}"
              f"{nb(differents):>8}{'' if reprise is None else f'{reprise:.1f} s':>8}")
    print(f"   débit sans panne : {nb(len(evts) / duree_ref)} événements par seconde ;")
    print(f"   agrégation en deux phases : {nb(phases[0])} enregistrements lus par la source,")
    print(f"   {nb(phases[1])} agrégats partiels envoyés à l'agrégation finale")
    print("   doubles : résultats reçus plusieurs fois pour une même fenêtre ;")
    print("   ≠ vrai : fenêtres dont le dernier compte reçu diffère du compte exact ;")
    print("   reprise : de la panne au redémarrage de la tâche")


def main():
    preparer()
    try:
        direct()
        relecture()
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
