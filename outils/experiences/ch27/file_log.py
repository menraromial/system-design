"""Chapitre 27 : une file RabbitMQ et un log Kafka, côte à côte.

  1. RabbitMQ : 20 000 messages durables de 200 octets publiés avec accusé de
     publication dans une file ; un consommateur A les lit et les acquitte ; un
     consommateur B arrive ensuite. Puis une diffusion (échange fanout) vers
     deux files abonnées.
  2. Kafka : 100 000 messages de 200 octets dans un sujet (topic) de 6
     partitions, avec une clé (1 000 utilisateurs) et acks=all ; deux groupes
     de consommateurs A et B les lisent ; A relit après avoir remis ses
     positions (offsets) au début ; ce qui reste dans le log.
  3. Kafka, un groupe de 3 consommateurs : partitions attribuées, ordre des
     messages d'un même utilisateur, ordre global ; puis 7 consommateurs pour
     6 partitions.
  4. Kafka, rééquilibrage : un producteur envoie 2 000 messages par seconde ;
     3 consommateurs lisent, dont un lent (3 s par message) ; un quatrième
     rejoint le groupe au bout de 8 s. Plus longue pause des consommateurs
     rapides, avec le protocole de groupe classique puis avec le protocole
     « consumer » (KIP-848).
  Le désordre par clé est aussi mesuré sans producteur idempotent.
Prérequis : docker, images rabbitmq:4-management et apache/kafka,
pip install -r requirements.txt. Usage : python3 file_log.py
"""
import threading
import time

import pika
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic

import commun

CORPS = b"x" * 200


def rabbitmq():
    params = pika.URLParameters(commun.RABBIT)
    cx = pika.BlockingConnection(params)
    ch = cx.channel()
    ch.confirm_delivery()
    ch.queue_declare("travaux", durable=True)
    t = time.perf_counter()
    for _ in range(20_000):
        ch.basic_publish("", "travaux", CORPS, pika.BasicProperties(delivery_mode=2))
    d = time.perf_counter() - t
    print(f"   20 000 messages publiés en {d:.1f} s ({commun.nombre(20_000 / d)} /s), un accusé par message")

    def consommer(nom):
        c = pika.BlockingConnection(params)
        k = c.channel()
        k.basic_qos(prefetch_count=200)
        n, t = 0, time.perf_counter()
        for methode, _, _ in k.consume("travaux", inactivity_timeout=1):
            if methode is None:
                break
            k.basic_ack(methode.delivery_tag)
            n += 1
        d = time.perf_counter() - t - 1
        k.cancel()
        c.close()
        return n, d

    n, d = consommer("A")
    print(f"   consommateur A : {commun.nombre(n)} messages lus et acquittés ({commun.nombre(n / d)} /s)")
    restants = ch.queue_declare("travaux", durable=True, passive=True).method.message_count
    n, _ = consommer("B")
    print(f"   messages restant dans la file : {restants} ; consommateur B ensuite : {n} message")

    ch.exchange_declare("diffusion", exchange_type="fanout")
    for q in ("abonne1", "abonne2"):
        ch.queue_declare(q, durable=True)
        ch.queue_bind(q, "diffusion")
    for _ in range(1000):
        ch.basic_publish("diffusion", "", CORPS)
    comptes = [ch.queue_declare(q, durable=True, passive=True).method.message_count for q in ("abonne1", "abonne2")]
    print(f"   diffusion de 1 000 messages vers 2 files abonnées : {comptes[0]} et {comptes[1]}")
    cx.close()


def lire_tout(groupe, sujet, attendus, delai=60, depuis_debut=False):
    c = Consumer({"bootstrap.servers": commun.KAFKA, "group.id": groupe, "auto.offset.reset": "earliest",
                  "enable.auto.commit": True})
    if depuis_debut:
        parts = [TopicPartition(sujet, p, OFFSET_BEGINNING) for p in range(6)]
        c.assign(parts)
    else:
        c.subscribe([sujet])
    n, t, fin = 0, None, time.monotonic() + delai
    while n < attendus and time.monotonic() < fin:
        m = c.poll(0.5)
        if m is None or m.error():
            if t is not None and n and m is None:
                break
            continue
        t = t or time.perf_counter()
        n += 1
    d = time.perf_counter() - t if t else 0
    if depuis_debut:
        c.commit(asynchronous=False)
    c.close()
    return n, d


def kafka_bases(admin):
    admin.create_topics([NewTopic("evenements", num_partitions=6, replication_factor=1)])["evenements"].result()
    p = Producer({"bootstrap.servers": commun.KAFKA, "acks": "all", "linger.ms": 5, "enable.idempotence": True})
    t = time.perf_counter()
    for i in range(100_000):
        u = i % 1000
        p.produce("evenements", key=f"u{u}", value=b"%d %d " % (i, i // 1000) + CORPS[:190])
        if i % 10_000 == 0:
            p.poll(0)
    p.flush()
    d = time.perf_counter() - t
    print(f"   100 000 messages produits en {d:.1f} s ({commun.nombre(100_000 / d)} /s),")
    print("   acks=all, producteur idempotent")
    for g in ("A", "B"):
        n, d = lire_tout(g, "evenements", 100_000)
        print(f"   groupe {g} : {commun.nombre(n)} messages lus ({commun.nombre(n / d)} /s)")
    n, _ = lire_tout("A", "evenements", 100_000, delai=5)
    print(f"   groupe A, de nouveau : {n} message (ses positions sont enregistrées)")
    n, _ = lire_tout("A", "evenements", 100_000, depuis_debut=True)
    print(f"   groupe A, positions remises au début : {commun.nombre(n)} messages relus")
    c = Consumer({"bootstrap.servers": commun.KAFKA, "group.id": "inspection"})
    total = sum(c.get_watermark_offsets(TopicPartition("evenements", k))[1] for k in range(6))
    c.close()
    taille = commun.docker("exec", "cours-sd-ch27-kafka", "sh", "-c",
                           "du -cb /var/lib/kafka/journal/evenements-*/*.log | tail -1", check=False).stdout.split()[0]
    print(f"   messages toujours dans le log : {commun.nombre(total)}, {int(taille) / 2**20:.0f} Mo de segments sur le disque")


def groupe(nom, nombre, attendus, sujet="evenements"):
    """nombre consommateurs dans un groupe ; chacun note ses partitions et ses messages."""
    recus = [[] for _ in range(nombre)]
    parts = [set() for _ in range(nombre)]
    total, verrou, fin = [0], threading.Lock(), threading.Event()

    def lire(k):
        c = Consumer({"bootstrap.servers": commun.KAFKA, "group.id": nom, "auto.offset.reset": "earliest"})
        c.subscribe([sujet])
        while not fin.is_set():
            m = c.poll(0.2)
            if m is None or m.error():
                continue
            parts[k].add(m.partition())
            i, seq = m.value().split()[:2]
            recus[k].append((time.perf_counter(), int(i), m.key().decode(), int(seq)))
            with verrou:
                total[0] += 1
                if total[0] >= attendus:
                    fin.set()
        c.close()

    fils = [threading.Thread(target=lire, args=(k,)) for k in range(nombre)]
    for f in fils:
        f.start()
    fin.wait(120)
    fin.set()
    for f in fils:
        f.join()
    return recus, parts


def kafka_groupes():
    recus, parts = groupe("C", 3, 100_000)
    for k in range(3):
        print(f"   consommateur {k + 1} : partitions {sorted(parts[k])}, {commun.nombre(len(recus[k]))} messages")
    inversions_cle, doublons, vus = 0, 0, set()
    for _, i, cle, seq in sorted(m for liste in recus for m in liste):
        if i in vus:                  # relu après un rééquilibrage du groupe
            doublons += 1
            continue
        vus.add(i)
    for liste in recus:
        dernier, deja = {}, set()
        for _, i, cle, seq in liste:
            if i in deja:
                continue
            deja.add(i)
            if seq < dernier.get(cle, -1):
                inversions_cle += 1
            dernier[cle] = max(seq, dernier.get(cle, -1))
    tous = sorted(m for liste in recus for m in liste)
    inversions = sum(1 for a, b in zip(tous, tous[1:]) if b[1] < a[1])
    print(f"   messages lus deux fois : {doublons}")
    print(f"   messages d'un même utilisateur lus dans le désordre : {inversions_cle}")
    print(f"   messages lus avant un message produit plus tôt (ordre global) : {commun.nombre(inversions)}")
    recus, parts = groupe("D", 7, 100_000)
    vides = sum(1 for p in parts if not p)
    print(f"   7 consommateurs pour 6 partitions : {vides} consommateur sans partition")
    admin = AdminClient({"bootstrap.servers": commun.KAFKA})
    admin.create_topics([NewTopic("sans-idempotence", num_partitions=6, replication_factor=1)])[
        "sans-idempotence"].result()
    p = Producer({"bootstrap.servers": commun.KAFKA, "acks": "all", "linger.ms": 5})
    p.list_topics("sans-idempotence", timeout=5)
    for i in range(100_000):
        p.produce("sans-idempotence", key=f"u{i % 1000}", value=b"%d %d " % (i, i // 1000))
        if i % 10_000 == 0:
            p.poll(0)
    p.flush()
    c = Consumer({"bootstrap.servers": commun.KAFKA, "group.id": "verif", "auto.offset.reset": "earliest"})
    c.assign([TopicPartition("sans-idempotence", k, 0) for k in range(6)])
    n, inv, dernier = 0, 0, {}
    while n < 100_000:
        m = c.poll(1)
        if m is None or m.error():
            continue
        n += 1
        cle, seq = m.key(), int(m.value().split()[1])
        inv += seq < dernier.get(cle, -1)
        dernier[cle] = max(seq, dernier.get(cle, -1))
    c.close()
    print("   même production sans producteur idempotent :")
    print(f"   {commun.nombre(inv)} messages d'un même utilisateur lus dans le désordre")


def reequilibrage(protocole):
    """3 consommateurs, dont un lent (3 s par message) ; un quatrième rejoint le groupe au
    bout de 8 s. On mesure la plus longue pause des consommateurs rapides."""
    sujet = f"flux-{protocole}"
    admin = AdminClient({"bootstrap.servers": commun.KAFKA})
    admin.create_topics([NewTopic(sujet, num_partitions=6, replication_factor=1)])[sujet].result()
    arret = threading.Event()
    lus = []
    verrou = threading.Lock()

    def produire():
        p = Producer({"bootstrap.servers": commun.KAFKA, "linger.ms": 1})
        i = 0
        t0 = time.perf_counter()
        while not arret.is_set():
            p.produce(sujet, key=f"u{i % 1000}", value=CORPS)
            i += 1
            p.poll(0)
            attente = t0 + i / 2000 - time.perf_counter()
            if attente > 0:
                time.sleep(attente)
        p.flush()

    def consommer(lent):
        conf = {"bootstrap.servers": commun.KAFKA, "group.id": sujet, "auto.offset.reset": "latest",
                "max.poll.interval.ms": 60000}
        if protocole == "consumer":
            conf["group.protocol"] = "consumer"
        c = Consumer(conf)
        c.subscribe([sujet])
        while not arret.is_set():
            m = c.poll(0.05)
            if m is None or m.error():
                continue
            if lent:
                time.sleep(3)                  # traitement long d'un message
            else:
                with verrou:
                    lus.append(time.perf_counter())
        c.close()

    fils = ([threading.Thread(target=produire)] + [threading.Thread(target=consommer, args=(k == 0,))
                                                     for k in range(3)])
    for f in fils:
        f.start()
    time.sleep(8)
    arrivee = time.perf_counter()
    quatrieme = threading.Thread(target=consommer, args=(False,))
    quatrieme.start()
    time.sleep(15)
    arret.set()
    for f in fils + [quatrieme]:
        f.join()
    apres = sorted(t for t in lus if t > arrivee)
    pause = max(b - a for a, b in zip([arrivee] + apres, apres)) if apres else float("nan")
    avant = sorted(t for t in lus if arrivee - 4 < t <= arrivee)
    pause_avant = max(b - a for a, b in zip(avant, avant[1:])) if len(avant) > 1 else float("nan")
    print(f"   protocole {protocole:<9}: pause des rapides {1000 * pause:>5.0f} ms"
          f" (avant l'arrivée : {1000 * pause_avant:.0f} ms)")


def main():
    try:
        version = commun.lancer_rabbitmq()
        print(f"1. RabbitMQ {version}, une file durable")
        rabbitmq()
        commun.supprimer("rabbitmq")
        version = commun.lancer_kafka()
        print(f"\n2. Kafka {version}, un sujet de 6 partitions")
        kafka_bases(AdminClient({"bootstrap.servers": commun.KAFKA}))
        print("\n3. Kafka, un groupe de 3 consommateurs sur le même sujet")
        kafka_groupes()
        print("\n4. Kafka, un quatrième consommateur rejoint le groupe ; 2 000 messages/s,")
        print("   3 consommateurs dont un lent (3 s par message)")
        for protocole in ("classic", "consumer"):
            reequilibrage(protocole)
    finally:
        commun.supprimer("rabbitmq", "kafka")


if __name__ == "__main__":
    main()
