"""Chapitre 28 : accusés, redélivrance, message empoisonné et ordre avec RabbitMQ.

  1. redélivrance : 1 000 messages dans une file ; un consommateur A en reçoit
     au plus 50 d'avance (prefetch), acquitte les 300 premiers qu'il traite,
     puis sa connexion se ferme sans acquitter le reste ; un consommateur B
     reçoit la suite. Combien de messages B reçoit-il, et combien marqués
     « redelivered » ?
  2. message empoisonné : 99 messages normaux et un message que le
     consommateur ne sait pas traiter (il le refuse et le remet dans la file) :
     - file classique, refus par basic.reject : combien de fois en 5 secondes ?
     - file quorum, limite de livraisons par défaut (20), refus par basic.nack,
       qui ne compte pas comme un échec de livraison ;
     - la même, refus par basic.reject, sans file de lettres mortes ;
     - file quorum, x-delivery-limit = 3, reject, avec une file de lettres mortes.
  3. ordre et parallélisme : 2 000 messages, 20 clés, numérotés par clé et
     publiés par rafales de 5 messages d'une même clé ;
     traitement de 1 à 4 ms. Un consommateur ; quatre consommateurs sur la même
     file ; quatre files choisies par la clé, un consommateur chacune.
     Durée, et messages d'une clé traités avant un message plus ancien.
Prérequis : docker, image rabbitmq:4-management, pip install -r requirements.txt.
Usage : python3 rabbit_garanties.py
"""
import random
import threading
import time
import zlib

import pika

import commun

PARAMS = pika.URLParameters(commun.RABBIT)


def connexion():
    cx = pika.BlockingConnection(PARAMS)
    return cx, cx.channel()


def redelivrance():
    cx, ch = connexion()
    ch.queue_declare("paiements", durable=True, arguments={"x-queue-type": "quorum"})
    for i in range(1000):
        ch.basic_publish("", "paiements", str(i).encode(), pika.BasicProperties(delivery_mode=2))
    a, cha = connexion()
    cha.basic_qos(prefetch_count=50)
    recus_a, acquittes = 0, 0
    for methode, _, corps in cha.consume("paiements"):
        recus_a += 1
        if acquittes < 300:
            cha.basic_ack(methode.delivery_tag)
            acquittes += 1
        if acquittes == 300:
            break
    a.close()                                        # fermée sans acquitter : comme une panne
    b, chb = connexion()
    recus_b, redelivres, ids = 0, 0, set()
    for methode, _, corps in chb.consume("paiements", inactivity_timeout=1):
        if methode is None:
            break
        recus_b += 1
        redelivres += methode.redelivered
        ids.add(int(corps))
        chb.basic_ack(methode.delivery_tag)
    b.close()
    print(f"   A acquitte 300 messages, puis sa connexion se ferme")
    print(f"   B reçoit {recus_b} messages, dont {redelivres} marqués redelivered")
    print("   (déjà remis à A, jamais acquittés)")
    cx.close()


def poison(nom, arguments, refus="reject", duree=5):
    cx, ch = connexion()
    ch.queue_declare(nom, durable=True, arguments=arguments)
    for i in range(100):
        ch.basic_publish("", nom, b"poison" if i == 50 else str(i).encode(), pika.BasicProperties(delivery_mode=2))
    c, chc = connexion()
    chc.basic_qos(prefetch_count=10)
    normaux, livraisons_poison, debut = 0, 0, time.monotonic()
    for methode, props, corps in chc.consume(nom, inactivity_timeout=1):
        if time.monotonic() - debut > duree:
            break
        if methode is None:
            break
        if corps == b"poison":
            livraisons_poison += 1
            if refus == "reject":                                  # échec : remis dans la file
                chc.basic_reject(methode.delivery_tag, requeue=True)
            else:
                chc.basic_nack(methode.delivery_tag, requeue=True)
        else:
            normaux += 1
            chc.basic_ack(methode.delivery_tag)
    c.close()
    time.sleep(0.5)
    reste = ch.queue_declare(nom, durable=True, passive=True, arguments=arguments).method.message_count
    cx.close()
    return normaux, livraisons_poison, reste, time.monotonic() - debut


def messages_empoisonnes():
    n, k, reste, d = poison("classique", {"x-queue-type": "classic"})
    print(f"   file classique, reject : {n} messages normaux traités ;")
    print(f"      message empoisonné reçu {k} fois en {d:.0f} s,")
    print(f"      toujours dans la file ({reste})")
    n, k, reste, d = poison("quorum-nack", {"x-queue-type": "quorum"}, refus="nack")
    print(f"   file quorum, limite par défaut, nack : reçu {k} fois en {d:.0f} s,")
    print(f"      toujours dans la file ({reste})")
    n, k, reste, d = poison("quorum-defaut", {"x-queue-type": "quorum"})
    print(f"   file quorum, limite par défaut, reject, sans DLQ : reçu {k} fois,")
    print(f"      puis supprimé ; messages restant dans la file : {reste}")
    cx, ch = connexion()
    ch.exchange_declare("lettres-mortes", exchange_type="fanout", durable=True)
    ch.queue_declare("dlq", durable=True, arguments={"x-queue-type": "quorum"})
    ch.queue_bind("dlq", "lettres-mortes")
    cx.close()
    n, k, reste, d = poison("quorum-dlq", {"x-queue-type": "quorum", "x-delivery-limit": 3,
                                           "x-dead-letter-exchange": "lettres-mortes"})
    cx, ch = connexion()
    methode, props, corps = ch.basic_get("dlq", auto_ack=True)
    raison = props.headers.get("x-first-death-reason") if props and props.headers else None
    print(f"   file quorum, x-delivery-limit = 3, reject, avec DLQ : reçu {k} fois ;")
    print(f"      dans la DLQ : {corps.decode() if corps else 'rien'} (raison : {raison})")
    cx.close()


def ordre(nb_files, nb_consommateurs, nom):
    cx, ch = connexion()
    files = [f"{nom}-{k}" for k in range(nb_files)]
    for f in files:
        ch.queue_declare(f, durable=True)
    suivant = {}
    for i in range(2000):
        cle = (i // 5) % 20                  # les messages d'une clé arrivent par rafales de 5
        seq = suivant.get(cle, 0)
        suivant[cle] = seq + 1
        f = files[zlib.crc32(str(cle).encode()) % nb_files]
        ch.basic_publish("", f, f"{cle} {seq}".encode())
    cx.close()
    traites, verrou = [], threading.Lock()

    def consommer(f):
        alea = random.Random(f)
        c, chc = connexion()
        chc.basic_qos(prefetch_count=10)
        for methode, _, corps in chc.consume(f, inactivity_timeout=1):
            if methode is None:
                break
            time.sleep(alea.uniform(0.001, 0.004))             # traitement de 1 à 4 ms
            cle, seq = map(int, corps.split())
            with verrou:
                traites.append((cle, seq))
            chc.basic_ack(methode.delivery_tag)
        c.close()

    cibles = [files[k % nb_files] for k in range(nb_consommateurs)]
    fils = [threading.Thread(target=consommer, args=(f,)) for f in cibles]
    t = time.perf_counter()
    for f in fils:
        f.start()
    for f in fils:
        f.join()
    d = time.perf_counter() - t - 1                            # moins l'attente finale d'1 s
    dernier, inversions = {}, 0
    for cle, seq in traites:
        if seq < dernier.get(cle, -1):
            inversions += 1
        dernier[cle] = max(seq, dernier.get(cle, -1))
    return d, inversions, len(traites)


def main():
    try:
        version = commun.lancer_rabbitmq()
        print(f"RabbitMQ {version}")
        print("\n1. redélivrance après une panne du consommateur (quorum, prefetch 50)")
        redelivrance()
        print("\n2. un message empoisonné parmi 100, refusé et remis dans la file")
        messages_empoisonnes()
        print("\n3. ordre : 2 000 messages, 20 clés par rafales de 5, traitement de 1 à 4 ms")
        print(f"   {'configuration':<42}{'durée':>8}{'désordre':>10}")
        for nb_files, nb_cons, nom, titre in ((1, 1, "un", "1 file, 1 consommateur"),
                                             (1, 4, "partage", "1 file, 4 consommateurs"),
                                             (4, 4, "parcle", "4 files par clé, 1 consommateur par file")):
            d, inv, n = ordre(nb_files, nb_cons, nom)
            print(f"   {titre:<42}{d:>6.1f} s{inv:>10}")
    finally:
        commun.supprimer("rabbitmq")


if __name__ == "__main__":
    main()
