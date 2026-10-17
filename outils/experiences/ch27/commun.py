"""Chapitre 27 : un courtier RabbitMQ et un courtier Kafka, dans des conteneurs.

  - cours-sd-ch27-rabbitmq : RabbitMQ 4, AMQP sur le port 10672, interface
    d'administration HTTP sur 10673 (utilisateur guest, mot de passe guest) ;
  - cours-sd-ch27-kafka : Kafka en mode KRaft, un seul nœud qui est à la fois
    courtier et contrôleur, en réseau hôte, sur le port 10792.
Les conteneurs sont supprimés par leur nom, avec leurs volumes.
"""
import json
import subprocess
import time
import urllib.request

RABBIT = "amqp://guest:guest@127.0.0.1:10672/"
KAFKA = "127.0.0.1:10792"


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def supprimer(*noms):
    subprocess.run(["docker", "rm", "-f", "-v", *(f"cours-sd-ch27-{n}" for n in noms)], capture_output=True)


def lancer_rabbitmq(delai=120):
    supprimer("rabbitmq")
    docker("run", "-d", "--name", "cours-sd-ch27-rabbitmq", "-p", "10672:5672", "-p", "10673:15672",
           "rabbitmq:4-management")
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            version = rabbit_api("overview")["rabbitmq_version"]
            if docker("exec", "cours-sd-ch27-rabbitmq", "rabbitmq-diagnostics", "-q", "check_port_connectivity",
                      check=False).returncode == 0:
                return version
        except OSError:
            pass
        time.sleep(1)
    raise RuntimeError("RabbitMQ ne répond pas")


def rabbit_api(chemin):
    requete = urllib.request.Request(f"http://127.0.0.1:10673/api/{chemin}")
    requete.add_header("Authorization", "Basic Z3Vlc3Q6Z3Vlc3Q=")      # guest:guest
    with urllib.request.urlopen(requete, timeout=5) as r:
        return json.load(r)


def lancer_kafka(delai=120):
    supprimer("kafka")
    env = {
        "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://127.0.0.1:10792,CONTROLLER://127.0.0.1:10793",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{KAFKA}",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@127.0.0.1:10793",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_HEAP_OPTS": "-Xmx768m -Xms768m",
        "KAFKA_LOG_DIRS": "/var/lib/kafka/journal",
    }
    args = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
    docker("run", "-d", "--name", "cours-sd-ch27-kafka", "--network", "host", *args, "apache/kafka:latest")
    from confluent_kafka.admin import AdminClient
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        try:
            AdminClient({"bootstrap.servers": KAFKA}).list_topics(timeout=2)
            return docker("exec", "cours-sd-ch27-kafka", "sh", "-c",
                          "ls /opt/kafka/libs | grep -o 'kafka-clients-[0-9.]*' | head -1",
                          check=False).stdout.strip().removeprefix("kafka-clients-").rstrip(".")
        except Exception:
            time.sleep(1)
    raise RuntimeError("Kafka ne répond pas")


def nombre(x):
    return f"{x:,.0f}".replace(",", " ")
