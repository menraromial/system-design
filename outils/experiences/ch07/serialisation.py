"""Chapitre 7 : la même commande encodée en JSON, MessagePack, Protobuf et Avro.

Mesure la taille de l'encodage et le temps moyen d'encodage et de décodage
(un seul cœur, Python ; les bibliothèques Protobuf et fastavro ont un noyau
en C, json et msgpack aussi).
Usage : python3 serialisation.py   (pip install -r requirements.txt)
"""
import gzip
import json
import pathlib
import subprocess
import sys
import tempfile
import timeit
from io import BytesIO

import fastavro
import msgpack

ICI = pathlib.Path(__file__).parent
SORTIE = pathlib.Path(tempfile.mkdtemp())
subprocess.run([sys.executable, "-m", "grpc_tools.protoc", f"-I{ICI}", f"--python_out={SORTIE}",
                str(ICI / "commande.proto")], check=True)
sys.path.insert(0, str(SORTIE))
import commande_pb2  # noqa: E402  (généré ci-dessus)

COMMANDE = {
    "id": 9_481_203_557, "client_id": 51_220_387, "cree_le_ms": 1_791_712_800_000,
    "statut": "payee", "ville": "Lyon", "code_postal": "69003",
    "lignes": [
        {"produit_id": 100_000 + i, "libelle": libelle, "quantite": q, "prix_centimes": p}
        for i, (libelle, q, p) in enumerate([
            ("Cafetière à piston 1 L", 1, 3_490), ("Café en grains 1 kg", 2, 1_890),
            ("Tasse en grès", 4, 850), ("Filtres papier (x100)", 1, 390),
            ("Moulin manuel", 1, 4_590)])
    ],
}

AVRO = fastavro.parse_schema({
    "type": "record", "name": "Commande", "fields": [
        {"name": "id", "type": "long"}, {"name": "client_id", "type": "long"},
        {"name": "cree_le_ms", "type": "long"}, {"name": "statut", "type": "string"},
        {"name": "lignes", "type": {"type": "array", "items": {
            "type": "record", "name": "Ligne", "fields": [
                {"name": "produit_id", "type": "long"}, {"name": "libelle", "type": "string"},
                {"name": "quantite", "type": "int"}, {"name": "prix_centimes", "type": "long"}]}}},
        {"name": "ville", "type": "string"}, {"name": "code_postal", "type": "string"}]})


def avro_encoder(c):
    tampon = BytesIO()
    fastavro.schemaless_writer(tampon, AVRO, c)
    return tampon.getvalue()


def protobuf_encoder(c):
    m = commande_pb2.Commande(**{k: v for k, v in c.items() if k != "lignes"})
    m.lignes.extend(commande_pb2.Ligne(**l) for l in c["lignes"])
    return m.SerializeToString()


FORMATS = {
    "JSON": (lambda c: json.dumps(c, separators=(",", ":"), ensure_ascii=False).encode(),
             lambda b: json.loads(b)),
    "MessagePack": (msgpack.packb, msgpack.unpackb),
    "Protobuf": (protobuf_encoder, lambda b: commande_pb2.Commande.FromString(b)),
    "Avro (sans schéma joint)": (avro_encoder,
                                  lambda b: fastavro.schemaless_reader(BytesIO(b), AVRO)),
}


def microsecondes(f, n=20_000):
    return min(timeit.repeat(f, number=n, repeat=3)) / n * 1e6


def main():
    print(f"{'format':26} {'octets':>7} {'gzip':>6} {'encodage':>10} {'décodage':>10}")
    for nom, (enc, dec) in FORMATS.items():
        b = enc(COMMANDE)
        print(f"{nom:26} {len(b):7d} {len(gzip.compress(b)):6d} "
              f"{microsecondes(lambda: enc(COMMANDE)):8.2f} µs {microsecondes(lambda: dec(b)):8.2f} µs")


if __name__ == "__main__":
    main()
