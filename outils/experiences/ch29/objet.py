"""Chapitre 29 : un stockage objet S3, comparé à un système de fichiers.

Un serveur SeaweedFS (cours-sd-ch29-seaweed) expose l'API S3 sur le port 10933,
avec une clé d'accès ; ses données sont dans ~/.cache/cours-sd-ch29.
  1. opérations de base : PUT et GET de petits objets (1 Ko), métadonnées,
     liste par préfixe et délimiteur ;
  2. fichier contre objet : modifier 1 octet au milieu de 100 Mo ; renommer un
     « dossier » de 1 000 éléments ;
  3. téléversement en plusieurs parties (multipart) : 256 Mo en une requête, ou
     en parties de 8 Mo envoyées par 4 fils ; ETag ; reprise après une panne à
     la dixième partie (ListParts) ;
  4. URL présignée : lecture et écriture par une simple requête HTTP, sans
     clé, avant et après expiration ;
  5. codage d'effacement : un objet de 300 Mo, codé en Reed-Solomon 10 + 4
     (ec.encode) ; place occupée ; lecture après la perte de 4 fragments,
     puis de 5.
Prérequis : docker, image chrislusf/seaweedfs, pip install -r requirements.txt.
Usage : python3 objet.py
"""
import concurrent.futures
import glob
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

DOSSIER = Path.home() / ".cache" / "cours-sd-ch29"
NOM, PORT = "cours-sd-ch29-seaweed", 10933
CLE, SECRET = "cours", "secret-du-cours"


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def nettoyer():
    subprocess.run(["docker", "rm", "-f", "-v", NOM], capture_output=True)
    if DOSSIER.exists():                         # fichiers écrits par le conteneur
        subprocess.run(["docker", "run", "--rm", "-v", f"{DOSSIER}:/d", "--entrypoint", "sh",
                        "chrislusf/seaweedfs:latest", "-c", "rm -rf /d/*"], capture_output=True)
        shutil.rmtree(DOSSIER, ignore_errors=True)


def client():
    return boto3.client("s3", endpoint_url=f"http://127.0.0.1:{PORT}", aws_access_key_id=CLE,
                        aws_secret_access_key=SECRET, region_name="us-east-1",
                        config=Config(retries={"max_attempts": 1}, read_timeout=120, signature_version="s3v4"))


def attendre(s3, test, delai=90):
    fin = time.monotonic() + delai
    while True:
        try:
            return test(s3)
        except Exception:
            if time.monotonic() > fin:
                raise
            time.sleep(1)


def lancer():
    (DOSSIER / "donnees").mkdir(parents=True)
    (DOSSIER / "s3.json").write_text(json.dumps({"identities": [{
        "name": "cours", "credentials": [{"accessKey": CLE, "secretKey": SECRET}],
        "actions": ["Admin", "Read", "Write", "List", "Tagging"]}]}))
    docker("run", "-d", "--name", NOM, "-p", f"{PORT}:8333",
           "-v", f"{DOSSIER / 'donnees'}:/data", "-v", f"{DOSSIER / 's3.json'}:/etc/s3.json:ro",
           "chrislusf/seaweedfs:latest", "server", "-dir=/data", "-s3", "-s3.config=/etc/s3.json",
           "-volume.max=200", "-master.volumeSizeLimitMB=64")
    s3 = client()
    attendre(s3, lambda c: c.list_buckets())
    return s3


def weed_shell(commandes):
    script = "lock\n" + "\n".join(commandes) + "\nunlock\n"
    return docker("exec", NOM, "sh", "-c", f"printf '{script}' | weed shell -master=localhost:9333",
                  check=False).stdout


def md5(octets):
    return hashlib.md5(octets).hexdigest()


def mediane_ms(f, n):
    durees = []
    for i in range(n):
        t = time.perf_counter()
        f(i)
        durees.append(1000 * (time.perf_counter() - t))
    return statistics.median(durees)


def bases(s3):
    s3.create_bucket(Bucket="photos")
    petit = os.urandom(1024)
    put = mediane_ms(lambda i: s3.put_object(Bucket="photos", Key=f"petits/{i}", Body=petit), 200)
    get = mediane_ms(lambda i: s3.get_object(Bucket="photos", Key=f"petits/{i}")["Body"].read(), 200)
    print(f"   PUT d'un objet de 1 Ko : {put:.1f} ms ; GET : {get:.1f} ms (médianes sur 200)")
    s3.put_object(Bucket="photos", Key="2026/vacances/plage.jpg", Body=b"...", ContentType="image/jpeg",
                  Metadata={"appareil": "x100", "auteur": "romial"})
    tete = s3.head_object(Bucket="photos", Key="2026/vacances/plage.jpg")
    print(f"   métadonnées relues : {tete['ContentType']},")
    print(f"      {tete['Metadata']}")
    for k in range(1000):
        s3.put_object(Bucket="photos", Key=f"2026/{'vacances' if k % 2 else 'travail'}/img{k:04d}.jpg", Body=b"x")
    t = time.perf_counter()
    r = s3.list_objects_v2(Bucket="photos", Prefix="2026/", Delimiter="/")
    d = 1000 * (time.perf_counter() - t)
    print(f"   liste de 2026/ avec le délimiteur / ({d:.0f} ms) :")
    print(f"      {[p['Prefix'] for p in r.get('CommonPrefixes', [])]}")
    pages, n, t = 0, 0, time.perf_counter()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="photos", Prefix="2026/vacances/",
                                                             PaginationConfig={"PageSize": 100}):
        pages += 1
        n += page.get("KeyCount", 0)
    print(f"   liste de 2026/vacances/ : {n} clés en {pages} pages de 100,"
          f" {1000 * (time.perf_counter() - t):.0f} ms")


def fichier_contre_objet(s3):
    chemin = DOSSIER / "fichier.bin"
    contenu = os.urandom(100 * 2**20)
    chemin.write_bytes(contenu)
    s3.put_object(Bucket="photos", Key="gros/fichier.bin", Body=contenu)
    t = time.perf_counter()
    fd = os.open(chemin, os.O_WRONLY)
    os.pwrite(fd, b"Z", 50 * 2**20)
    os.fsync(fd)
    os.close(fd)
    d_fichier = 1000 * (time.perf_counter() - t)
    t = time.perf_counter()
    corps = bytearray(s3.get_object(Bucket="photos", Key="gros/fichier.bin")["Body"].read())
    corps[50 * 2**20] = ord("Z")
    s3.put_object(Bucket="photos", Key="gros/fichier.bin", Body=bytes(corps))
    d_objet = 1000 * (time.perf_counter() - t)
    print(f"   modifier 1 octet au milieu de 100 Mo : fichier {d_fichier:.1f} ms (pwrite + fsync),")
    print(f"      objet {d_objet:.0f} ms (relire et réécrire l'objet entier)")
    dossier = DOSSIER / "ancien"
    dossier.mkdir()
    for k in range(1000):
        (dossier / f"f{k}").write_bytes(b"x")
    t = time.perf_counter()
    os.rename(dossier, DOSSIER / "nouveau")
    d_fichier = 1000 * (time.perf_counter() - t)
    for k in range(1000):
        s3.put_object(Bucket="photos", Key=f"ancien/f{k}", Body=b"x")
    t = time.perf_counter()
    for k in range(1000):
        s3.copy_object(Bucket="photos", Key=f"nouveau/f{k}", CopySource={"Bucket": "photos", "Key": f"ancien/f{k}"})
        s3.delete_object(Bucket="photos", Key=f"ancien/f{k}")
    d_objet = 1000 * (time.perf_counter() - t)
    print(f"   renommer un dossier de 1 000 éléments : fichier {d_fichier:.2f} ms (rename),")
    print(f"      objet {d_objet:.0f} ms (1 000 copies et 1 000 suppressions)")


def plusieurs_parties(s3):
    s3.create_bucket(Bucket="videos")
    donnees = os.urandom(256 * 2**20)
    t = time.perf_counter()
    s3.put_object(Bucket="videos", Key="unique.bin", Body=donnees)
    d_unique = time.perf_counter() - t
    taille = 8 * 2**20
    parts = [donnees[i:i + taille] for i in range(0, len(donnees), taille)]

    def envoyer(up, numero, cle="parties.bin"):
        r = s3.upload_part(Bucket="videos", Key=cle, UploadId=up, PartNumber=numero, Body=parts[numero - 1])
        return {"PartNumber": numero, "ETag": r["ETag"]}

    t = time.perf_counter()
    up = s3.create_multipart_upload(Bucket="videos", Key="parties.bin")["UploadId"]
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        faites = list(pool.map(lambda n: envoyer(up, n), range(1, len(parts) + 1)))
    r = s3.complete_multipart_upload(Bucket="videos", Key="parties.bin", UploadId=up,
                                     MultipartUpload={"Parts": faites})
    d_parties = time.perf_counter() - t
    print(f"   256 Mo en une requête : {d_unique:.1f} s ; en {len(parts)} parties de 8 Mo par 4 fils :"
          f" {d_parties:.1f} s")
    print(f"   ETag de l'objet en parties : {r['ETag']}")
    print("      (empreinte des empreintes des parties, suivie du nombre de parties)")
    up = s3.create_multipart_upload(Bucket="videos", Key="reprise.bin")["UploadId"]
    for n in range(1, 11):
        envoyer(up, n, "reprise.bin")
    print("   panne après la partie 10 ; liste des parties déjà reçues (ListParts) :")
    recues = s3.list_parts(Bucket="videos", Key="reprise.bin", UploadId=up)["Parts"]
    print(f"      {len(recues)} parties déjà reçues, {sum(p['Size'] for p in recues) // 2**20} Mo")
    deja = {p["PartNumber"]: p["ETag"] for p in recues}
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        nouvelles = list(pool.map(lambda n: envoyer(up, n, "reprise.bin"),
                                  [n for n in range(1, len(parts) + 1) if n not in deja]))
    liste = sorted([{"PartNumber": n, "ETag": e} for n, e in deja.items()] + nouvelles, key=lambda p: p["PartNumber"])
    s3.complete_multipart_upload(Bucket="videos", Key="reprise.bin", UploadId=up, MultipartUpload={"Parts": liste})
    relu = s3.get_object(Bucket="videos", Key="reprise.bin")["Body"].read()
    print(f"      {len(nouvelles)} parties envoyées après la reprise ; objet relu identique :"
          f" {md5(relu) == md5(donnees)}")


def presignee(s3):
    s3.put_object(Bucket="photos", Key="facture.pdf", Body=b"%PDF facture")
    url = s3.generate_presigned_url("get_object", Params={"Bucket": "photos", "Key": "facture.pdf"}, ExpiresIn=3)

    def lire(u):
        try:
            with urllib.request.urlopen(u) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    print(f"   lecture par l'URL présignée (valable 3 s), sans clé : {lire(url)}")
    print(f"   même objet, sans signature : {lire(f'http://127.0.0.1:{PORT}/photos/facture.pdf')}")
    time.sleep(4)
    print(f"   la même URL, 4 s plus tard : {lire(url)}")
    url = s3.generate_presigned_url("put_object", ExpiresIn=60,
                                    Params={"Bucket": "photos", "Key": "depot/photo.jpg", "ContentType": "image/jpeg"})
    requete = urllib.request.Request(url, data=b"image envoyee par le navigateur", method="PUT",
                                     headers={"Content-Type": "image/jpeg"})
    with urllib.request.urlopen(requete) as r:
        code = r.status
    corps = s3.get_object(Bucket="photos", Key="depot/photo.jpg")["Body"].read()
    print(f"   envoi par une URL présignée en écriture : {code}")
    print(f"      objet relu : {corps[:30]!r}")
    print(f"   longueur de l'URL : {len(url)} caractères")


def effacement(s3):
    s3.create_bucket(Bucket="archives")
    donnees = os.urandom(300 * 2**20)
    s3.put_object(Bucket="archives", Key="sauvegarde.bin", Body=donnees)
    empreinte = md5(donnees)
    avant = sum(os.path.getsize(f) for f in glob.glob(str(DOSSIER / "donnees" / "archives_*.dat")))
    time.sleep(3)                                  # ec.encode ignore un volume modifié depuis moins d'1 s
    t = time.perf_counter()
    sortie = weed_shell(["ec.encode -collection=archives -force -fullPercent=0 -quietFor=1s"])
    d = time.perf_counter() - t
    if "Successfully completed" not in sortie:
        print(sortie[-1500:])
    fragments = glob.glob(str(DOSSIER / "donnees" / "archives_*.ec[0-9][0-9]"))
    apres = sum(os.path.getsize(f) for f in fragments)
    volumes = {Path(f).name.split(".")[0] for f in fragments}
    print(f"   objet de 300 Mo ; volumes avant codage : {avant / 2**20:.0f} Mo")
    print(f"   ec.encode en {d:.0f} s : {len(volumes)} volumes de 14 fragments, {apres / 2**20:.0f} Mo")
    print(f"      ({apres / avant:.2f} fois les données)")

    def relire():
        """Redémarre le serveur (qui recharge les fragments restants), puis lit l'objet ;
        jusqu'à 10 essais espacés de 3 s, car le serveur met un moment à être prêt."""
        docker("restart", NOM)
        c = client()
        erreur = None
        for _ in range(10):
            try:
                corps = c.get_object(Bucket="archives", Key="sauvegarde.bin")["Body"].read()
                return "identique" if md5(corps) == empreinte else "différent"
            except Exception as e:
                erreur = e
                time.sleep(3)
        return f"en échec après 10 essais ({type(erreur).__name__})"

    for perdus in ((0, 1, 2, 3), (4,)):
        for k in perdus:
            for f in glob.glob(str(DOSSIER / "donnees" / f"archives_*.ec{k:02d}")):
                os.remove(f)
        restants = 14 - (len(perdus) if perdus == (0, 1, 2, 3) else 5)
        print(f"   {14 - restants} fragments supprimés par volume, {restants} restants :")
        print(f"      lecture {relire()}")


def main():
    nettoyer()
    try:
        s3 = lancer()
        version = docker("exec", NOM, "weed", "version").stdout.split()[1]
        print(f"SeaweedFS {version}, API S3 ; système de fichiers local (ext4, SSD NVMe)")
        print("\n1. opérations de base")
        bases(s3)
        print("\n2. fichier contre objet")
        fichier_contre_objet(s3)
        print("\n3. téléversement en plusieurs parties")
        plusieurs_parties(s3)
        print("\n4. URL présignée")
        presignee(s3)
        print("\n5. codage d'effacement Reed-Solomon 10 + 4")
        effacement(s3)
    finally:
        nettoyer()


if __name__ == "__main__":
    main()
