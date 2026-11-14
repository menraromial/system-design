"""Un environnement d'exécution de la mini-plateforme : un processus Python qui
s'initialise (import de numpy, connexion facultative à PostgreSQL), se connecte à
la passerelle pour signaler qu'il est prêt, puis traite une invocation à la fois.

    python3 travailleur.py <identifiant> <port de la passerelle>
"""
import json
import os
import socket
import sys
import time

t = time.perf_counter()
ident, port = sys.argv[1], int(sys.argv[2])
import numpy  # noqa: E402,F401  (initialisation réelle : environ 0,1 s)

conn, erreur = None, None
if os.environ.get("DSN"):
    import psycopg
    try:
        conn = psycopg.connect(os.environ["DSN"], autocommit=True, prepare_threshold=None,
                               connect_timeout=10)
    except Exception as e:                       # trop de clients, refus, délai
        erreur = " ".join(str(e).split())[:200]

s = socket.create_connection(("127.0.0.1", port))
f = s.makefile("rw", buffering=1)
if erreur:
    f.write(json.dumps({"id": ident, "erreur": erreur}) + "\n")
    sys.exit(1)
f.write(json.dumps({"id": ident, "pret_ms": (time.perf_counter() - t) * 1000}) + "\n")
for ligne in f:
    evt = json.loads(ligne)
    if conn is not None:
        conn.execute("select pg_sleep(%s)", (evt.get("requete_s", 0.02),))
    else:
        time.sleep(evt.get("duree_s", 0.05))
    f.write('{"ok": true}\n')
