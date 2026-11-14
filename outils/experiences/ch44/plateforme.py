"""Chapitre 44 : une mini-plateforme de fonctions à la demande, pour mesurer ce que
le modèle produit en charge réelle.

Chaque environnement d'exécution est un processus (travailleur.py) qui
s'initialise (import de numpy, et connexion à PostgreSQL si demandé), puis traite
une invocation à la fois, comme un environnement Lambda. La passerelle réutilise
un environnement libre s'il y en a un, en crée un sinon (démarrage à froid), dans
la limite de concurrence, et arrête les environnements restés libres plus
longtemps que le délai de maintien en vie.

  1. trafic de Poisson à différents débits, maintien en vie de 0,5 s et 2 s :
     part de démarrages à froid, latence, temps passé par les environnements à
     attendre sans travailler ;
  2. rafale de 100 invocations simultanées depuis zéro, puis une seconde rafale ;
     la même avec une limite de concurrence de 40 ;
  3. connexions à PostgreSQL 18 (max_connections = 40) : 100 environnements qui
     se connectent directement, puis à travers PgBouncer en mode transaction
     (10 connexions vers la base).

    python3 plateforme.py        (environ 12 minutes)
"""
import asyncio
import json
import math
import os
import random
import signal
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psycopg

ICI = Path(__file__).parent
PG, PGB = 12432, 12433
MDP = "cours44"
PASSERELLE = 12440
N_LANCEURS = 8
# Les environnements sont créés par des processus lanceurs à part : un fork dans
# le processus qui mesure retient le GIL quelques millisecondes et fausserait les
# latences. Chaque environnement se connecte ensuite à la passerelle par TCP.


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def centile(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))] if v else float("nan")


class Environnement:
    def __init__(self, lecteur, ecrivain):
        self.lecteur, self.ecrivain = lecteur, ecrivain
        self.libre_depuis = None
        self.ne = time.monotonic()

    async def invoquer(self, evt):
        self.ecrivain.write((json.dumps(evt) + "\n").encode())
        await self.ecrivain.drain()
        return json.loads(await self.lecteur.readline())

    def arreter(self):
        self.ecrivain.close()            # fin de flux : le travailleur s'arrête


def lanceur():
    """Processus lanceur : crée un travailleur par ligne « identifiant dsn » reçue."""
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)       # pas de zombies
    for ligne in sys.stdin:
        ident, dsn = ligne.split(" ", 1)
        env = dict(os.environ)
        if dsn.strip():
            env["DSN"] = dsn.strip()
        subprocess.Popen([sys.executable, str(ICI / "travailleur.py"), ident, str(PASSERELLE)],
                         env=env, stdin=subprocess.DEVNULL)


class Usine:
    """Passerelle TCP et lanceurs, partagés par toutes les plateformes d'un essai."""
    def __init__(self):
        self.attente, self.n = {}, 0

    async def demarrer(self):
        self.serveur = await asyncio.start_server(self.accueil, "127.0.0.1", PASSERELLE)
        self.lanceurs = [subprocess.Popen([sys.executable, __file__, "--lanceur"],
                                          stdin=subprocess.PIPE, text=True, bufsize=1)
                         for _ in range(N_LANCEURS)]

    async def accueil(self, lecteur, ecrivain):
        rep = json.loads(await lecteur.readline())
        fut = self.attente.pop(rep["id"])
        if "erreur" in rep:
            ecrivain.close()
            fut.set_result((None, rep["erreur"]))
        else:
            fut.set_result((Environnement(lecteur, ecrivain), None))

    async def creer(self, dsn):
        self.n += 1
        ident = str(self.n)
        fut = asyncio.get_running_loop().create_future()
        self.attente[ident] = fut
        self.lanceurs[self.n % N_LANCEURS].stdin.write(f"{ident} {dsn or ''}\n")
        self.lanceurs[self.n % N_LANCEURS].stdin.flush()
        return await fut

    async def arreter(self):
        self.serveur.close()
        for p in self.lanceurs:
            p.stdin.close()
            p.wait()


USINE = Usine()


class Plateforme:
    def __init__(self, maintien, limite=1000, dsn=None):
        self.maintien, self.limite, self.dsn = maintien, limite, dsn
        self.libres, self.total, self.max_total = [], 0, 0
        self.froids = self.refus = self.erreurs = 0
        self.vies = 0.0              # secondes-environnement écoulées (vivants)
        self.travail = 0.0           # secondes passées à traiter des invocations
        self.balai = None

    async def __aenter__(self):
        self.balai = asyncio.create_task(self.balayer())
        return self

    async def __aexit__(self, *_):
        self.balai.cancel()
        for e in self.libres:
            self.retirer(e)
        self.libres = []

    def retirer(self, e):
        self.vies += time.monotonic() - e.ne
        e.arreter()
        self.total -= 1

    async def balayer(self):
        while True:
            await asyncio.sleep(0.02)
            t = time.monotonic()
            garder = []
            for e in self.libres:
                if t - e.libre_depuis > self.maintien:
                    self.retirer(e)
                else:
                    garder.append(e)
            self.libres = garder

    async def creer(self):
        return await USINE.creer(self.dsn)

    async def invoquer(self, evt):
        t = time.monotonic()
        froid = False
        if self.libres:
            e = self.libres.pop()                  # le plus récemment libéré
        else:
            if self.total >= self.limite:
                self.refus += 1
                return "refus", time.monotonic() - t
            self.total += 1
            self.max_total = max(self.max_total, self.total)
            froid = True
            self.froids += 1
            e, err = await self.creer()
            if e is None:
                self.total -= 1
                self.erreurs += 1
                self.derniere_erreur = err
                return "erreur", time.monotonic() - t
        t1 = time.monotonic()
        await e.invoquer(evt)
        self.travail += time.monotonic() - t1
        e.libre_depuis = time.monotonic()
        self.libres.append(e)
        return ("froid" if froid else "chaud"), time.monotonic() - t


async def trafic():
    print("1. trafic de Poisson, 80 invocations de 50 ms par point")
    print(f"   {'maintien':>8}{'req/s':>7}{'froids':>8}{'e^-λK':>8}{'méd. ms':>9}"
          f"{'99e ms':>8}{'env. max':>9}{'inactif':>9}")
    alea = random.Random(44)
    for maintien in (0.5, 2.0):
        for debit in (0.5, 1, 2, 5, 20):
            async with Plateforme(maintien) as pf:
                taches, lat = [], []

                async def une():
                    r, d = await pf.invoquer({"duree_s": 0.05})
                    lat.append(d * 1000)

                for _ in range(80):
                    taches.append(asyncio.create_task(une()))
                    await asyncio.sleep(alea.expovariate(debit))
                await asyncio.gather(*taches)
                await asyncio.sleep(maintien + 0.1)       # laisser le balai finir
                vies, travail = pf.vies, pf.travail
            inactif = 1 - travail / vies if vies else float("nan")
            print(f"   {maintien:>7.1f}s{debit:>7.1f}{100 * pf.froids / 80:>7.0f}%"
                  f"{100 * math.exp(-debit * maintien):>7.0f}%{statistics.median(lat):>9.0f}"
                  f"{centile(lat, .99):>8.0f}{pf.max_total:>9}{100 * inactif:>8.0f}%")
    print("   e^-λK : part des intervalles entre deux invocations plus longs que le")
    print("   maintien K ; inactif : part de la vie des environnements passée sans")
    print("   travail")


async def rafales():
    print("\n2. rafales de 100 invocations simultanées (50 ms chacune)")
    print(f"   {'cas':<26}{'froids':>7}{'refus':>7}{'méd. ms':>9}{'99e ms':>8}")
    async with Plateforme(0) as pf:
        lat = []
        for _ in range(10):
            lat.append((await pf.invoquer({"duree_s": 0.05}))[1] * 1000)
            await asyncio.sleep(0.1)
        print(f"   {'une à la fois, à froid':<26}{pf.froids:>7}{0:>7}"
              f"{statistics.median(lat):>9.0f}{centile(lat, .99):>8.0f}")
    for limite in (1000, 40):
        async with Plateforme(60, limite=limite) as pf:
            for n in (1, 2):
                f0, r0 = pf.froids, pf.refus
                res = await asyncio.gather(*(pf.invoquer({"duree_s": 0.05}) for _ in range(100)))
                lat = [d * 1000 for r, d in res if r in ("froid", "chaud")]
                cas = f"limite {limite}, rafale {n}"
                print(f"   {cas:<26}{pf.froids - f0:>7}{pf.refus - r0:>7}"
                      f"{statistics.median(lat):>9.0f}{centile(lat, .99):>8.0f}")


def docker(*a):
    return subprocess.run(["docker", *a], capture_output=True, text=True)


def lancer_bases():
    for n in ("pg", "pgb"):
        docker("rm", "-f", "-v", f"cours-sd-ch44-{n}")
    docker("run", "-d", "--name", "cours-sd-ch44-pg", "--network", "host",
           "-e", f"POSTGRES_PASSWORD={MDP}", "postgres:18-alpine",
           "-c", f"port={PG}", "-c", "max_connections=40")
    docker("run", "-d", "--name", "cours-sd-ch44-pgb", "--network", "host",
           "-e", "DB_HOST=127.0.0.1", "-e", f"DB_PORT={PG}", "-e", "DB_USER=postgres",
           "-e", f"DB_PASSWORD={MDP}", "-e", "AUTH_TYPE=scram-sha-256",
           "-e", f"LISTEN_PORT={PGB}", "-e", "POOL_MODE=transaction",
           "-e", "DEFAULT_POOL_SIZE=10", "-e", "MAX_CLIENT_CONN=1000",
           "edoburu/pgbouncer:latest")
    for port in (PG, PGB):
        for _ in range(300):
            try:
                psycopg.connect(f"postgresql://postgres:{MDP}@127.0.0.1:{port}/postgres",
                                connect_timeout=2).close()
                break
            except psycopg.Error:
                time.sleep(0.2)


async def connexions():
    print("\n3. 100 invocations simultanées qui interrogent PostgreSQL")
    print("   (max_connections = 40 ; requête de 20 ms ; deux rafales)")
    print(f"   {'chemin':<10}{'rafale':>7}{'réussies':>9}{'erreurs':>8}{'méd. ms':>9}"
          f"{'99e ms':>8}{'conn. PG':>9}")
    admin = await psycopg.AsyncConnection.connect(
        f"postgresql://postgres:{MDP}@127.0.0.1:{PG}/postgres", autocommit=True)
    erreur = ""
    for chemin, port in (("directe", PG), ("PgBouncer", PGB)):
        dsn = f"postgresql://postgres:{MDP}@127.0.0.1:{port}/postgres"
        for _ in range(500):                     # connexions de l'essai précédent fermées
            c = await admin.execute("select count(*) from pg_stat_activity where backend_type ="
                                    " 'client backend' and pid <> pg_backend_pid()")
            if (await c.fetchone())[0] == 0:
                break
            await asyncio.sleep(0.02)
        async with Plateforme(60, dsn=dsn) as pf:
            for n in (1, 2):
                vu = 0
                fini = False

                async def sonder():
                    nonlocal vu
                    while not fini:
                        c = await admin.execute(
                            "select count(*) from pg_stat_activity where backend_type ="
                            " 'client backend' and pid <> pg_backend_pid()")
                        vu = max(vu, (await c.fetchone())[0])
                        await asyncio.sleep(0.01)

                s = asyncio.create_task(sonder())
                e0 = pf.erreurs
                res = await asyncio.gather(*(pf.invoquer({"requete_s": 0.02}) for _ in range(100)))
                fini = True
                await s
                lat = [d * 1000 for r, d in res if r in ("froid", "chaud")]
                print(f"   {chemin:<10}{n:>7}{len(lat):>9}{pf.erreurs - e0:>8}"
                      f"{statistics.median(lat):>9.0f}{centile(lat, .99):>8.0f}{vu:>9}")
                if pf.erreurs > e0:
                    erreur = pf.derniere_erreur
    await admin.close()
    if erreur:
        print(f"   erreur à la connexion : {erreur.split('FATAL:')[-1].strip()[:50]}")


async def principal():
    await USINE.demarrer()
    await trafic()
    await rafales()
    lancer_bases()
    try:
        await connexions()
    finally:
        for n in ("pg", "pgb"):
            docker("rm", "-f", "-v", f"cours-sd-ch44-{n}")
        await USINE.arreter()


if __name__ == "__main__":
    if "--lanceur" in sys.argv:
        lanceur()
    else:
        asyncio.run(principal())
