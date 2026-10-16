"""Chapitre 24 : un primaire PostgreSQL et ses répliques, dans des conteneurs.

Tous les conteneurs sont en réseau hôte et écoutent sur 127.0.0.1 :
  - cours-sd-ch24-primaire, port 10432 ;
  - cours-sd-ch24-r1 et cours-sd-ch24-r2, ports 10433 et 10434.
Chaque réplique reçoit le journal (WAL) du primaire à travers un relais écrit en
Python (ports 10441 et 10442), qui retarde les octets d'un délai réglable dans
chaque sens et peut couper la liaison, pour imiter une liaison lointaine ou une
panne de réseau. Les conteneurs sont supprimés par leur nom, avec leurs volumes.
"""
import asyncio
import statistics
import subprocess
import threading
import time

import psycopg

IMAGE = "postgres:18-alpine"
PGDATA = "/var/lib/postgresql/18/docker"
PRIMAIRE = 10432
REPLIQUES = {"r1": (10433, 10441), "r2": (10434, 10442)}   # nom : (port, port du relais)


def dsn(port, **options):
    texte = f"host=127.0.0.1 port={port} dbname=postgres user=postgres"
    return " ".join([texte, *(f"{k}={v}" for k, v in options.items())])


def nombre(x):
    return f"{x:,.0f}".replace(",", " ")


def centiles(durees):
    durees = sorted(durees)
    return statistics.median(durees), durees[int(0.99 * (len(durees) - 1))]


# ---- relais à délai ------------------------------------------------------------
class Relais:
    """Relais TCP qui retarde chaque octet de `delai` secondes dans chaque sens.
    Coupé, il retient les octets sans fermer les connexions, comme un réseau qui
    ne répond plus. Il tourne dans un fil à lui, avec sa propre boucle asyncio."""

    def __init__(self, port_ecoute, port_cible, delai=0.0):
        self.port_ecoute, self.port_cible, self.delai = port_ecoute, port_cible, delai
        self.generation, self.connexions = 0, []
        self.boucle = asyncio.new_event_loop()
        pret = threading.Event()
        threading.Thread(target=self._tourner, args=(pret,), daemon=True).start()
        pret.wait()

    def _tourner(self, pret):
        asyncio.set_event_loop(self.boucle)
        self.ouvert = asyncio.Event()
        self.ouvert.set()
        self.boucle.run_until_complete(asyncio.start_server(self._accueillir, "127.0.0.1", self.port_ecoute))
        pret.set()
        self.boucle.run_forever()

    async def _accueillir(self, lecteur, ecrivain):
        try:
            l2, e2 = await asyncio.open_connection("127.0.0.1", self.port_cible)
        except OSError:
            ecrivain.close()
            return
        gen = self.generation
        self.connexions.append((gen, ecrivain, e2))
        await asyncio.gather(self._transmettre(lecteur, e2, gen), self._transmettre(l2, ecrivain, gen),
                             return_exceptions=True)
        for e in (ecrivain, e2):
            e.close()

    async def _transmettre(self, lecteur, ecrivain, gen):
        file = asyncio.Queue()

        async def livrer():
            while True:
                echeance, donnees = await file.get()
                if donnees is None:
                    return
                attente = echeance - time.monotonic()
                if attente > 0:
                    await asyncio.sleep(attente)
                await self.ouvert.wait()
                if gen != self.generation:      # liaison rompue : les octets en route sont perdus
                    return
                ecrivain.write(donnees)
                await ecrivain.drain()

        livreur = asyncio.create_task(livrer())
        try:
            while True:
                donnees = await lecteur.read(65536)
                if not donnees:
                    break
                file.put_nowait((time.monotonic() + self.delai, donnees))
        finally:
            file.put_nowait((0, None))
            await livreur
            ecrivain.close()

    def couper(self):
        self.boucle.call_soon_threadsafe(self.ouvert.clear)

    def retablir(self):
        self.boucle.call_soon_threadsafe(self.ouvert.set)

    def rompre(self):
        """Ferme les connexions en cours en jetant les octets retenus ou en route ;
        les nouvelles connexions passent normalement."""
        def faire():
            self.generation += 1
            for gen, a, b in self.connexions:
                if gen < self.generation:
                    a.close()
                    b.close()
            self.connexions = [c for c in self.connexions if c[0] == self.generation]
            self.ouvert.set()
        self.boucle.call_soon_threadsafe(faire)
        time.sleep(0.2)


# ---- conteneurs -----------------------------------------------------------------
def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def reglages(port):
    return ["-c", f"port={port}", "-c", "listen_addresses=127.0.0.1", "-c", "shared_buffers=256MB",
            "-c", "max_wal_size=2GB", "-c", "wal_keep_size=1GB"]


def lancer_primaire():
    docker("run", "-d", "--name", "cours-sd-ch24-primaire", "--network", "host",
           "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGE, *reglages(PRIMAIRE))
    return connecter(PRIMAIRE)


def lancer_replique(nom, *options):
    """Copie le primaire par pg_basebackup à travers le relais, puis démarre en réplique."""
    port, relais = REPLIQUES[nom]
    copie = (f'pg_basebackup -d "host=127.0.0.1 port={relais} user=postgres application_name={nom}"'
             f" -D /tmp/donnees -R -X stream -c fast && chmod 700 /tmp/donnees && exec postgres -D /tmp/donnees "
             + " ".join([*reglages(port), *options]))
    docker("run", "-d", "--name", f"cours-sd-ch24-{nom}", "--network", "host", "--user", "postgres",
           "--entrypoint", "sh", IMAGE, "-c", copie)
    return connecter(port)


def connecter(port, delai=120, **options):
    fin = time.monotonic() + delai
    while True:
        try:
            return psycopg.connect(dsn(port, **options), autocommit=True)
        except psycopg.OperationalError:
            if time.monotonic() > fin:
                raise
            time.sleep(0.5)


def attendre_flux(primaire, *noms, delai=60):
    """Attend que chaque réplique nommée reçoive le journal en continu."""
    fin = time.monotonic() + delai
    while time.monotonic() < fin:
        etats = dict(primaire.execute("SELECT application_name, state FROM pg_stat_replication").fetchall())
        if all(etats.get(n) == "streaming" for n in noms):
            return
        time.sleep(0.2)
    raise RuntimeError(f"réplication pas établie : {noms}")


def regler(primaire, nom, valeur):
    primaire.execute(f"ALTER SYSTEM SET {nom} = '{valeur}'")
    primaire.execute("SELECT pg_reload_conf()")
    time.sleep(0.3)


def supprimer(*noms):
    noms = noms or ("primaire", *REPLIQUES)
    subprocess.run(["docker", "rm", "-f", "-v", *(f"cours-sd-ch24-{n}" for n in noms)],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
