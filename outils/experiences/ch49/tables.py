"""Chapitre 49 : ce qu'un format de table (Delta Lake, par delta-rs) ajoute à un
dossier de fichiers Parquet. Données dans ~/.cache/cours-sd-ch49/tables.

  1. un écrivain ajoute 5 lots de 2 millions de lignes, chacun en 20 fichiers,
     pendant qu'un lecteur compte les lignes en boucle : dossier de fichiers
     Parquet nus, puis table Delta ; états intermédiaires vus par le lecteur ;
  2. historique et voyage dans le temps sur la table Delta ;
  3. petits fichiers : 300 ajouts de 1 000 lignes, puis compactage et nettoyage ;
  4. deux écrivains concurrents : 2 × 30 ajouts, puis une suppression et une
     mise à jour simultanées des mêmes lignes (contrôle de concurrence optimiste) ;
  5. supprimer une ligne dans une table de 2 millions de lignes en 20 fichiers :
     fichiers réécrits (copie à l'écriture) ;
  6. évolution du schéma : ajout d'une colonne.

    python3 tables.py            (environ 2 minutes)
"""
import multiprocessing as mp
import shutil
import statistics
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

RACINE = Path.home() / ".cache" / "cours-sd-ch49" / "tables"
CTX = mp.get_context("spawn")             # delta-rs (tokio) refuse les processus issus de fork
LOT, FICHIERS = 2_000_000, 20


def nb(x, d=0):
    return f"{x:,.{d}f}".replace(",", " ")


def lot(k, n=LOT):
    ids = pa.array(range(k * n, (k + 1) * n), pa.int64())
    return pa.table({"id": ids, "magasin": pc.remainder(ids, 500),
                     "montant": pc.multiply(pc.remainder(ids, 997), 1.5),
                     "commentaire": pa.array([f"vente {i}" for i in range(k * n, (k + 1) * n)])})


# ---- 1. lecteur concurrent ----------------------------------------------------------------
def ecrivain_lac(dossier):
    """Comme un moteur de calcul : chaque fichier est écrit sous un nom temporaire
    puis renommé (atomique fichier par fichier, pas lot par lot)."""
    for k in range(5):
        t = lot(k)
        pas = LOT // FICHIERS
        for f in range(FICHIERS):
            tmp = dossier / f".part-{k:02d}-{f:02d}.tmp"
            pq.write_table(t.slice(f * pas, pas), tmp)
            tmp.rename(dossier / f"part-{k:02d}-{f:02d}.parquet")


def ecrivain_delta(chemin):
    for k in range(5):
        write_deltalake(str(chemin), lot(k), mode="append", target_file_size=6_000_000)


def lire(mode, chemin):
    chemin = Path(chemin)
    if mode == "lac":
        if not any(chemin.glob("*.parquet")):
            return 0
        return duckdb.execute(f"select count(*) from read_parquet('{chemin}/*.parquet')").fetchone()[0]
    return DeltaTable(str(chemin)).count() if (chemin / "_delta_log").exists() else 0


def lecteur(mode, chemin, fin, vus, erreurs):
    while not fin.is_set():
        try:
            vus.append(lire(mode, chemin))
        except Exception:
            erreurs.value += 1


def concurrence():
    print(f"1. un écrivain ajoute 5 lots de {nb(LOT)} lignes (plusieurs fichiers par")
    print("   lot), un lecteur compte les lignes en boucle")
    print(f"   {'stockage':<22}{'lectures':>9}{'erreurs':>9}{'comptes vus':>13}{'intermédiaires':>16}")
    lac, delta = RACINE / "lac", RACINE / "delta"
    lac.mkdir(parents=True)

    for nom, ecrire, cible, mode in (("Parquet nu", ecrivain_lac, lac, "lac"),
                                     ("table Delta", ecrivain_delta, delta, "delta")):
        mgr = CTX.Manager()
        vus, fin, erreurs = mgr.list(), mgr.Event(), mgr.Value("i", 0)
        lec = CTX.Process(target=lecteur, args=(mode, cible, fin, vus, erreurs))
        lec.start()
        p = CTX.Process(target=ecrire, args=(cible,))
        p.start()
        p.join()
        fin.set()
        lec.join()
        vus = list(vus)
        distincts = sorted(set(vus))
        inter = [v for v in distincts if v % LOT != 0]
        print(f"   {nom:<22}{len(vus):>9}{erreurs.value:>9}{len(distincts):>13}{len(inter):>16}")
        mgr.shutdown()
    dt = DeltaTable(str(delta))
    print(f"   Parquet nu : {len(list(lac.glob('*.parquet')))} fichiers ; table Delta :"
          f" {len(dt.file_uris())} fichiers, version {dt.version()}")
    return delta


# ---- 2. historique ---------------------------------------------------------------------------
def historique(delta):
    print("\n2. historique et voyage dans le temps (table Delta)")
    dt = DeltaTable(str(delta))
    for h in sorted(dt.history(), key=lambda h: h["version"]):
        m = h.get("operationMetrics", {})
        print(f"   version {h['version']} : {h['operation']:<6} {m.get('num_added_files', '?'):>3} fichiers,"
              f" {nb(m.get('num_added_rows', 0)):>10} lignes")
    v0 = DeltaTable(str(delta), version=0)
    print(f"   lignes à la version 0 : {nb(v0.count())} ; à la dernière : {nb(dt.count())}")
    print(f"   journal : {len(list((delta / '_delta_log').glob('*.json')))} fichiers JSON")


# ---- 3. petits fichiers ---------------------------------------------------------------------
def petits_fichiers():
    print("\n3. petits fichiers : 300 ajouts de 1 000 lignes")
    chemin = RACINE / "petits"
    for k in range(300):
        write_deltalake(str(chemin), lot(k, 1000), mode="append")

    def requete():                                   # ouvrir la table et sommer une colonne
        dt = DeltaTable(str(chemin))
        return pc.sum(dt.to_pyarrow_table(columns=["montant"])["montant"]).as_py()

    def mesure():
        d = []
        for _ in range(5):
            t = time.perf_counter()
            requete()
            d.append((time.perf_counter() - t) * 1000)
        return statistics.median(d)

    dt = DeltaTable(str(chemin))
    print(f"   {'état':<26}{'fichiers':>9}{'version':>9}{'somme ms':>10}")
    print(f"   {'avant compactage':<26}{len(dt.file_uris()):>9}{dt.version():>9}{mesure():>10.1f}")
    t = time.perf_counter()
    r = dt.optimize.compact()
    dc = time.perf_counter() - t
    dt = DeltaTable(str(chemin))
    print(f"   {'après compactage':<26}{len(dt.file_uris()):>9}{dt.version():>9}{mesure():>10.1f}")
    supprimes = dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    restants = len(list(chemin.glob("*.parquet")))
    print(f"   compactage en {dc:.2f} s : {r['numFilesRemoved']} fichiers retirés, {r['numFilesAdded']} ajouté ;")
    print(f"   nettoyage : {len(supprimes)} fichiers supprimés du disque, {restants} restant")


# ---- 4. écrivains concurrents ---------------------------------------------------------------
def ajouts(chemin, base, compte):
    for k in range(30):
        while True:
            try:
                write_deltalake(str(chemin), lot(base + k, 1000), mode="append")
                break
            except CommitFailedError:
                compte.value += 1


def modifier(chemin, quoi, res):
    try:
        dt = DeltaTable(str(chemin))
        if quoi == "supprimer":
            dt.delete("id < 5000")
        else:
            dt.update(updates={"montant": "0.0"}, predicate="id < 5000")
        res[quoi] = "validée"
    except Exception as e:
        res[quoi] = type(e).__name__


def concurrents():
    print("\n4. deux écrivains concurrents")
    chemin = RACINE / "concurrents"
    write_deltalake(str(chemin), lot(0, 1000), mode="append")
    mgr = CTX.Manager()
    compte = mgr.Value("i", 0)
    ps = [CTX.Process(target=ajouts, args=(chemin, 1 + 100 * j, compte)) for j in range(2)]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    dt = DeltaTable(str(chemin))
    print(f"   2 × 30 ajouts simultanés : version {dt.version()}, {nb(dt.count())} lignes ;")
    print(f"   validations refusées puis refaites par le script : {compte.value}")
    res = mgr.dict()
    ps = [CTX.Process(target=modifier, args=(chemin, q, res)) for q in ("supprimer", "mettre à jour")]
    for p in ps:
        p.start()
    for p in ps:
        p.join()
    print(f"   suppression et mise à jour simultanées des lignes id < 5000 :")
    for q in ("supprimer", "mettre à jour"):
        print(f"     {q:<14} {res[q]}")
    mgr.shutdown()


# ---- 5. supprimer une ligne -------------------------------------------------------------------
def une_ligne():
    print(f"\n5. supprimer une ligne dans une table de {nb(LOT)} lignes")
    chemin = RACINE / "une"
    write_deltalake(str(chemin), lot(0), mode="append", target_file_size=6_000_000)
    dt = DeltaTable(str(chemin))
    n_fichiers = len(dt.file_uris())
    t = time.perf_counter()
    m = dt.delete("id = 123456")
    d = time.perf_counter() - t
    print(f"   {n_fichiers} fichiers ; suppression en {1000 * d:.0f} ms : {m['num_removed_files']} fichier retiré,"
          f" {m['num_added_files']} ajouté,")
    print(f"   {nb(m['num_copied_rows'])} lignes recopiées pour 1 supprimée")


# ---- 6. schéma ----------------------------------------------------------------------------------
def schema():
    print("\n6. ajout d'une colonne (schema_mode = merge)")
    chemin = RACINE / "schema"
    write_deltalake(str(chemin), lot(0, 1000), mode="append")
    t = lot(1, 1000).append_column("canal", pa.array(["web"] * 1000))
    write_deltalake(str(chemin), t, mode="append", schema_mode="merge")
    dt = DeltaTable(str(chemin))
    tab = dt.to_pyarrow_table()
    print(f"   colonnes : {', '.join(dt.schema().to_arrow().names)}")
    print(f"   lignes sans canal (écrites avant) : {nb(tab['canal'].null_count)}")
    print(f"   colonnes de la version 0 : "
          f"{', '.join(DeltaTable(str(chemin), version=0).schema().to_arrow().names)}")


def main():
    shutil.rmtree(RACINE, ignore_errors=True)
    RACINE.mkdir(parents=True)
    try:
        delta = concurrence()
        historique(delta)
        petits_fichiers()
        concurrents()
        une_ligne()
        schema()
    finally:
        shutil.rmtree(RACINE, ignore_errors=True)


if __name__ == "__main__":
    main()
