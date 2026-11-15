"""Chapitre 47 : le même travail avec Apache Spark (PySpark, mode local à 8 cœurs).

  1. trois écritures de l'agrégation par page : RDD avec groupByKey (pas de
     combinaison avant l'échange), RDD avec reduceByKey (combinaison locale),
     DataFrame (optimiseur Catalyst) ; durée et octets écrits par l'échange ;
  2. évaluation paresseuse : définir des transformations ne lance rien, l'action
     lance le calcul ; mise en cache d'un résultat réutilisé ;
  3. jointure avec une table de 100 000 pages : diffusion (broadcast), puis
     jointure par tri-fusion avec et sans traitement adaptatif du déséquilibre
     (AQE skew join), seuils abaissés pour la taille du jeu de données
     (partition déséquilibrée au-delà de 16 Mo et de 2 fois la médiane).

Les durées et les octets de l'échange viennent de l'API REST de l'interface de
Spark (port 4040). À lancer sous un plafond de mémoire (MemoryMax=6G).

    python3 donnees.py ; python3 spark_lots.py       (environ 5 minutes)
"""
import json
import statistics
import time
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

RACINE = Path.home() / ".cache" / "cours-sd-ch47"
JOURNAUX = str(RACINE / "journaux")
SCHEMA = StructType([StructField("ts", LongType()), StructField("page", StringType()),
                     StructField("user", StringType()), StructField("statut", IntegerType()),
                     StructField("octets", LongType())])


def session():
    return (SparkSession.builder.master("local[8]").appName("cours-sd-ch47")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.ui.port", "4040")
            .config("spark.local.dir", str(RACINE / "spark-tmp"))
            .config("spark.sql.shuffle.partitions", "16")
            .getOrCreate())


class Mesure:
    """Octets d'échange et tâches des étapes lancées depuis le dernier appel."""
    def __init__(self, spark):
        self.app = spark.sparkContext.applicationId
        self.vus = set(s["stageId"] for s in self.etapes())

    def api(self, chemin):
        with urllib.request.urlopen(f"http://127.0.0.1:4040/api/v1/applications/{self.app}/{chemin}") as r:
            return json.load(r)

    def etapes(self):
        return [s for s in self.api("stages?status=complete")]

    def nouvelles(self):
        time.sleep(3)                     # l'interface publie les étapes avec un léger retard
        tout = self.etapes()
        neuves = [s for s in tout if s["stageId"] not in self.vus]
        self.vus |= set(s["stageId"] for s in tout)
        return neuves

    def echange(self):
        return sum(s["shuffleWriteBytes"] for s in self.nouvelles())

    def taches_echange(self):
        """Étape qui lit le plus de données d'échange (la jointure) :
        (nombre de tâches, tâche la plus longue, médiane) en s, ou None."""
        lues = [s for s in self.nouvelles() if s["shuffleReadBytes"] > 0]
        if not lues:
            return None
        s = max(lues, key=lambda s: s["shuffleReadBytes"])
        q = self.api(f"stages/{s['stageId']}/{s['attemptId']}/taskSummary?quantiles=0.5,1.0")
        med, mx = q["executorRunTime"]
        return s["numTasks"], mx / 1000, med / 1000, s["shuffleReadBytes"]


def chrono(f):
    t = time.perf_counter()
    r = f()
    return r, time.perf_counter() - t


def agregations(spark, m):
    print("1. agrégation par page (accès et octets), 16 millions de lignes")
    print("   (durée à chaud : seconde de deux exécutions)")
    print(f"   {'écriture':<26}{'durée s':>9}{'échange':>12}")
    sc = spark.sparkContext

    def analyse(ligne):
        _, page, _, _, octets = ligne.split()
        return page, int(octets)

    lignes = sc.textFile(JOURNAUX).map(analyse)
    cas = [("RDD, groupByKey", lambda: lignes.groupByKey().mapValues(lambda v: (len(v), sum(v))).count()),
           ("RDD, reduceByKey", lambda: lignes.mapValues(lambda o: (1, o))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).count()),
           ("DataFrame, groupBy", lambda: spark.read.csv(JOURNAUX, sep=" ", schema=SCHEMA)
            .groupBy("page").agg(F.count("*").alias("acces"), F.sum("octets").alias("octets")).count())]
    m.nouvelles()
    for nom, f in cas:
        f()
        m.nouvelles()
        n, d = chrono(f)
        print(f"   {nom:<26}{d:>9.1f}{m.echange() / 1e6:>9.1f} Mo")
    print(f"   pages distinctes : {n}")


def paresse(spark, m):
    print("\n2. évaluation paresseuse")
    sc = spark.sparkContext
    avant = len(sc.statusTracker().getJobIdsForGroup())
    df, d_def = chrono(lambda: spark.read.csv(JOURNAUX, sep=" ", schema=SCHEMA)
                       .filter(F.col("statut") == 404).groupBy("page").count())
    apres_def = len(sc.statusTracker().getJobIdsForGroup())
    _, d_act = chrono(lambda: df.orderBy(F.desc("count")).limit(5).collect())
    apres_act = len(sc.statusTracker().getJobIdsForGroup())
    print(f"   définir lecture, filtre et regroupement : {1000 * d_def:.0f} ms,"
          f" travaux lancés : {apres_def - avant}")
    print(f"   action (5 pages les plus en erreur) : {d_act:.1f} s, travaux lancés : {apres_act - apres_def}")
    plan = df._jdf.queryExecution().executedPlan().toString()
    print(f"   filtre poussé dans la lecture (PushedFilters) : {'PushedFilters: [IsNotNull(statut), EqualTo(statut,404)]' in plan}")
    base = spark.read.csv(JOURNAUX, sep=" ", schema=SCHEMA).filter(F.col("statut") == 404)
    _, d1 = chrono(lambda: base.groupBy("page").count().count())
    _, d2 = chrono(lambda: base.groupBy("user").count().count())
    base.cache()
    _, d3 = chrono(lambda: base.groupBy("page").count().count())
    _, d4 = chrono(lambda: base.groupBy("user").count().count())
    base.unpersist()
    print(f"   deux agrégations sur les lignes 404, sans cache : {d1:.1f} s puis {d2:.1f} s")
    print(f"   avec cache : {d3:.1f} s (lecture et mise en cache) puis {d4:.1f} s")


def jointures(spark, m):
    print("\n3. jointure des 16 millions d'accès avec 100 000 pages (catégorie)")
    print(f"   {'stratégie':<26}{'durée s':>8}{'échange':>9}{'tâches':>7}{'max':>8}{'méd.':>7}")
    pages = spark.range(100_000).select(F.concat(F.lit("/page/"), F.col("id")).alias("page"),
                                        (F.col("id") % 20).alias("categorie"))
    pages.cache().count()
    faits = spark.read.csv(JOURNAUX, sep=" ", schema=SCHEMA)
    conf = spark.conf
    cas = [("diffusion (broadcast)", "10485760", "true"),
           ("tri-fusion, skew join off", "-1", "false"),
           ("tri-fusion, skew join on", "-1", "true")]
    for nom, seuil, skew in cas:
        conf.set("spark.sql.autoBroadcastJoinThreshold", seuil)
        conf.set("spark.sql.adaptive.skewJoin.enabled", skew)
        conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
        conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8MB")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
        m.nouvelles()
        _, d = chrono(lambda: faits.join(pages, "page").groupBy("categorie")
                      .agg(F.sum("octets")).collect())
        n, mx, med, lus = m.taches_echange()
        print(f"   {nom:<26}{d:>8.1f}{lus / 1e6:>6.0f} Mo{n:>7}{mx:>6.2f} s{med:>5.2f} s")
    print("   colonnes 3 à 6 : l'étape qui lit le plus de données d'échange")
    print("   (la jointure, ou l'agrégation finale quand la table des pages est")
    print("   diffusée)")


def main():
    spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    m = Mesure(spark)
    try:
        agregations(spark, m)
        paresse(spark, m)
        jointures(spark, m)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
