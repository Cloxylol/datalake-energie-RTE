#!/usr/bin/env python3
"""
Silver : Bronze eco2mix (CSV batch + JSON streaming) -> grid_load + grid_generation.

Trois regles font le travail :

1. NORMALISATION TEMPORELLE. eco2mix publie en heure locale francaise avec
   offset ISO. On convertit en UTC et on garde ts_local pour les features
   calendaires du ML.

2. DEDUPLICATION AVEC PRIORITE DE QUALITE. Le temps reel est publie tout de
   suite puis remplace par du consolide. Cle (ts_utc, zone_id), le consolide
   gagne toujours. C'est ce qui justifie d'avoir garde les deux flux.

3. DEPIVOTEMENT DES FILIERES. eco2mix livre une colonne par filiere. On passe
   en lignes : le schema absorbe une nouvelle filiere sans changer.

TOLERANCE AUX NOMS DE CHAMPS : les colonnes reellement presentes sont
detectees a l'execution par intersection avec une liste de candidats. Un
champ absent est ignore avec un avertissement, il ne fait pas tomber le job.

    spark-submit silver_grid.py --start 2024-03-01 --end 2024-03-31
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s [silver-grid] %(message)s")
log = logging.getLogger(__name__)

# Candidats : on prend ce qui existe reellement dans les donnees.
TIME_CANDIDATES = ["date_heure", "date_et_heure", "datetime", "date"]
LOAD_CANDIDATES = {
    "consumption_mw": ["consommation", "consommation_mw", "conso"],
    "forecast_j1_mw": ["prevision_j1", "prevision_j_1", "previsionj1"],
    "forecast_j_mw": ["prevision_j", "previsionj"],
    "co2_rate_g_kwh": ["taux_co2", "taux_de_co2", "co2"],
}
# Filieres de production. Renouvelable = True/False.
FILIERES = {
    "nucleaire": False, "thermique": False, "charbon": False, "fioul": False,
    "gaz": False, "eolien": True, "eolien_terrestre": True,
    "eolien_offshore": True, "solaire": True, "hydraulique": True,
    "bioenergies": True, "pompage": False, "stockage_batterie": False,
    "destockage_batterie": False,
}


def pick(cols: list[str], candidates: list[str]) -> str | None:
    """Premier candidat present dans les colonnes, insensible a la casse."""
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in lower:
            return lower[cand]
    return None


# ---------------------------------------------------------------------------
# Lecture Bronze
# ---------------------------------------------------------------------------

def read_bronze_batch(spark: SparkSession, layout: Layout, fs: str,
                      start: str, end: str) -> DataFrame | None:
    """CSV consolide. inferSchema volontairement actif ici : les colonnes
    varient selon les annees, et on retype explicitement juste apres."""
    path = f"{fs}{layout.root}/bronze/eco2mix_cons"
    try:
        df = (spark.read
              .option("header", True)
              .option("sep", ";")
              .option("inferSchema", True)
              .option("mode", "PERMISSIVE")
              .csv(f"{path}/year=*/month=*/*.csv"))
    except Exception as exc:  # noqa: BLE001
        log.warning("Aucun lot batch lisible : %s", exc)
        return None

    if not df.columns:
        return None
    log.info("Bronze batch : %d colonnes detectees.", len(df.columns))
    return df.withColumn("_quality", F.lit("consolidated")) \
             .withColumn("_source", F.lit("eco2mix_cons"))


def read_bronze_stream(spark: SparkSession, layout: Layout, fs: str) -> DataFrame | None:
    """JSON produit par Structured Streaming. Le payload est encapsule dans
    raw_value : on le parse ici, pas en Bronze."""
    path = f"{fs}{layout.bronze_stream('eco2mix_tr')}"
    try:
        raw = spark.read.json(f"{path}/ingest_date=*/ingest_hour=*/*")
    except Exception as exc:  # noqa: BLE001
        log.warning("Aucun lot streaming lisible : %s", exc)
        return None

    if "raw_value" not in raw.columns:
        log.warning("Colonne raw_value absente du Bronze streaming.")
        return None

    # Le schema du payload est decouvert sur un echantillon.
    sample = raw.select("raw_value").limit(1000)
    inferred = spark.read.json(sample.rdd.map(lambda r: r.raw_value)).schema

    parsed = (raw
              .withColumn("j", F.from_json("raw_value", inferred))
              .select("j.payload.*", F.col("bronze_ingested_at"))
              .withColumn("_quality", F.lit("realtime"))
              .withColumn("_source", F.lit("eco2mix_tr")))
    log.info("Bronze streaming : %d colonnes.", len(parsed.columns))
    return parsed


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def normalize_time(df: DataFrame) -> DataFrame:
    """ts_utc + ts_local.

    PIEGE : si la colonne source est deja de type timestamp (inferSchema a
    fait son travail, ou le JSON etait type), Spark l'a DEJA ramenee en UTC
    en interpretant l'offset ISO. Reappliquer to_utc_timestamp decalerait
    tout d'une heure supplementaire. On teste donc le TYPE de la colonne,
    pas seulement son contenu.

    Trois cas :
      1. deja timestamp        -> rien a faire, c'est de l'UTC
      2. string avec offset    -> to_timestamp suffit, l'offset est lu
      3. string sans offset    -> interpreter en Europe/Paris

    Le cas 3 est celui du changement d'heure : 02:00 locale existe deux fois
    le dernier dimanche d'octobre. Sans offset l'information est perdue et
    Spark choisit une des deux, ce qui ecrase un quart d'heure. On le
    signale plutot que de le masquer.
    """
    tcol = pick(df.columns, TIME_CANDIDATES)
    if tcol is None:
        raise ValueError(
            f"Aucune colonne temporelle trouvee parmi {TIME_CANDIDATES}. "
            f"Colonnes disponibles : {df.columns[:20]}"
        )

    dtype = dict(df.dtypes)[tcol]
    log.info("Colonne temporelle : %s (type %s)", tcol, dtype)

    if dtype.startswith("timestamp"):
        # Cas 1 : Spark a deja lu l'offset. Ne surtout pas reconvertir.
        ts_utc = F.col(tcol)
    else:
        as_str = F.col(tcol).cast("string")
        has_offset = as_str.rlike(r"([+-]\d{2}:?\d{2}|Z)$")
        ts_utc = F.when(has_offset, F.to_timestamp(as_str)) \
                  .otherwise(F.to_utc_timestamp(F.to_timestamp(as_str),
                                                "Europe/Paris"))
        log.info("Colonne texte : conversion de fuseau appliquee si besoin.")

    return (df
            .withColumn("ts_utc", ts_utc)
            .filter(F.col("ts_utc").isNotNull())
            .withColumn("ts_local", F.from_utc_timestamp("ts_utc", "Europe/Paris")))


def build_grid_load(df: DataFrame) -> DataFrame:
    """Table de consommation, colonnes communes + mesures."""
    out = df
    for target, cands in LOAD_CANDIDATES.items():
        src = pick(df.columns, cands)
        if src:
            out = out.withColumn(target, F.col(src).cast(T.DoubleType()))
        else:
            log.warning("Champ %s introuvable (candidats %s), mis a null.",
                        target, cands)
            out = out.withColumn(target, F.lit(None).cast(T.DoubleType()))

    return (out
            .withColumn("zone_id", F.lit("fr"))
            .withColumn("source", F.col("_source"))
            .withColumn("quality", F.col("_quality"))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
            .select("ts_utc", "ts_local", "zone_id", "source", "quality",
                    "ingested_at", "source_file",
                    *LOAD_CANDIDATES.keys())
            # Garde-fou metier : la France ne consomme ni 0 ni 200 GW.
            .filter((F.col("consumption_mw").isNull()) |
                    ((F.col("consumption_mw") > 10000) &
                     (F.col("consumption_mw") < 120000))))


def build_grid_generation(df: DataFrame) -> DataFrame:
    """Depivotement : une colonne par filiere -> une ligne par filiere."""
    present = [f for f in FILIERES if pick(df.columns, [f])]
    if not present:
        raise ValueError(
            f"Aucune filiere trouvee. Colonnes : {df.columns[:30]}"
        )
    log.info("%d filiere(s) detectee(s) : %s", len(present), ", ".join(present))

    # stack(n, 'a', a, 'b', b, ...) : le depivotement natif Spark.
    pairs = ", ".join(
        f"'{f}', CAST(`{pick(df.columns, [f])}` AS DOUBLE)" for f in present
    )
    expr = f"stack({len(present)}, {pairs}) as (filiere, generation_mw)"

    renew = F.create_map(*[x for f in present
                           for x in (F.lit(f), F.lit(FILIERES[f]))])

    return (df
            .select("ts_utc", "ts_local", "_source", "_quality",
                    F.expr(expr))
            .filter(F.col("generation_mw").isNotNull())
            .withColumn("zone_id", F.lit("fr"))
            .withColumn("source", F.col("_source"))
            .withColumn("quality", F.col("_quality"))
            .withColumn("is_renewable", renew[F.col("filiere")])
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.lit(""))
            .select("ts_utc", "ts_local", "zone_id", "filiere", "generation_mw",
                    "is_renewable", "source", "quality", "ingested_at",
                    "source_file"))


def dedupe(df: DataFrame, keys: list[str]) -> DataFrame:
    """Dedup avec priorite : consolide > temps reel, puis ingestion la plus
    recente. C'est la regle qui fait que le temps reel se fait ecraser des
    que sa version consolidee arrive."""
    from pyspark.sql import Window

    w = (Window.partitionBy(*keys)
         .orderBy(F.when(F.col("quality") == "consolidated", 0).otherwise(1),
                  F.col("ingested_at").desc()))
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


def add_partitions(df: DataFrame) -> DataFrame:
    return (df.withColumn("year", F.year("ts_utc"))
              .withColumn("month", F.month("ts_utc")))


def write_silver(df: DataFrame, path: str, label: str) -> int:
    """Ecrasement dynamique : seules les partitions presentes dans df sont
    remplacees. Sans dynamic, overwrite detruirait toute la table."""
    n = df.count()
    if n == 0:
        log.warning("%s : 0 ligne, ecriture ignoree.", label)
        return 0
    (df.repartition("year", "month")
       .write.mode("overwrite")
       .partitionBy("year", "month")
       .parquet(path))
    log.info("%s : %d ligne(s) ecrite(s) dans %s", label, n, path)
    return n


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    fs = cfg["hdfs"]["fs_uri"]

    spark = (SparkSession.builder.appName("silver-grid")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    parts = [d for d in (read_bronze_batch(spark, layout, fs, args.start, args.end),
                         read_bronze_stream(spark, layout, fs)) if d is not None]
    if not parts:
        log.error("Aucune donnee Bronze. Lancer l'ingestion d'abord.")
        return 1

    loads, gens = [], []
    for df in parts:
        norm = normalize_time(df)
        norm = norm.filter((F.col("ts_utc") >= F.lit(args.start)) &
                           (F.col("ts_utc") < F.date_add(F.lit(args.end).cast("date"), 1)))
        loads.append(build_grid_load(norm))
        try:
            gens.append(build_grid_generation(norm))
        except ValueError as exc:
            log.warning("Depivotement impossible sur une source : %s", exc)

    from functools import reduce
    load_df = reduce(DataFrame.unionByName, loads)
    load_df = add_partitions(dedupe(load_df, ["ts_utc", "zone_id"]))
    write_silver(load_df, f"{fs}{layout.silver('grid_load')}", "grid_load")

    if gens:
        gen_df = reduce(DataFrame.unionByName, gens)
        gen_df = add_partitions(dedupe(gen_df, ["ts_utc", "zone_id", "filiere"]))
        write_silver(gen_df, f"{fs}{layout.silver('grid_generation')}",
                     "grid_generation")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
