#!/usr/bin/env python3
"""
Gold : Silver -> mix_horaire, kpi_daily, ml_features.

C'est ici que les deux sources se croisent, sur la cle commune
(ts_utc, zone_id). Si cette jointure produit des lignes, le datalake tient
sa promesse : ce ne sont pas deux pipelines paralleles.

Trois tables :
  mix_horaire  : conso + production par filiere + meteo, au pas horaire
  kpi_daily    : agregats metier prets pour la restitution
  ml_features  : cible a H+24, lags, features calendaires et meteo

    spark-submit gold_build.py --start 2024-03-01 --end 2024-03-31
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s [gold] %(message)s")
log = logging.getLogger(__name__)

# Seuil de confort : base des degres-jours. 18 C est la convention en France.
BASE_TEMP = 18.0


def to_hourly(df: DataFrame, ts_col: str = "ts_utc") -> DataFrame:
    """eco2mix est au quart d'heure, la meteo a l'heure. On aligne sur
    l'heure : c'est le plus petit denominateur commun, et il faut le
    justifier plutot que de le subir."""
    return df.withColumn("ts_hour", F.date_trunc("hour", F.col(ts_col)))


def build_mix_horaire(load: DataFrame, gen: DataFrame,
                      weather: DataFrame) -> DataFrame:
    """La table de jointure : le produit du croisement des sources."""

    load_h = (to_hourly(load)
              .groupBy("ts_hour", "zone_id")
              .agg(F.avg("consumption_mw").alias("consumption_mw"),
                   F.avg("forecast_j1_mw").alias("forecast_j1_mw"),
                   F.avg("forecast_j_mw").alias("forecast_j_mw"),
                   F.avg("co2_rate_g_kwh").alias("co2_rate_g_kwh")))

    # Production : pivot des filieres en colonnes + totaux.
    gen_h = to_hourly(gen).groupBy("ts_hour", "zone_id", "filiere") \
                          .agg(F.avg("generation_mw").alias("mw"),
                               F.first("is_renewable").alias("is_renew"))

    pivoted = (gen_h.groupBy("ts_hour", "zone_id")
               .pivot("filiere")
               .agg(F.first("mw")))

    totals = (gen_h.groupBy("ts_hour", "zone_id")
              .agg(F.sum("mw").alias("generation_total_mw"),
                   F.sum(F.when(F.col("is_renew"), F.col("mw"))
                         .otherwise(0)).alias("generation_renewable_mw")))

    weather_h = (to_hourly(weather)
                 .filter(F.col("zone_id") == "fr")
                 .groupBy("ts_hour")
                 .agg(F.avg("temperature_c").alias("temperature_c"),
                      F.avg("humidity_pct").alias("humidity_pct"),
                      F.avg("wind_speed_ms").alias("wind_speed_ms"),
                      F.avg("cloud_cover_pct").alias("cloud_cover_pct")))

    out = (load_h
           .join(pivoted, ["ts_hour", "zone_id"], "left")
           .join(totals, ["ts_hour", "zone_id"], "left")
           .join(weather_h, ["ts_hour"], "left")
           .withColumnRenamed("ts_hour", "ts_utc")
           .withColumn("ts_local", F.from_utc_timestamp("ts_utc", "Europe/Paris"))
           .withColumn("renewable_share_pct",
                       F.when(F.col("generation_total_mw") > 0,
                              100 * F.col("generation_renewable_mw")
                              / F.col("generation_total_mw")))
           .withColumn("forecast_error_mw",
                       F.col("consumption_mw") - F.col("forecast_j1_mw")))

    return out.withColumn("year", F.year("ts_utc")) \
              .withColumn("month", F.month("ts_utc"))


def build_kpi_daily(mix: DataFrame) -> DataFrame:
    """KPIs metier, directement lisibles par le notebook."""
    day = mix.withColumn("date_local", F.to_date("ts_local"))

    peak = (day.withColumn(
        "_rk", F.row_number().over(
            Window.partitionBy("date_local")
                  .orderBy(F.col("consumption_mw").desc_nulls_last())))
        .filter(F.col("_rk") == 1)
        .select("date_local",
                F.col("consumption_mw").alias("peak_mw"),
                F.hour("ts_local").alias("peak_hour_local")))

    agg = (day.groupBy("date_local").agg(
        (F.sum("consumption_mw") / 1000).alias("consumption_gwh"),
        F.avg("consumption_mw").alias("consumption_avg_mw"),
        F.avg("co2_rate_g_kwh").alias("co2_avg_g_kwh"),
        F.avg("renewable_share_pct").alias("renewable_share_pct"),
        F.avg("temperature_c").alias("temperature_avg_c"),
        F.min("temperature_c").alias("temperature_min_c"),
        F.max("temperature_c").alias("temperature_max_c"),
        F.avg(F.abs("forecast_error_mw")).alias("forecast_mae_mw"),
        F.count("*").alias("n_hours")))

    return (agg.join(peak, "date_local", "left")
            # Degres-jours : la variable qui explique le mieux la conso.
            .withColumn("hdd", F.greatest(F.lit(BASE_TEMP) - F.col("temperature_avg_c"),
                                          F.lit(0.0)))
            .withColumn("cdd", F.greatest(F.col("temperature_avg_c") - F.lit(BASE_TEMP),
                                          F.lit(0.0)))
            .withColumn("year", F.year("date_local")))


def build_ml_features(mix: DataFrame, holidays: list[str] | None = None) -> DataFrame:
    """Cible : consommation a H+24. Features : lags, calendrier, meteo.

    La prevision J-1 de RTE est conservee comme colonne de reference :
    c'est le benchmark contre lequel comparer le modele, et il est deja
    dans les donnees.
    """
    w = Window.partitionBy("zone_id").orderBy(F.col("ts_utc").cast("long"))

    df = (mix
          .withColumn("target_consumption_h24", F.lead("consumption_mw", 24).over(w))
          .withColumn("lag_24h", F.lag("consumption_mw", 24).over(w))
          .withColumn("lag_48h", F.lag("consumption_mw", 48).over(w))
          .withColumn("lag_168h", F.lag("consumption_mw", 168).over(w))
          .withColumn("roll_mean_24h",
                      F.avg("consumption_mw").over(w.rowsBetween(-23, 0)))
          .withColumn("roll_std_24h",
                      F.stddev("consumption_mw").over(w.rowsBetween(-23, 0)))
          # Calendrier sur l'heure LOCALE : la conso suit le rythme humain,
          # pas UTC.
          .withColumn("hour", F.hour("ts_local"))
          .withColumn("dow", F.dayofweek("ts_local"))
          .withColumn("month_of_year", F.month("ts_local"))
          .withColumn("is_weekend", F.col("dow").isin(1, 7).cast("int"))
          # Encodage cyclique : 23h et 0h sont voisines.
          .withColumn("hour_sin", F.sin(2 * F.lit(3.14159265) * F.col("hour") / 24))
          .withColumn("hour_cos", F.cos(2 * F.lit(3.14159265) * F.col("hour") / 24))
          .withColumn("hdd", F.greatest(F.lit(BASE_TEMP) - F.col("temperature_c"),
                                        F.lit(0.0)))
          .withColumn("cdd", F.greatest(F.col("temperature_c") - F.lit(BASE_TEMP),
                                        F.lit(0.0))))

    if holidays:
        df = df.withColumn(
            "is_holiday",
            F.to_date("ts_local").cast("string").isin(holidays).cast("int"))
    else:
        df = df.withColumn("is_holiday", F.lit(0))

    return (df.filter(F.col("target_consumption_h24").isNotNull())
              .select("ts_utc", "ts_local", "zone_id",
                      "target_consumption_h24", "consumption_mw",
                      "lag_24h", "lag_48h", "lag_168h",
                      "roll_mean_24h", "roll_std_24h",
                      "hour", "dow", "month_of_year", "is_weekend",
                      "hour_sin", "hour_cos", "is_holiday",
                      "temperature_c", "hdd", "cdd", "wind_speed_ms",
                      "cloud_cover_pct",
                      # Benchmark RTE : le modele doit se comparer a ca.
                      F.col("forecast_j1_mw").alias("rte_forecast_j1_mw"),
                      "year", "month"))


def fetch_holidays() -> list[str]:
    """Jours feries francais. API publique sans cle. Echec non bloquant."""
    try:
        import requests
        r = requests.get("https://calendrier.api.gouv.fr/jours-feries/metropole.json",
                         timeout=15)
        r.raise_for_status()
        days = sorted(r.json().keys())
        log.info("%d jours feries recuperes.", len(days))
        return days
    except Exception as exc:  # noqa: BLE001
        log.warning("Jours feries indisponibles (%s), feature mise a 0.", exc)
        return []


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--conf", default=None)
    ap.add_argument("--no-holidays", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    fs = cfg["hdfs"]["fs_uri"]

    spark = (SparkSession.builder.appName("gold-build")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    load = spark.read.parquet(f"{fs}{layout.silver('grid_load')}")
    gen = spark.read.parquet(f"{fs}{layout.silver('grid_generation')}")
    try:
        weather = spark.read.parquet(f"{fs}{layout.silver('weather')}")
    except Exception:  # noqa: BLE001
        log.warning("silver/weather absent : Gold sans meteo.")
        weather = spark.createDataFrame(
            [], "ts_utc timestamp, zone_id string, temperature_c double, "
                "humidity_pct double, wind_speed_ms double, cloud_cover_pct double")

    mix = build_mix_horaire(load, gen, weather)
    mix = mix.filter((F.col("ts_utc") >= F.lit(args.start)) &
                     (F.col("ts_utc") < F.date_add(F.lit(args.end).cast("date"), 1)))
    mix.cache()

    n_mix = mix.count()
    n_joined = mix.filter(F.col("temperature_c").isNotNull()).count()
    log.info("mix_horaire : %d ligne(s), dont %d avec meteo jointe (%.0f%%).",
             n_mix, n_joined, 100 * n_joined / max(n_mix, 1))
    if n_mix and not n_joined:
        log.warning("AUCUNE jointure meteo : verifier que les deux sources "
                    "couvrent bien la meme periode.")

    (mix.write.mode("overwrite").partitionBy("year", "month")
        .parquet(f"{fs}{layout.gold('mix_horaire')}"))

    kpi = build_kpi_daily(mix)
    (kpi.write.mode("overwrite").partitionBy("year")
        .parquet(f"{fs}{layout.gold('kpi_daily')}"))
    log.info("kpi_daily : %d jour(s).", kpi.count())

    holidays = [] if args.no_holidays else fetch_holidays()
    feats = build_ml_features(mix, holidays)
    (feats.write.mode("overwrite").partitionBy("year", "month")
        .parquet(f"{fs}{layout.gold('ml_features')}"))
    log.info("ml_features : %d ligne(s).", feats.count())

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
