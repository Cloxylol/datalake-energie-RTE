#!/usr/bin/env python3
"""
Silver : Bronze meteo_archive (JSON Open-Meteo) -> silver/weather.

Le travail specifique de cette source : Open-Meteo renvoie des TABLEAUX
PARALLELES, pas des lignes.

    {"hourly": {"time": [t0, t1, ...],
                "temperature_2m": [v0, v1, ...], ...}}

hourly.time[i] correspond a hourly.temperature_2m[i]. On passe en lignes
avec arrays_zip + explode. C'est exactement le genre de normalisation qui
justifie l'existence de la couche Silver.

Produit aussi la temperature nationale ponderee (zone fr), qui servira de
feature au modele : la consommation electrique francaise depend surtout de
la temperature moyenne ponderee par la population.

    spark-submit silver_weather.py --start 2024-03-01 --end 2024-03-31
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s [silver-weather] %(message)s")
log = logging.getLogger(__name__)

VAR_MAP = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "wind_speed_10m": "wind_speed_ms",
    "cloud_cover": "cloud_cover_pct",
}


def explode_hourly(df: DataFrame) -> DataFrame:
    """arrays_zip + explode : tableaux paralleles -> lignes."""
    hourly_cols = [c for c in df.select("hourly.*").columns]
    present = [v for v in VAR_MAP if v in hourly_cols]
    if not present:
        raise ValueError(f"Aucune variable connue dans hourly : {hourly_cols}")
    log.info("Variables horaires : %s", ", ".join(present))

    zipped = F.arrays_zip(
        F.col("hourly.time").alias("time"),
        *[F.col(f"hourly.{v}").alias(v) for v in present],
    )

    out = (df
           .withColumn("z", F.explode(zipped))
           .withColumn("ts_utc", F.to_timestamp(F.col("z.time")))
           .withColumn("city_lat", F.col("latitude"))
           .withColumn("city_lon", F.col("longitude")))

    for src in present:
        out = out.withColumn(VAR_MAP[src], F.col(f"z.{src}").cast("double"))

    for tgt in VAR_MAP.values():
        if tgt not in out.columns:
            out = out.withColumn(tgt, F.lit(None).cast("double"))

    return out.drop("z", "hourly", "hourly_units")


def national_average(df: DataFrame, weights: dict[str, float]) -> DataFrame:
    """Temperature France = moyenne ponderee des villes.

    Ponderation renormalisee sur les villes reellement presentes, sinon un
    lot manquant biaiserait la moyenne vers le bas.
    """
    wmap = F.create_map(*[x for k, v in weights.items()
                          for x in (F.lit(k), F.lit(float(v)))])

    weighted = df.withColumn("w", F.coalesce(wmap[F.col("zone_id")], F.lit(0.0)))

    aggs = [
        (F.sum(F.col(c) * F.col("w")) / F.sum(F.when(F.col(c).isNotNull(),
                                                     F.col("w")))).alias(c)
        for c in VAR_MAP.values()
    ]

    return (weighted.groupBy("ts_utc").agg(*aggs)
            .withColumn("zone_id", F.lit("fr"))
            .withColumn("source", F.lit("open_meteo"))
            .withColumn("quality", F.lit("consolidated"))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.lit("aggregat_pondere")))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--conf", default=None)
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    fs = cfg["hdfs"]["fs_uri"]

    spark = (SparkSession.builder.appName("silver-weather")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    src = f"{fs}{layout.root}/bronze/meteo_archive"
    # basePath : Spark recupere city= depuis le chemin comme colonne.
    raw = (spark.read.option("basePath", src)
           .json(f"{src}/city=*/year=*/month=*/*.json"))

    if raw.rdd.isEmpty():
        log.error("Aucune donnee meteo en Bronze.")
        return 1

    df = explode_hourly(raw).withColumnRenamed("city", "zone_id")

    per_city = (df
                .withColumn("source", F.lit("open_meteo"))
                .withColumn("quality", F.lit("consolidated"))
                .withColumn("ingested_at", F.current_timestamp())
                .withColumn("source_file", F.input_file_name())
                .select("ts_utc", "zone_id", "source", "quality",
                        "ingested_at", "source_file", *VAR_MAP.values())
                # Garde-fou : -50 a +55 degres en France, c'est genereux.
                .filter(F.col("temperature_c").between(-50, 55)))

    national = national_average(per_city, cfg["weather_weights"]) \
        .select(*per_city.columns)

    out = (per_city.unionByName(national)
           .dropDuplicates(["ts_utc", "zone_id"])
           .filter((F.col("ts_utc") >= F.lit(args.start)) &
                   (F.col("ts_utc") < F.date_add(F.lit(args.end).cast("date"), 1)))
           .withColumn("ts_local", F.from_utc_timestamp("ts_utc", "Europe/Paris"))
           .withColumn("year", F.year("ts_utc"))
           .withColumn("month", F.month("ts_utc")))

    n = out.count()
    (out.repartition("year", "month").write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{fs}{layout.silver('weather')}"))
    log.info("weather : %d ligne(s) ecrite(s).", n)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
