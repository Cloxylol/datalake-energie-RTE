#!/usr/bin/env python3
"""
Spark Structured Streaming : Kafka -> Bronze (JSON brut sur HDFS).

Bronze conserve le format brut d'origine, sans transformation. On ne parse
donc PAS le JSON ici : la valeur Kafka est stockee telle quelle en chaine.
Le seul ajout est un jeu de colonnes techniques (offset, partition, instant
d'ingestion) qui rend la reprise auditable.

PARTITIONNEMENT sur ingest_date / ingest_hour, c'est-a-dire la date
D'INGESTION et non la date metier. Au moment de l'ecriture streaming on ne
connait pas la date metier de chaque message sans payer un shuffle, ce qui
tuerait la latence. Silver rebascule sur la date metier.

EXACTLY-ONCE : le checkpointLocation gere les offsets Kafka et le repertoire
_spark_metadata du sink fichier garantit qu'un micro-batch interrompu n'est
pas compte deux fois. Ce repertoire ne doit JAMAIS etre supprime entre deux
runs, sinon on reingere tout depuis earliest.

Deux modes :
  --trigger availableNow : traite ce qui est disponible puis sort.
                           C'est le mode pilote par Airflow (recommande).
  --trigger continuous   : processus permanent avec micro-batch de 30 s.

Soumission :
  spark-submit \
    --master yarn --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    stream_bronze_eco2mix.py --trigger availableNow
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, load_config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [stream-bronze] %(message)s",
)
log = logging.getLogger(__name__)

SOURCE = "eco2mix_tr"


def build_session(app_name: str = "bronze-eco2mix-stream") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Fichiers de sortie raisonnables : le debit est faible, on evite
        # de generer des milliers de micro-fichiers qui tueraient le NameNode.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.stateStore.compression.codec", "zstd")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, cfg: dict, starting: str):
    """Source Kafka. failOnDataLoss=false : un topic purge ne doit pas
    faire tomber le job, Bronze fait deja foi."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["kafka"]["bootstrap_servers"])
        .option("subscribe", cfg["kafka"]["topics"]["eco2mix_tr"])
        .option("startingOffsets", starting)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5000)
        .load()
    )


def to_bronze(df):
    """Colonnes techniques + partitions d'ingestion. Le payload reste brut."""
    return (
        df.select(
            # La valeur brute, non parsee : c'est le contrat de Bronze.
            F.col("value").cast("string").alias("raw_value"),
            F.col("key").cast("string").alias("message_key"),
            # Provenance Kafka : permet de rejouer un intervalle d'offsets
            # precis en cas de doute sur un lot.
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("bronze_ingested_at"),
        )
        .withColumn("ingest_date", F.to_date("bronze_ingested_at"))
        .withColumn("ingest_hour", F.date_format("bronze_ingested_at", "HH"))
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conf", default=None)
    ap.add_argument(
        "--trigger", choices=["availableNow", "continuous"], default="availableNow"
    )
    ap.add_argument("--interval", default="30 seconds", help="si --trigger continuous")
    ap.add_argument(
        "--starting-offsets",
        default="earliest",
        help="ignore si un checkpoint existe deja",
    )
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    fs = cfg["hdfs"]["fs_uri"]

    out_path = f"{fs}{layout.bronze_stream(SOURCE)}"
    ckpt_path = f"{fs}{layout.checkpoint(SOURCE)}"

    log.info("Sortie Bronze  : %s", out_path)
    log.info("Checkpoint     : %s", ckpt_path)
    log.info("Trigger        : %s", args.trigger)

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    stream = to_bronze(read_kafka(spark, cfg, args.starting_offsets))

    writer = (
        stream.writeStream.format("json")
        .outputMode("append")
        .option("path", out_path)
        .option("checkpointLocation", ckpt_path)
        .option("compression", "gzip")
        .partitionBy("ingest_date", "ingest_hour")
    )

    query = (
        writer.trigger(availableNow=True).start()
        if args.trigger == "availableNow"
        else writer.trigger(processingTime=args.interval).start()
    )

    query.awaitTermination()

    if args.trigger == "availableNow":
        prog = query.lastProgress
        if prog:
            log.info(
                "Micro-batch termine : %s ligne(s) ecrite(s).",
                prog.get("numInputRows", 0),
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
