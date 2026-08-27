"""
DAGs du datalake energie.

    dag_ingest_batch  @daily  ─┐
                               ├─→ dag_silver ─→ dag_gold ─→ dag_ml
    dag_stream_bronze @*/15   ─┘

Deux choix a defendre en soutenance :

1. DEPENDANCES PAR DATASETS, pas par ExternalTaskSensor. Chaque tache
   declare ce qu'elle produit (outlets) ; le DAG aval se declenche seul
   quand ses entrees sont fraiches. Aucun sensor ne bloque un worker.

2. IDEMPOTENCE PAR FENETRE. Chaque job recoit data_interval_start et
   data_interval_end en argument. Un rejeu Airflow d'un intervalle passe
   retraite exactement les memes donnees et ecrase exactement les memes
   partitions. Relancer deux fois produit le meme resultat.

Le streaming ne rentre pas dans le modele DAG (processus permanent), donc
on le pilote en micro-batch : trigger availableNow declenche toutes les
15 minutes. Plus simple a demontrer qu'un job YARN permanent, et suffisant
pour le debit d'eco2mix.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

SRC = "/opt/datalake/src"
CONF = os.environ.get("DATALAKE_CONF", "/opt/datalake/conf/sources.yml")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Jetons de dependance entre DAGs.
DS_BRONZE_BATCH = Dataset("hdfs://namenode:8020/datalake/bronze/eco2mix_cons")
DS_BRONZE_METEO = Dataset("hdfs://namenode:8020/datalake/bronze/meteo_archive")
DS_BRONZE_STREAM = Dataset("hdfs://namenode:8020/datalake/bronze/eco2mix_tr")
DS_SILVER = Dataset("hdfs://namenode:8020/datalake/silver/grid_load")
DS_GOLD = Dataset("hdfs://namenode:8020/datalake/gold/mix_horaire")

DEFAULTS = {
    "owner": "tp-bigdata",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,   # chaque intervalle est independant
    "email_on_failure": False,
}

# Fenetre passee au job. Les macros Airflow garantissent qu'un rejeu
# manuel utilise le MEME intervalle que le run d'origine.
WINDOW = "--start {{ data_interval_start | ds }} --end {{ data_interval_end | ds }}"


def spark_submit(script: str, args: str = WINDOW, packages: str = "") -> str:
    pkg = f"--packages {packages} " if packages else ""
    return (
        f"spark-submit --master {SPARK_MASTER} "
        f"--conf spark.sql.sources.partitionOverwriteMode=dynamic "
        f"--conf spark.sql.session.timeZone=UTC "
        f"{pkg}{script} {args} --conf {CONF}"
    )


# ---------------------------------------------------------------------------
# 1. Ingestion batch
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_ingest_batch",
    description="Bronze : CSV eco2mix + JSON meteo",
    start_date=datetime(2024, 3, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,          # jamais deux ingestions concurrentes
    default_args=DEFAULTS,
    tags=["bronze", "batch"],
) as dag_ingest:

    # Ces deux taches sont independantes : elles tournent en parallele.
    # Le marker _SUCCESS rend chacune rejouable sans duplication.
    ingest_eco2mix = BashOperator(
        task_id="ingest_eco2mix_csv",
        bash_command=(
            f"python {SRC}/ingestion/batch_eco2mix_cons.py {WINDOW} --conf {CONF}"
        ),
        outlets=[DS_BRONZE_BATCH],
    )

    ingest_meteo = BashOperator(
        task_id="ingest_open_meteo",
        bash_command=(
            f"python {SRC}/ingestion/batch_open_meteo.py {WINDOW} --conf {CONF}"
        ),
        outlets=[DS_BRONZE_METEO],
    )

    # ENTSO-E : sort en code 0 si le token est absent, ne bloque donc rien.
    ingest_entsoe = BashOperator(
        task_id="ingest_entsoe_xml",
        bash_command=(
            f"python {SRC}/ingestion/batch_entsoe.py {WINDOW} --conf {CONF}"
        ),
        trigger_rule="all_done",
    )

    done = EmptyOperator(task_id="bronze_batch_ready", trigger_rule="none_failed")

    [ingest_eco2mix, ingest_meteo, ingest_entsoe] >> done


# ---------------------------------------------------------------------------
# 2. Streaming -> Bronze
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_stream_bronze",
    description="Bronze : micro-batch Kafka -> HDFS",
    start_date=datetime(2024, 3, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,          # essentiel : deux jobs sur le meme
                                # checkpoint se marcheraient dessus
    default_args={**DEFAULTS, "retries": 1},
    tags=["bronze", "streaming"],
) as dag_stream:

    BashOperator(
        task_id="consume_kafka_to_bronze",
        bash_command=spark_submit(
            f"{SRC}/ingestion/stream_bronze_eco2mix.py",
            args="--trigger availableNow",
            packages=KAFKA_PKG,
        ),
        outlets=[DS_BRONZE_STREAM],
        execution_timeout=timedelta(minutes=12),
    )


# ---------------------------------------------------------------------------
# 3. Silver
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_silver",
    description="Silver : validation, dedup, normalisation UTC",
    start_date=datetime(2024, 3, 1),
    # Se declenche des que Bronze batch OU streaming est rafraichi.
    schedule=[DS_BRONZE_BATCH, DS_BRONZE_METEO, DS_BRONZE_STREAM],
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["silver"],
) as dag_silver:

    silver_grid = BashOperator(
        task_id="silver_grid",
        bash_command=spark_submit(f"{SRC}/silver/silver_grid.py"),
        outlets=[DS_SILVER],
    )

    silver_weather = BashOperator(
        task_id="silver_weather",
        bash_command=spark_submit(f"{SRC}/silver/silver_weather.py"),
    )

    [silver_grid, silver_weather]


# ---------------------------------------------------------------------------
# 4. Gold
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_gold",
    description="Gold : mix_horaire, kpi_daily, ml_features",
    start_date=datetime(2024, 3, 1),
    schedule=[DS_SILVER],
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["gold"],
) as dag_gold:

    BashOperator(
        task_id="build_gold_tables",
        bash_command=spark_submit(f"{SRC}/gold/gold_build.py"),
        outlets=[DS_GOLD],
    )


# ---------------------------------------------------------------------------
# 5. ML (bonus)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="dag_ml_train",
    description="Bonus : prevision de consommation a H+24",
    start_date=datetime(2024, 3, 1),
    schedule=[DS_GOLD],
    catchup=False,
    default_args=DEFAULTS,
    tags=["ml", "bonus"],
) as dag_ml:

    BashOperator(
        task_id="train_forecast_model",
        bash_command=(
            f"python {SRC}/ml/train_forecast.py --conf {CONF} "
            f"--out /opt/datalake/models"
        ),
    )
