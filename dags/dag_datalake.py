"""
DAGs du datalake energie.

    dag_ingest_batch  @daily  ─┐
                               ├─→ dag_silver ─→ dag_gold ─→ dag_ml
    dag_stream_bronze @*/15   ─┘

Deux choix a defendre en soutenance :

1. DEPENDANCES PAR DATASETS, pas par ExternalTaskSensor. Chaque tache
   declare ce qu'elle produit (outlets) ; le DAG aval se declenche seul
   quand ses entrees sont fraiches. Aucun sensor ne bloque un worker.

2. IDEMPOTENCE PAR FENETRE. Chaque job recoit une fenetre alignee sur le
   mois, unite de partition des tables Silver et Gold. Un rejeu Airflow d'un
   intervalle passe retraite exactement les memes donnees et ecrase
   exactement les memes partitions. Relancer deux fois produit le meme
   resultat. Voir docs/decisions.md, qui fait foi.

Le streaming ne rentre pas dans le modele DAG (processus permanent), donc
on le pilote en micro-batch : trigger availableNow declenche toutes les
15 minutes. Plus simple a demontrer qu'un job YARN permanent, et suffisant
pour le debit d'eco2mix.

REJOUER UN MOIS, OU EN CHARGER PLUSIEURS

Les DAGs a fenetre exposent deux parametres, remplissables dans "Trigger DAG
w/ config" :

    month       AAAA-MM   le mois a traiter, quelle que soit la date du run
    month_end   AAAA-MM   dernier mois d'un backfill ; vide = un seul mois

Vides, le comportement est celui du run : le mois de son propre intervalle.

Un declenchement manuel depuis l'interface ne peut pas faire autrement que
prendre l'intervalle courant, qui n'est pas celui des donnees disponibles :
c'est exactement ce que `month` sert a contourner.

Backfill de deux ans, en ligne de commande. Les DAGs se declenchent en
cascade par Datasets, mais un run declenche par Dataset ne recoit PAS la
conf du run amont : il faut donc passer la meme conf aux trois, dans l'ordre,
en attendant la fin de chacun.

    C='{"month": "2023-01", "month_end": "2024-12"}'
    docker compose exec airflow-scheduler airflow dags trigger dag_ingest_batch --conf "$C"
    docker compose exec airflow-scheduler airflow dags trigger dag_silver       --conf "$C"
    docker compose exec airflow-scheduler airflow dags trigger dag_gold         --conf "$C"

L'ingestion Bronze est reprenable : chaque mois porte son marker _SUCCESS et
un lot deja telecharge est saute. Une interruption au 14e mois se rattrape en
relancant la meme commande.
"""

from __future__ import annotations

import calendar
import os
from datetime import date, datetime, timedelta

from airflow import DAG, Dataset
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
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

# ---------------------------------------------------------------------------
# Fenetre passee aux jobs : LE MOIS qui contient le debut de l'intervalle.
#
# Deux problemes que cette forme resout, et un piege qu'elle evite.
#
# 1. Les tables Silver sont partitionnees par year/month et leurs jobs
#    ecrivent la fenetre qu'ils recoivent SANS l'aligner eux-memes
#    (docs/decisions.md : "non — couvert par le DAG"). C'est donc ici que
#    l'alignement doit se faire. En ecrasement dynamique, ecrire un demi-mois
#    dans une partition mensuelle ne met pas a jour une moitie : il supprime
#    l'autre. Gold realigne de son cote, mais recevoir des bornes deja
#    alignees rend les journaux des deux couches comparables.
#
# 2. `data_interval_end` est EXCLUSIVE cote Airflow, alors que tous les jobs
#    du depot lisent `--end` comme un jour INCLUS : silver.transform.
#    restrict_window ("bornes de jour incluses"), common.hdfs_io.
#    partition_months, gold.windows.TimeWindow. La passer telle quelle
#    ajoutait un jour, et le run du 31 mars debordait sur avril entier.
#
# Le piege : corriger le point 2 par `data_interval_end.subtract(days=1)`
# fabrique une fenetre INVERSEE. Les DAGs declenches par Dataset — silver,
# gold, ml — n'ont pas d'intervalle de temps : Airflow y pose
# data_interval_start == data_interval_end. Retrancher un jour a la borne
# haute la fait passer AVANT la borne basse, et le mois d'avant des que le
# declenchement tombe un 1er. Constate au rendu : le 2024-03-01 donnait
# `--start 2024-03-01 --end 2024-02-29`.
#
# On ne se sert donc pas du tout de la borne haute d'Airflow. La fenetre est
# le mois de `data_interval_start`, ce qui donne la meme reponse pour un
# intervalle quotidien, pour un intervalle mensuel et pour un declenchement
# par Dataset — et une borne `--end` inclusive par construction.
# ---------------------------------------------------------------------------
# Le calcul est en Python et non en Jinja : il porte deux cas et une
# validation, et un gabarit de trois lignes imbriquees ne se relit pas. Il est
# expose aux templates par `user_defined_macros`.
MOIS_FORMAT = r"^$|^\d{4}-(0[1-9]|1[0-2])$"


def _premier_du_mois(valeur: str | None, defaut: date | None = None) -> date | None:
    """'2024-03' -> date(2024, 3, 1). Vide -> `defaut`."""
    texte = str(valeur or "").strip()
    if not texte:
        return defaut
    try:
        annee, mois = (int(part) for part in texte.split("-")[:2])
        return date(annee, mois, 1)
    except (TypeError, ValueError) as exc:
        raise AirflowFailException(
            f"Mois invalide : {texte!r}. Format attendu AAAA-MM, par exemple "
            "2024-03."
        ) from exc


def fenetre_mois(params, data_interval_start) -> str:
    """Les arguments --start / --end du job, alignes sur des mois entiers.

    Trois usages, un seul chemin de code :

      params vides                 le mois de l'intervalle du run
      month=2024-03                ce mois-la, quelle que soit la date du run
      month=2023-01 month_end=2024-12   les 24 mois, pour un backfill

    Une fenetre multi-mois reste alignee : chaque partition year/month qu'elle
    couvre est recalculee en entier, donc l'invariant de docs/decisions.md
    tient aussi bien sur 24 mois que sur un seul.
    """
    par_defaut = date(data_interval_start.year, data_interval_start.month, 1)
    debut = _premier_du_mois(params.get("month"), defaut=par_defaut)
    dernier_mois = _premier_du_mois(params.get("month_end"), defaut=debut)

    if dernier_mois < debut:
        raise AirflowFailException(
            f"Fenetre inversee : month={debut:%Y-%m} est apres "
            f"month_end={dernier_mois:%Y-%m}."
        )

    fin = date(dernier_mois.year, dernier_mois.month,
               calendar.monthrange(dernier_mois.year, dernier_mois.month)[1])
    return f"--start {debut.isoformat()} --end {fin.isoformat()}"


WINDOW = "{{ fenetre_mois(params, data_interval_start) }}"

MACROS = {"fenetre_mois": fenetre_mois}

# Les deux champs proposes dans "Trigger DAG w/ config". Laisses vides, le DAG
# se comporte exactement comme avant : le mois de son propre intervalle.
PARAMS_FENETRE = {
    "month": Param(
        default="",
        type="string",
        pattern=MOIS_FORMAT,
        title="Mois a traiter (AAAA-MM)",
        description="Vide : le mois de l'intervalle du run. Rempli : ce "
                    "mois-la, quelle que soit la date de declenchement. "
                    "C'est le champ a remplir pour rejouer un mois passe "
                    "depuis l'interface.",
    ),
    "month_end": Param(
        default="",
        type="string",
        pattern=MOIS_FORMAT,
        title="Dernier mois d'un backfill (AAAA-MM)",
        description="Vide : un seul mois, celui ci-dessus. Rempli : traite "
                    "tous les mois de `month` a `month_end` inclus, en une "
                    "seule fenetre. Les lots deja ingeres sont sautes grace "
                    "aux markers _SUCCESS, donc une relance reprend ou elle "
                    "s'etait arretee.",
    ),
}


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
    params=PARAMS_FENETRE,
    user_defined_macros=MACROS,
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
    params=PARAMS_FENETRE,
    user_defined_macros=MACROS,
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
    params=PARAMS_FENETRE,
    user_defined_macros=MACROS,
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
