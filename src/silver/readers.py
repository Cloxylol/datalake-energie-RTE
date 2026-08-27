#!/usr/bin/env python3
"""
Lecture des lots Bronze, pilotee par la section `sources` du mapping.

Trois formats coexistent en Bronze, et c'est voulu : Bronze conserve la
forme d'origine de chaque source, c'est Silver qui unifie.

  csv            export mensuel eco2mix, delimiteur d'origine conserve
  json_envelope  sortie du streaming : la valeur Kafka brute est stockee en
                 chaine sous raw_value, encapsulee par le producteur dans
                 {_meta, payload}. Bronze ne parse pas, Silver parse.
  json_arrays    Open-Meteo : tableaux paralleles hourly.time[] /
                 hourly.temperature_2m[], a zipper puis exploser.

Chaque lecteur renvoie None plutot que de lever quand la source est
absente : un datalake ou l'on n'a ingere que le batch doit pouvoir
construire Silver.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .mapping import SilverMapping, SourceSpec
from .validation import ValidationError, check_required

log = logging.getLogger(__name__)


def _path(fs: str, root: str, spec: SourceSpec) -> str:
    return f"{fs}{root}/{spec.bronze}"


def read_csv(spark: SparkSession, spec: SourceSpec, base: str) -> DataFrame | None:
    """Export CSV mensuel.

    inferSchema est actif volontairement : les colonnes varient selon les
    annees et l'inference sert a LIRE. Le schema Silver, lui, vient du
    mapping et est applique par un cast explicite juste apres.
    """
    opts = spec.options.get("csv", {})
    try:
        df = (spark.read
              .option("header", opts.get("header", True))
              .option("sep", opts.get("sep", ";"))
              .option("inferSchema", opts.get("infer_schema", True))
              .option("mode", "PERMISSIVE")
              .csv(f"{base}/{spec.glob}"))
    except Exception as exc:  # noqa: BLE001
        log.warning("Aucun lot lisible pour %s : %s", spec.name, exc)
        return None

    if not df.columns:
        return None
    log.info("%s : %d colonne(s) detectee(s).", spec.name, len(df.columns))
    return df.withColumn("source_file", F.input_file_name())


def read_json_envelope(spark: SparkSession, spec: SourceSpec,
                       base: str) -> DataFrame | None:
    """Sortie du streaming : payload JSON encapsule dans une chaine.

    Le schema du payload est decouvert sur un echantillon plutot que fige :
    le flux temps reel a 41 champs la ou le consolide en a 37, et la liste
    bouge quand RTE ajoute une filiere.
    """
    env = spec.options.get("envelope", {})
    raw_col = env.get("raw_column", "raw_value")

    try:
        raw = spark.read.json(f"{base}/{spec.glob}")
    except Exception as exc:  # noqa: BLE001
        log.warning("Aucun lot lisible pour %s : %s", spec.name, exc)
        return None

    if raw_col not in raw.columns:
        log.warning("Colonne %s absente de %s : lot ignore.", raw_col, spec.name)
        return None

    sample = raw.select(raw_col).limit(int(env.get("sample_size", 1000)))
    rows = sample.rdd.map(lambda r: r[0]).filter(lambda v: v is not None)
    if rows.isEmpty():
        log.warning("%s : aucun payload exploitable.", spec.name)
        return None
    inferred = spark.read.json(rows).schema

    payload = env.get("payload_path", "payload")
    ingested = env.get("ingested_at_column", "bronze_ingested_at")

    parsed = raw.withColumn("_j", F.from_json(raw_col, inferred))
    if payload not in [f.name for f in inferred.fields]:
        raise ValidationError(
            f"{spec.name} : chemin '{payload}' absent de l'enveloppe "
            f"({[f.name for f in inferred.fields]}). Le producteur a change "
            "de format d'enveloppe."
        )

    cols = [F.col(f"_j.{payload}.*")]
    if ingested in raw.columns:
        cols.append(F.col(ingested))
    out = parsed.select(*cols).withColumn("source_file", F.lit(spec.bronze))
    log.info("%s : %d colonne(s) apres parsing du payload.",
             spec.name, len(out.columns))
    return out


def read_json_arrays(spark: SparkSession, spec: SourceSpec,
                     base: str) -> DataFrame | None:
    """Documents a tableaux paralleles. Renvoie le document non explose.

    L'explosion est faite par silver_weather, qui a besoin du bloc
    hourly_units pour verifier les unites avant conversion.
    """
    try:
        raw = (spark.read.option("basePath", base)
               .json(f"{base}/{spec.glob}"))
    except Exception as exc:  # noqa: BLE001
        log.warning("Aucun lot lisible pour %s : %s", spec.name, exc)
        return None

    if not raw.columns or raw.rdd.isEmpty():
        log.warning("%s : aucune donnee.", spec.name)
        return None
    return raw.withColumn("source_file", F.input_file_name())


READERS = {
    "csv": read_csv,
    "json_envelope": read_json_envelope,
    "json_arrays": read_json_arrays,
}


def read_source(spark: SparkSession, mapping: SilverMapping, name: str,
                fs: str, root: str) -> DataFrame | None:
    """Lit une source Bronze declaree dans le mapping.

    Le controle des champs structurants est fait ICI, au plus tot : mieux
    vaut echouer sur "date_heure absent" que produire une table Silver vide
    et le decouvrir en Gold.
    """
    spec = mapping.source(name)
    reader = READERS.get(spec.fmt)
    if reader is None:
        raise ValidationError(
            f"Format Bronze inconnu pour {name} : {spec.fmt!r}. "
            f"Connus : {', '.join(sorted(READERS))}"
        )

    base = _path(fs, root, spec)
    log.info("Lecture %s (%s) depuis %s/%s", name, spec.fmt, base, spec.glob)
    df = reader(spark, spec, base)
    if df is None:
        return None

    check_required(df.columns, spec.required_fields, mapping, name)
    return df
