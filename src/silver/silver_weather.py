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

DEUX CONTROLES QUE LE MAPPING REND POSSIBLES, et qui manquent a la plupart
des pipelines meteo :

  - LES UNITES. Open-Meteo publie le vent en km/h, pas en m/s. Une colonne
    nommee wind_speed_ms qui contient des km/h ment de 3,6x, et personne ne
    s'en apercoit avant d'entrainer un modele dessus. Le facteur de
    conversion est declare dans conf/silver_mapping.yml et l'unite reelle
    annoncee par l'API (bloc hourly_units) est comparee a l'unite attendue
    a chaque run.

  - LE FUSEAU. L'archive est demandee en UTC mais l'API repond
    timezone: "GMT". C'est utc_offset_seconds qui fait foi : un lot ingere
    par erreur en heure locale decalerait toute la jointure avec eco2mix.

Produit aussi la temperature nationale ponderee (zone fr), qui servira de
feature au modele : la consommation electrique francaise depend surtout de
la temperature moyenne ponderee par la population.

    spark-submit silver_weather.py --start 2024-03-01 --end 2024-03-31
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/datalake/src")

from common.config import Layout, load_config  # noqa: E402
from silver.mapping import SilverMapping, load_mapping  # noqa: E402
from silver.readers import read_source  # noqa: E402
from silver.transform import (  # noqa: E402
    add_partitions, dedupe, restrict_window, write_silver,
)
from silver.validation import (  # noqa: E402
    SchemaValidator, ValidationError, write_rejects,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s [silver-weather] %(message)s")
log = logging.getLogger(__name__)

SOURCE = "meteo_archive"
TABLE = "weather"


# ---------------------------------------------------------------------------
# Controles propres a la source
# ---------------------------------------------------------------------------

def check_timezone(df: DataFrame, mapping: SilverMapping) -> list[str]:
    """L'archive doit etre en UTC. On verifie l'offset, pas le libelle.

    Open-Meteo repond "GMT" quand on demande "UTC" : se fier au libelle
    ferait echouer un lot parfaitement valide, et se fier au nom du
    parametre envoye ne verifierait rien du tout.
    """
    rules = mapping.source(SOURCE).options.get("validation", {})
    expected = rules.get("expect_utc_offset_seconds")
    notes: list[str] = []

    if expected is None or "utc_offset_seconds" not in df.columns:
        return notes

    bad = (df.filter(F.col("utc_offset_seconds") != F.lit(int(expected)))
             .select("utc_offset_seconds").distinct().collect())
    if bad:
        offsets = ", ".join(str(r[0]) for r in bad)
        raise ValidationError(
            f"{SOURCE} : offsets {offsets} au lieu de {expected}. Le lot a ete "
            "ingere en heure locale : toute la jointure avec eco2mix serait "
            "decalee. Reingerer avec timezone=UTC."
        )
    return notes


def check_units(df: DataFrame, mapping: SilverMapping) -> list[str]:
    """Compare les unites annoncees par l'API a celles du mapping.

    Le bloc hourly_units voyage avec la donnee jusqu'en Bronze, puisque le
    job d'ingestion ecrit la reponse telle quelle. On peut donc verifier a
    chaque run que le facteur de conversion declare s'applique bien a
    l'unite qu'on croit convertir.
    """
    opts = mapping.source(SOURCE).options.get("arrays", {})
    container = opts.get("units_container", "hourly_units")
    notes: list[str] = []

    if container not in df.columns:
        return notes

    units = df.select(f"{container}.*").limit(1).collect()
    if not units:
        return notes
    seen = units[0].asDict()

    for target, measure in mapping.table(TABLE).measures.items():
        src = mapping.pick(seen, measure.candidates)
        if src is None or measure.source_unit is None:
            continue
        actual = seen.get(src)
        if actual and actual != measure.source_unit:
            note = (f"unite inattendue pour {target} : l'API annonce "
                    f"{actual!r}, le mapping attend {measure.source_unit!r} "
                    f"(facteur {measure.factor}). Verifier conf/silver_mapping.yml.")
            log.warning(note)
            notes.append(note)
        else:
            log.info("%s : %s -> %s", target, actual or "?", measure.unit)
    return notes


# ---------------------------------------------------------------------------
# Depivotement des tableaux paralleles
# ---------------------------------------------------------------------------

def explode_hourly(df: DataFrame, mapping: SilverMapping) -> DataFrame:
    """arrays_zip + explode : tableaux paralleles -> lignes."""
    opts = mapping.source(SOURCE).options.get("arrays", {})
    container = opts.get("container", "hourly")
    index = opts.get("index_field", "time")

    available = df.select(f"{container}.*").columns
    resolved, missing = mapping.resolve_measures(TABLE, available)
    if not resolved:
        raise ValidationError(
            f"Aucune variable connue dans {container} : {available}"
        )
    log.info("Variables horaires : %s", ", ".join(sorted(resolved.values())))
    if missing:
        log.warning("Variables absentes de ce lot, mises a null : %s",
                    ", ".join(missing))

    zipped = F.arrays_zip(
        F.col(f"{container}.{index}").alias(index),
        *[F.col(f"{container}.`{src}`").alias(src)
          for src in resolved.values()],
    )

    out = df.withColumn("_z", F.explode(zipped))
    # Open-Meteo horodate sans offset ("2024-03-01T00:00") : c'est le
    # parametre timezone=UTC de la requete qui donne le sens, et
    # check_timezone vient de le verifier.
    out = out.withColumn("ts_utc", F.to_timestamp(F.col(f"_z.{index}")))

    # Les colonnes gardent leur nom SOURCE (temperature_2m, wind_speed_10m).
    # Le passage au nom du modele commun, le cast et la conversion d'unite
    # sont le travail du validateur : les faire ici les dedoublerait, et
    # cast_measures ne retrouverait plus ses champs.
    for src in resolved.values():
        out = out.withColumn(src, F.col(f"_z.`{src}`"))

    return out.drop("_z", container,
                    opts.get("units_container", "hourly_units"))


def national_average(df: DataFrame, weights: dict[str, float],
                     measures: list[str]) -> DataFrame:
    """Temperature France = moyenne ponderee des villes.

    Ponderation RENORMALISEE sur les villes reellement presentes : sans
    cela, un lot manquant sur Paris (35 % du poids) tirerait la moyenne
    nationale vers le bas sans que rien ne le signale.
    """
    wmap = F.create_map(*[x for k, v in weights.items()
                          for x in (F.lit(k), F.lit(float(v)))])
    weighted = df.withColumn("_w", F.coalesce(wmap[F.col("zone_id")], F.lit(0.0)))

    aggs = [(F.sum(F.col(c) * F.col("_w"))
             / F.sum(F.when(F.col(c).isNotNull(), F.col("_w")))).alias(c)
            for c in measures]

    return (weighted.groupBy("ts_utc").agg(*aggs)
            .withColumn("zone_id", F.lit("fr"))
            .withColumn("source", F.lit(SOURCE))
            .withColumn("quality", F.lit("consolidated"))
            .withColumn("quality_rank", F.lit(1))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.lit("aggregat_pondere")))


def build_weather(df: DataFrame, mapping: SilverMapping):
    """Validation + typage + conversion d'unites, par ville."""
    spec = mapping.table(TABLE)
    validator = SchemaValidator(mapping, TABLE, SOURCE)
    src_spec = mapping.source(SOURCE)

    payload_columns = df.columns
    zone_col = src_spec.options.get("partition_column", "city")

    out = validator.cast_measures(df)
    out = (out
           .withColumn("zone_id", F.col(zone_col) if zone_col in df.columns
                       else F.lit(None).cast("string"))
           .withColumn("source", F.lit(SOURCE))
           .withColumn("quality", F.lit(src_spec.default_quality))
           .withColumn("quality_rank",
                       F.lit(mapping.quality_rank(src_spec.default_quality)))
           .withColumn("ingested_at", F.current_timestamp()))

    # Detection d'abord, neutralisation ensuite : cf. silver_grid.
    valid, rejects = validator.split(out, payload_columns)
    valid = validator.null_out_of_range(valid)
    valid = validator.drop_empty_rows(valid)

    columns = [c for c in mapping.lineage_columns
               if c in valid.columns and c != "ts_local"]
    valid = valid.select(*columns, *spec.measures.keys())

    validator.enforce_ratio()
    return valid, rejects, validator.report


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--conf", default=None)
    ap.add_argument("--mapping", default=None)
    ap.add_argument("--report", default=None)
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    fs = cfg["hdfs"]["fs_uri"]
    mapping = load_mapping(args.mapping)

    spark = (SparkSession.builder.appName("silver-weather")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = read_source(spark, mapping, SOURCE, fs, layout.root)
    if raw is None:
        log.error("Aucune donnee meteo en Bronze.")
        spark.stop()
        return 1

    notes = check_timezone(raw, mapping) + check_units(raw, mapping)

    exploded = explode_hourly(raw, mapping)
    per_city, rejects, report = build_weather(exploded, mapping)
    report.notes.extend(notes)
    report.log_summary()

    measures = list(mapping.table(TABLE).measures)
    national = (national_average(per_city, cfg["weather_weights"], measures)
                .select(*per_city.columns))

    spec = mapping.table(TABLE)
    out = per_city.unionByName(national)
    out = dedupe(out, list(spec.keys), spec.dedup_strategy)
    out = restrict_window(out, args.start, args.end)
    out = out.withColumn(
        "ts_local",
        F.from_utc_timestamp("ts_utc",
                             mapping.time.get("local_timezone", "Europe/Paris")))
    out = add_partitions(out, mapping)

    n = write_silver(out, f"{fs}{layout.silver(TABLE)}", TABLE)

    window = f"{args.start}_{args.end}"
    if mapping.rejects.get("enabled", True):
        write_rejects(rejects, f"{fs}{layout.rejects(TABLE)}", window,
                      mapping.rejects.get("format", "parquet"))

    if args.report:
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump({"window": window, "weather": n,
                       "validation": [report.as_dict()]},
                      fh, ensure_ascii=False, indent=2)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
