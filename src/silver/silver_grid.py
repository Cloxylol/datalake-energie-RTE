#!/usr/bin/env python3
"""
Silver : Bronze eco2mix (CSV batch + JSON streaming) -> grid_load + grid_generation.

Quatre regles font tout le travail, et toutes sont pilotees par
conf/silver_mapping.yml plutot que codees en dur ici :

1. VALIDATION DE SCHEMA. Champs structurants controles avant lecture,
   typage explicite, bornes metier. Une ligne fautive part en quarantaine
   dans /datalake/silver/_rejects/, jamais a la poubelle.

2. NORMALISATION TEMPORELLE. date_heure porte un offset ISO ; on le lit et
   on produit ts_utc, plus ts_local pour les features calendaires du ML.

3. DEDUPLICATION AVEC PRIORITE DE QUALITE. Le meme quart d'heure est publie
   en temps reel puis remplace par du consolide puis par du definitif. La
   qualite est lue dans le champ `nature`, pas deduite du chemin Bronze.
   Sur grid_load la fusion se fait mesure par mesure : les deux flux sont
   complementaires, le consolide ne publie les mesures qu'au pas de 30 min
   la ou le temps reel les a au quart d'heure.

4. DEPIVOTEMENT DES FILIERES. Une colonne par filiere -> une ligne par
   filiere. Le schema absorbe une nouvelle filiere sans changer. Seuls les
   aggregats sont emis : eolien vaut deja eolien_terrestre +
   eolien_offshore, emettre les trois ferait double compte en Gold.

    spark-submit silver_grid.py --start 2024-03-01 --end 2024-03-31
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/datalake/src")

from common.config import Layout, load_config  # noqa: E402
from silver.mapping import SilverMapping, load_mapping  # noqa: E402
from silver.readers import read_source  # noqa: E402
from silver.transform import (  # noqa: E402
    add_partitions, dedupe, normalize_time, derive_quality, restrict_window,
    unpivot_filieres, write_silver,
)
from silver.validation import (  # noqa: E402
    SchemaValidator, ValidationError, ValidationReport, write_rejects,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s [silver-grid] %(message)s")
log = logging.getLogger(__name__)

GRID_SOURCES = ("eco2mix_cons", "eco2mix_tr")


@dataclass
class SilverResult:
    """Ce que produit une etape Silver : la table, sa quarantaine, son bilan."""

    df: DataFrame
    rejects: DataFrame
    report: ValidationReport


# ---------------------------------------------------------------------------
# Preparation commune aux deux tables
# ---------------------------------------------------------------------------

def prepare(df: DataFrame, mapping: SilverMapping, source: str,
            zone_id: str = "fr") -> DataFrame:
    """Horodatage UTC, qualite, colonnes de tracabilite.

    ingested_at reprend l'instant d'ingestion Bronze quand il existe
    (colonne technique posee par le job streaming) : c'est lui qui
    departage deux lignes de meme qualite, et il doit rester stable d'un
    rejeu a l'autre.
    """
    spec = mapping.source(source)
    out = normalize_time(df, mapping)
    out = derive_quality(out, mapping, spec.default_quality)

    ingested = (F.col("bronze_ingested_at").cast("timestamp")
                if "bronze_ingested_at" in df.columns
                else F.lit(None).cast("timestamp"))

    return (out
            .withColumn("zone_id", F.lit(zone_id))
            .withColumn("source", F.lit(source))
            .withColumn("ingested_at",
                        F.coalesce(ingested, F.current_timestamp()))
            .withColumn("source_file",
                        F.col("source_file") if "source_file" in df.columns
                        else F.lit("")))


# ---------------------------------------------------------------------------
# grid_load
# ---------------------------------------------------------------------------

def build_grid_load(df: DataFrame, mapping: SilverMapping | None = None,
                    source: str = "eco2mix_cons") -> SilverResult:
    """Table de consommation : mesures typees, bornees, tracees."""
    mapping = mapping or load_mapping()
    spec = mapping.table("grid_load")
    validator = SchemaValidator(mapping, "grid_load", source)

    payload_columns = df.columns
    out = validator.cast_measures(df)
    out = validator.cast_dimensions(out)

    # L'ORDRE COMPTE. La detection passe avant la neutralisation : une
    # mesure mise a null parce qu'elle est hors bornes ressemble trait pour
    # trait a un cast qui a echoue, et serait comptee comme telle. On
    # qualifie d'abord, on neutralise ensuite, sur les seules lignes gardees.
    valid, rejects = validator.split(out, payload_columns)
    valid = validator.null_out_of_range(valid)
    valid = validator.drop_empty_rows(valid)

    columns = [c for c in mapping.lineage_columns if c in valid.columns]
    valid = valid.select(*columns, *spec.measures.keys())

    validator.enforce_ratio()
    return SilverResult(valid, rejects, validator.report)


# ---------------------------------------------------------------------------
# grid_generation
# ---------------------------------------------------------------------------

def build_grid_generation(df: DataFrame, mapping: SilverMapping | None = None,
                          source: str = "eco2mix_cons") -> SilverResult:
    """Depivotement des filieres, puis validation de la table longue.

    L'ordre compte : les bornes de production s'appliquent a la valeur
    depivotee, pas a la colonne large. Une seule filiere aberrante ne doit
    pas emporter les onze autres du meme horodatage.
    """
    mapping = mapping or load_mapping()
    spec = mapping.table("grid_generation")

    long_df, present, missing = unpivot_filieres(df, mapping)

    value_measure = spec.value_measure()
    validator = SchemaValidator(mapping, "grid_generation", source,
                                measures={value_measure.target: value_measure})
    validator.report.present_filieres = present
    validator.report.missing_filieres = missing

    valid, rejects = validator.split(long_df, long_df.columns)
    validator.report.n_valid = valid.count()

    columns = [c for c in mapping.lineage_columns if c in valid.columns]
    valid = valid.select(*columns, "filiere", "generation_mw", "is_renewable",
                         "filiere_category", "filiere_level", "filiere_parent")

    validator.enforce_ratio()
    return SilverResult(valid, rejects, validator.report)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

def process_source(spark: SparkSession, mapping: SilverMapping, source: str,
                   fs: str, root: str, start: str, end: str
                   ) -> tuple[SilverResult | None, SilverResult | None]:
    """Lit une source Bronze et en tire les deux tables."""
    raw = read_source(spark, mapping, source, fs, root)
    if raw is None:
        return None, None

    prepared = prepare(raw, mapping, source)
    # keep_null : une ligne sans horodatage doit atteindre la quarantaine.
    # La filtrer ici la ferait disparaitre sans laisser de trace, ce qui est
    # exactement ce que la couche Silver est censee empecher.
    prepared = restrict_window(prepared, start, end, keep_null=True).cache()

    load = build_grid_load(prepared, mapping, source)
    load.report.log_summary()

    try:
        gen = build_grid_generation(prepared, mapping, source)
        gen.report.log_summary()
    except ValueError as exc:
        log.warning("Depivotement impossible sur %s : %s", source, exc)
        gen = None

    return load, gen


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--conf", default=None)
    ap.add_argument("--mapping", default=None,
                    help="chemin de silver_mapping.yml")
    ap.add_argument("--report", default=None,
                    help="ecrit le bilan de validation en JSON")
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    fs = cfg["hdfs"]["fs_uri"]
    mapping = load_mapping(args.mapping)
    log.info("Mapping Silver v%s : %d table(s), %d filiere(s) declaree(s).",
             mapping.version, len(mapping.tables), len(mapping.filieres))

    spark = (SparkSession.builder.appName("silver-grid")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    loads, gens, rejects, reports = [], [], [], []
    for source in GRID_SOURCES:
        try:
            load, gen = process_source(spark, mapping, source, fs, layout.root,
                                       args.start, args.end)
        except ValidationError as exc:
            log.error("Source %s invalide : %s", source, exc)
            spark.stop()
            return 1

        for result, bucket in ((load, loads), (gen, gens)):
            if result is not None:
                bucket.append(result.df)
                rejects.append(result.rejects)
                reports.append(result.report.as_dict())

    if not loads:
        log.error("Aucune donnee Bronze exploitable. Lancer l'ingestion d'abord.")
        spark.stop()
        return 1

    window = f"{args.start}_{args.end}"

    # -- grid_load : fusion mesure par mesure ------------------------------
    spec_load = mapping.table("grid_load")
    load_df = reduce(DataFrame.unionByName, loads)
    load_df = dedupe(load_df, list(spec_load.keys), spec_load.dedup_strategy,
                     list(spec_load.measures))
    n_load = write_silver(add_partitions(load_df, mapping),
                          f"{fs}{layout.silver('grid_load')}", "grid_load")

    # -- grid_generation ---------------------------------------------------
    n_gen = 0
    if gens:
        spec_gen = mapping.table("grid_generation")
        gen_df = reduce(DataFrame.unionByName, gens)
        gen_df = dedupe(gen_df, list(spec_gen.keys), spec_gen.dedup_strategy)
        n_gen = write_silver(add_partitions(gen_df, mapping),
                             f"{fs}{layout.silver('grid_generation')}",
                             "grid_generation")

    # -- Quarantaine -------------------------------------------------------
    if mapping.rejects.get("enabled", True) and rejects:
        all_rejects = reduce(DataFrame.unionByName, rejects)
        for table in ("grid_load", "grid_generation"):
            part = all_rejects.filter(F.col("table_name") == table)
            write_rejects(part, f"{fs}{layout.rejects(table)}", window,
                          mapping.rejects.get("format", "parquet"))

    summary = {"window": window, "grid_load": n_load,
               "grid_generation": n_gen, "validation": reports}
    log.info("Bilan : grid_load %d ligne(s), grid_generation %d ligne(s).",
             n_load, n_gen)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, ensure_ascii=False, indent=2)
        log.info("Rapport de validation ecrit dans %s", args.report)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
