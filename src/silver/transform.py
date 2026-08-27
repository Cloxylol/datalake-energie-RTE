#!/usr/bin/env python3
"""
Transformations Silver communes, pilotees par conf/silver_mapping.yml.

Les quatre operations que la couche doit a l'ensemble du datalake :

  normalize_time     texte horodate heterogene -> ts_utc + ts_local
  derive_quality     champ `nature` de la source -> quality + quality_rank
  dedupe             une seule ligne par cle, la meilleure
  unpivot_filieres   une colonne par filiere -> une ligne par filiere

Aucun nom de champ source n'apparait ici : tout vient du mapping. C'est ce
qui permet d'absorber l'ajout d'une filiere par RTE en editant un YAML.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .mapping import SilverMapping

log = logging.getLogger(__name__)

# Equivalent Spark de mapping.normalize() : minuscule, sans accent,
# ponctuation reduite a '_'. Une UDF Python ferait la meme chose en plus
# lent et en cassant le codegen, pour une cardinalite de trois valeurs.
_ACCENTS = "áàâäãåéèêëíìîïóòôöõúùûüýÿñç"
_PLAIN = "aaaaaaeeeeiiiiooooouuuuyync"


def spark_normalize(col: Column) -> Column:
    """Forme canonique d'une valeur textuelle, cote Spark."""
    out = F.lower(col.cast("string"))
    out = F.translate(out, _ACCENTS, _PLAIN)
    out = F.regexp_replace(out, "[^a-z0-9]+", "_")
    return F.regexp_replace(out, "^_+|_+$", "")


# ---------------------------------------------------------------------------
# 1. Normalisation temporelle
# ---------------------------------------------------------------------------

def normalize_time(df: DataFrame, mapping: SilverMapping) -> DataFrame:
    """Produit ts_utc (UTC) et ts_local (Europe/Paris).

    PIEGE : si la colonne source est deja de type timestamp (inferSchema a
    fait son travail, ou le JSON etait type), Spark a DEJA ramene la valeur
    en UTC en lisant l'offset ISO. Reappliquer to_utc_timestamp decalerait
    tout d'une heure de plus. On teste donc le TYPE de la colonne, pas
    seulement son contenu.

    Trois cas :
      1. deja timestamp     -> c'est de l'UTC, ne rien faire
      2. texte avec offset  -> to_timestamp lit l'offset
      3. texte sans offset  -> interpreter dans time.fallback_timezone

    Le cas 3 est ambigu la nuit du passage a l'heure d'hiver : 02:00 locale
    existe deux fois, et sans offset l'information est perdue. Les exports
    eco2mix actuels portent bien un offset (+00:00), le cas 3 ne concerne
    donc que des archives ; on le journalise plutot que de le masquer.
    """
    tcol = mapping.time_column(df.columns)
    dtype = dict(df.dtypes)[tcol]
    local_tz = mapping.time.get("local_timezone", "Europe/Paris")
    fallback_tz = mapping.time.get("fallback_timezone", local_tz)

    log.info("Colonne temporelle : %s (type %s)", tcol, dtype)

    if dtype.startswith("timestamp"):
        ts_utc = F.col(f"`{tcol}`")
    else:
        as_str = F.col(f"`{tcol}`").cast("string")
        has_offset = as_str.rlike(r"([+-]\d{2}:?\d{2}|Z)$")
        ts_utc = (F.when(has_offset, F.to_timestamp(as_str))
                   .otherwise(F.to_utc_timestamp(F.to_timestamp(as_str),
                                                 fallback_tz)))
        log.info("Colonne texte : offset lu s'il est present, sinon %s.",
                 fallback_tz)

    out = df.withColumn("ts_utc", ts_utc)
    if mapping.time.get("emit_local", True):
        out = out.withColumn("ts_local",
                             F.from_utc_timestamp("ts_utc", local_tz))
    return out


def count_unparsed_time(df: DataFrame, mapping: SilverMapping) -> int:
    """Lignes dont l'horodatage n'a pas pu etre lu. Comptees, pas ignorees."""
    tcol = mapping.time_column(df.columns)
    return df.filter(F.col(f"`{tcol}`").isNotNull()
                     & F.col("ts_utc").isNull()).count()


# ---------------------------------------------------------------------------
# 2. Qualite
# ---------------------------------------------------------------------------

def derive_quality(df: DataFrame, mapping: SilverMapping,
                   default_quality: str) -> DataFrame:
    """quality + quality_rank, lus DANS la donnee quand c'est possible.

    eco2mix porte la nature de la mesure dans le champ `nature` : temps
    reel, consolidee, definitive. La lire vaut mieux que la deduire du
    chemin Bronze, parce qu'un lot mensuel peut etre a cheval sur la
    frontiere consolide / definitif. On retombe sur la qualite declaree
    pour la source quand le champ est absent ou inconnu.
    """
    field = mapping.quality_field(df.columns)
    pairs = mapping.quality_pairs

    if field and pairs:
        lookup = F.create_map(*[x for k, v in pairs.items()
                                for x in (F.lit(k), F.lit(v))])
        quality = F.coalesce(lookup[spark_normalize(F.col(f"`{field}`"))],
                             F.lit(default_quality))
        log.info("Qualite lue dans le champ '%s' (defaut : %s).",
                 field, default_quality)
    else:
        quality = F.lit(default_quality)
        log.info("Champ de qualite absent, qualite forcee a '%s'.",
                 default_quality)

    ranks = mapping.rank_pairs
    rank_map = F.create_map(*[x for k, v in ranks.items()
                              for x in (F.lit(k), F.lit(int(v)))])
    unknown = int(ranks.get("unknown", 9))

    return (df.withColumn("quality", quality)
              .withColumn("quality_rank",
                          F.coalesce(rank_map[F.col("quality")],
                                     F.lit(unknown))))


# ---------------------------------------------------------------------------
# 3. Deduplication
# ---------------------------------------------------------------------------

def _priority_window(keys: list[str]) -> Window:
    """Meilleure qualite d'abord, puis ingestion la plus recente.

    `source` en dernier critere : sans lui, deux lignes de meme qualite et
    de meme instant d'ingestion se departageraient au hasard, et le job ne
    serait pas reproductible.
    """
    return (Window.partitionBy(*keys)
            .orderBy(F.col("quality_rank").asc(),
                     F.col("ingested_at").desc_nulls_last(),
                     F.col("source").asc()))


def dedupe(df: DataFrame, keys: list[str], strategy: str = "priority",
           merge_columns: list[str] | None = None) -> DataFrame:
    """Une seule ligne par cle.

    strategy = "priority" : on garde la meilleure ligne. Suffisant quand la
    ligne est atomique (une filiere, une mesure meteo).

    strategy = "merge" : pour CHAQUE mesure on retient la valeur non nulle
    de meilleure qualite. C'est necessaire sur grid_load parce que les deux
    flux sont complementaires : le consolide ne publie la consommation
    qu'une ligne sur deux, la ou le temps reel l'a partout. Garder betement
    la ligne consolidee perdrait la mesure temps reel du meme quart d'heure.
    """
    w = _priority_window(keys)

    out = df
    if strategy == "merge" and merge_columns:
        wide = w.rowsBetween(Window.unboundedPreceding,
                             Window.unboundedFollowing)
        for col in merge_columns:
            if col in df.columns:
                out = out.withColumn(col,
                                     F.first(F.col(col), ignorenulls=True).over(wide))

    return (out.withColumn("_rn", F.row_number().over(w))
               .filter(F.col("_rn") == 1)
               .drop("_rn"))


# ---------------------------------------------------------------------------
# 4. Depivotement des filieres
# ---------------------------------------------------------------------------

def unpivot_filieres(df: DataFrame, mapping: SilverMapping,
                     table: str = "grid_generation"
                     ) -> tuple[DataFrame, list[str], list[str]]:
    """Une colonne par filiere -> une ligne par filiere.

    Le schema de sortie ne bouge plus quand RTE ajoute une filiere : c'est
    tout l'interet du depivotement, et c'est ce qui rend la table lisible
    par un Gold qui n'a pas a connaitre la liste.

    Chaque ligne porte sa place dans la hierarchie (`filiere_level`,
    `filiere_parent`) et sa nature (`filiere_category`). Par defaut seuls
    les aggregats sont emis : eolien vaut deja eolien_terrestre +
    eolien_offshore, emettre les trois ferait double compte des que Gold
    somme la production totale.

    Retourne (DataFrame long, filieres presentes, filieres declarees absentes).
    """
    spec = mapping.table(table).unpivot
    value_col = spec.get("value_column", "generation_mw")
    name_col = spec.get("name_column", "filiere")

    found, missing = mapping.resolve_filieres(df.columns, table)
    if not found:
        raise ValueError(
            "Aucune filiere trouvee dans cette source. "
            f"Colonnes vues : {', '.join(df.columns[:30])}"
        )

    names = sorted(found)
    log.info("%d filiere(s) depivotee(s) : %s", len(names), ", ".join(names))

    pairs = ", ".join(f"'{n}', CAST(`{found[n]}` AS DOUBLE)" for n in names)
    stack = f"stack({len(names)}, {pairs}) as ({name_col}, {value_col})"

    def attr(getter) -> Column:
        return F.create_map(*[x for n in names
                              for x in (F.lit(n), F.lit(getter(mapping.filieres[n])))
                              ])[F.col(name_col)]

    keep = [c for c in ("ts_utc", "ts_local", "zone_id", "source", "quality",
                        "quality_rank", "ingested_at", "source_file")
            if c in df.columns]

    out = (df.select(*keep, F.expr(stack))
             .withColumn("is_renewable", attr(lambda f: bool(f.renewable)))
             .withColumn("filiere_category", attr(lambda f: f.category))
             .withColumn("filiere_level", attr(lambda f: f.level))
             .withColumn("filiere_parent",
                         attr(lambda f: f.parent if f.parent else "")))

    if spec.get("drop_null_values", True):
        out = out.filter(F.col(value_col).isNotNull())

    return out, names, missing


# ---------------------------------------------------------------------------
# Ecriture
# ---------------------------------------------------------------------------

def add_partitions(df: DataFrame, mapping: SilverMapping) -> DataFrame:
    """Partitions sur la date METIER : c'est ce qui rend le rejeu idempotent."""
    part = mapping.partitioning
    src = part.get("derived_from", "ts_utc")
    cols = part.get("columns", ["year", "month"])

    out = df
    if "year" in cols:
        out = out.withColumn("year", F.year(src))
    if "month" in cols:
        out = out.withColumn("month", F.month(src))
    return out


def restrict_window(df: DataFrame, start: str, end: str,
                    col: str = "ts_utc", keep_null: bool = False) -> DataFrame:
    """Borne le lot a la fenetre du job, bornes de jour incluses.

    Chaque job ne traite QUE sa fenetre : c'est ce qui garantit qu'un rejeu
    n'ecrase que les partitions correspondantes.

    keep_null=True conserve les lignes sans horodatage. Ce n'est pas un
    detail : un filtre sur ts_utc elimine les nulls en silence, et ce sont
    precisement les lignes que la quarantaine doit recevoir. On les garde
    ici pour que le validateur puisse les motiver, il les sortira ensuite.
    """
    in_window = ((F.col(col) >= F.lit(start).cast("timestamp"))
                 & (F.col(col) < F.date_add(F.lit(end).cast("date"), 1)
                    .cast("timestamp")))
    return df.filter(in_window | F.col(col).isNull() if keep_null else in_window)


def write_silver(df: DataFrame, path: str, label: str,
                 partitions: list[str] | None = None) -> int:
    """Ecrasement DYNAMIQUE de partition.

    Sans dynamic, mode("overwrite") detruit toute la table : c'est le piege
    classique, et la raison pour laquelle la conf est posee ici et dans le
    DAG plutot que laissee au hasard de la session Spark.
    """
    parts = partitions or ["year", "month"]
    n = df.count()
    if n == 0:
        log.warning("%s : 0 ligne, ecriture ignoree.", label)
        return 0

    (df.repartition(*parts)
       .write.mode("overwrite")
       .partitionBy(*parts)
       .parquet(path))
    log.info("%s : %d ligne(s) ecrite(s) dans %s", label, n, path)
    return n
