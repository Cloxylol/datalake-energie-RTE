#!/usr/bin/env python3
"""
Validation de schema de la couche Silver.

Le principe, et c'est ce qui distingue Silver d'un simple `spark.read` :
une ligne qui ne respecte pas le contrat n'est jamais supprimee en
silence. Elle part en quarantaine dans /datalake/silver/_rejects/<table>/
avec son motif, sa colonne fautive et sa charge utile complete. Une donnee
qui disparait sans trace est un bug qu'on ne voit qu'en soutenance.

Quatre familles de controles, dans cet ordre :

1. STRUCTURE. Les champs declares `required_fields` pour la source sont-ils
   la ? Sinon on echoue tout de suite : ce n'est pas une ligne fautive,
   c'est le mapping ou le lot Bronze qui est faux.

2. TYPAGE EXPLICITE. Chaque mesure est castee vers le type du modele
   commun. Un cast qui echoue rend null en Spark, silencieusement : on
   compare donc avant / apres pour detecter le cas. C'est exactement ce qui
   arrive a ech_comm_allemagne_belgique, publie en entier dans un flux et
   en chaine dans l'autre.

3. BORNES METIER. Hors bornes, deux traitements possibles selon le mapping :
   `reject` sort la ligne (une consommation de 200 GW invalide toute la
   ligne), `null_out` neutralise la seule mesure (une prevision aberrante
   ne doit pas faire perdre la consommation mesuree).

4. GARDE-FOU GLOBAL. Au-dela de `max_reject_ratio`, le job echoue au lieu
   de publier une table vide : un taux de rejet massif signale un mapping
   casse, pas des donnees sales.

L'ordre d'appel n'est pas negociable : cast_measures, puis split (qui
qualifie et sort les lignes fautives), puis null_out_of_range sur les
lignes gardees. Neutraliser avant de qualifier ferait passer toute mesure
hors bornes pour un cast en echec.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .mapping import Measure, SilverMapping

log = logging.getLogger(__name__)

REJECT_REASON = "_reject_reason"
REJECT_COLUMN = "_reject_column"

# Schema stable de la table de quarantaine : il ne doit pas dependre du
# schema de la source, sinon deux sources produisent deux tables de rejets
# incompatibles.
REJECT_COLUMNS = [
    "rejected_at", "table_name", "source", "quality",
    "reject_reason", "reject_column", "ts_utc", "zone_id",
    "source_file", "payload",
]


# ---------------------------------------------------------------------------
# Rapport
# ---------------------------------------------------------------------------

@dataclass
class ValidationReport:
    """Ce que le job doit pouvoir dire de son propre lot."""

    table: str
    source: str
    n_input: int = 0
    n_valid: int = 0
    n_rejected: int = 0
    n_dropped_empty: int = 0
    missing_measures: list[str] = field(default_factory=list)
    present_filieres: list[str] = field(default_factory=list)
    missing_filieres: list[str] = field(default_factory=list)
    reasons: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    @property
    def reject_ratio(self) -> float:
        return self.n_rejected / self.n_input if self.n_input else 0.0

    def log_summary(self) -> None:
        log.info(
            "[%s <- %s] %d ligne(s) : %d valide(s), %d rejetee(s) (%.2f %%), "
            "%d vide(s) ignoree(s).",
            self.table, self.source, self.n_input, self.n_valid,
            self.n_rejected, 100 * self.reject_ratio, self.n_dropped_empty,
        )
        for reason, count in sorted(self.reasons.items(),
                                    key=lambda kv: -kv[1]):
            log.warning("    rejet %-28s %d", reason, count)
        if self.missing_measures:
            log.warning("    mesures absentes, mises a null : %s",
                        ", ".join(self.missing_measures))
        if self.missing_filieres:
            log.info("    filieres declarees mais absentes de cette source : %s",
                     ", ".join(self.missing_filieres))
        for note in self.notes:
            log.warning("    %s", note)

    def as_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "source": self.source,
            "n_input": self.n_input,
            "n_valid": self.n_valid,
            "n_rejected": self.n_rejected,
            "n_dropped_empty": self.n_dropped_empty,
            "reject_ratio": round(self.reject_ratio, 6),
            "reasons": self.reasons,
            "missing_measures": self.missing_measures,
            "present_filieres": self.present_filieres,
            "missing_filieres": self.missing_filieres,
            "notes": self.notes,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), ensure_ascii=False, indent=2)


class ValidationError(RuntimeError):
    """Le lot est structurellement invalide : on echoue avant d'ecrire."""


# ---------------------------------------------------------------------------
# Briques de validation
# ---------------------------------------------------------------------------

def check_required(columns: list[str], required: tuple[str, ...],
                   mapping: SilverMapping, source: str) -> None:
    """Controle structurel : echec immediat, avant toute lecture massive."""
    missing = [f for f in required if mapping.pick(columns, [f]) is None]
    if missing:
        raise ValidationError(
            f"Source {source} : champ(s) structurant(s) absent(s) : "
            f"{', '.join(missing)}. Colonnes vues : {', '.join(columns[:25])}"
        )


def cast_measure(df: DataFrame, measure: Measure, src_col: str) -> DataFrame:
    """Cast explicite + facteur d'unite, sous le nom du modele commun.

    Le facteur est declare dans le mapping (Open-Meteo publie des km/h la ou
    le modele commun veut des m/s). Le convertir ici, une fois, evite qu'une
    colonne nommee wind_speed_ms contienne autre chose que des m/s.
    """
    casted = F.col(f"`{src_col}`").cast(measure.dtype)
    if measure.factor is not None:
        casted = casted * F.lit(float(measure.factor))
    return df.withColumn(measure.target, casted)


def _reason(cond: Column, reason: str, column: str) -> tuple[Column, Column]:
    return cond, F.struct(F.lit(reason).alias("reason"),
                          F.lit(column).alias("column"))


class SchemaValidator:
    """Applique le contrat d'une table du mapping a un DataFrame source."""

    def __init__(self, mapping: SilverMapping, table: str, source: str,
                 measures: dict[str, Measure] | None = None):
        self.mapping = mapping
        self.spec = mapping.table(table)
        self.table = table
        self.source = source
        # Une table depivotee n'a pas de mesures issues de champs source :
        # sa colonne de valeur nait du stack(). On injecte alors la mesure
        # synthetique du bloc `unpivot` pour garder un seul chemin de code.
        self.measures = measures if measures is not None else self.spec.measures
        self.report = ValidationReport(table=table, source=source)

    # -- Typage ------------------------------------------------------------

    def cast_measures(self, df: DataFrame) -> DataFrame:
        """Materialise toutes les mesures du modele commun.

        Une mesure absente de la source devient une colonne nulle typee :
        c'est ce qui permet a unionByName de reunir deux sources qui n'ont
        pas le meme nombre de colonnes (41 en temps reel, 37 en consolide).
        """
        resolved, missing = self.mapping.resolve_measures(self.table, df.columns)
        self.report.missing_measures = missing

        out = df
        for target, measure in self.measures.items():
            src = resolved.get(target)
            if src is None:
                out = out.withColumn(target,
                                     F.lit(None).cast(measure.dtype))
                continue
            # Sauvegarde de la valeur brute : sans elle, impossible de
            # distinguer "null a la source" de "cast qui a echoue".
            out = out.withColumn(f"_raw_{target}",
                                 F.col(f"`{src}`").cast("string"))
            out = cast_measure(out, measure, src)
        return out

    def cast_dimensions(self, df: DataFrame) -> DataFrame:
        resolved = self.mapping.resolve_dimensions(self.table, df.columns)
        out = df
        for target, dim in self.spec.dimensions.items():
            src = resolved.get(target)
            out = (out.withColumn(target, F.col(f"`{src}`").cast(dim.dtype))
                   if src else
                   out.withColumn(target, F.lit(None).cast(dim.dtype)))
        return out

    # -- Controles ---------------------------------------------------------

    def _rules(self, df: DataFrame) -> list[tuple[Column, Column]]:
        """Chaine de controles, dans l'ordre de priorite des motifs."""
        rules: list[tuple[Column, Column]] = []

        # 1. Cles nulles : la ligne n'a aucune cle de jointure.
        for col in self.spec.not_null:
            if col in df.columns:
                rules.append(_reason(F.col(col).isNull(), "null_key", col))

        # 2. Cast en echec : valeur presente a la source, nulle apres cast.
        for target in self.measures:
            raw = f"_raw_{target}"
            if raw in df.columns:
                rules.append(_reason(
                    F.col(raw).isNotNull() & (F.trim(F.col(raw)) != F.lit(""))
                    & F.col(target).isNull(),
                    "cast_failed", target))

        # 3. Dimensions hors valeurs attendues.
        for target, dim in self.spec.dimensions.items():
            if dim.expect_values and dim.on_violation == "reject":
                rules.append(_reason(
                    F.col(target).isNotNull()
                    & ~F.col(target).isin(list(dim.expect_values)),
                    "unexpected_value", target))

        # 4. Bornes metier marquees `reject`.
        for target, measure in self.measures.items():
            if (measure.has_range() and measure.on_range_violation == "reject"
                    and target in df.columns):
                rules.append(_reason(self._out_of_range(target, measure),
                                     "out_of_range", target))
        return rules

    @staticmethod
    def _out_of_range(col: str, measure: Measure) -> Column:
        cond = F.lit(False)
        if measure.minimum is not None:
            cond = cond | (F.col(col) < F.lit(float(measure.minimum)))
        if measure.maximum is not None:
            cond = cond | (F.col(col) > F.lit(float(measure.maximum)))
        return F.col(col).isNotNull() & cond

    def null_out_of_range(self, df: DataFrame) -> DataFrame:
        """Neutralise les mesures hors bornes marquees `null_out`.

        Une prevision aberrante ne doit pas faire perdre la consommation
        mesuree de la meme ligne : on annule la mesure, pas la ligne.
        """
        out = df
        for target, measure in self.measures.items():
            if (measure.has_range() and measure.on_range_violation == "null_out"
                    and target in df.columns):
                out = out.withColumn(
                    target,
                    F.when(self._out_of_range(target, measure),
                           F.lit(None).cast(measure.dtype))
                     .otherwise(F.col(target)))
        return out

    def flag(self, df: DataFrame) -> DataFrame:
        """Ajoute _reject_reason / _reject_column (null si la ligne est saine)."""
        rules = self._rules(df)
        if not rules:
            return (df.withColumn(REJECT_REASON, F.lit(None).cast("string"))
                      .withColumn(REJECT_COLUMN, F.lit(None).cast("string")))

        expr = F.when(*rules[0])
        for cond, value in rules[1:]:
            expr = expr.when(cond, value)

        return (df.withColumn("_reject", expr)
                  .withColumn(REJECT_REASON, F.col("_reject.reason"))
                  .withColumn(REJECT_COLUMN, F.col("_reject.column"))
                  .drop("_reject"))

    # -- Orchestration -----------------------------------------------------

    def split(self, df: DataFrame, payload_columns: list[str] | None = None
              ) -> tuple[DataFrame, DataFrame]:
        """(lignes valides, lignes en quarantaine).

        Les deux DataFrames sont caches : ils sont comptes pour le rapport
        puis reutilises, et le recalcul d'une lecture CSV n'est pas gratuit.
        """
        flagged = self.flag(df).cache()

        valid = flagged.filter(F.col(REJECT_REASON).isNull())
        rejected = flagged.filter(F.col(REJECT_REASON).isNotNull())

        self.report.n_input = flagged.count()
        self.report.n_rejected = rejected.count()
        self.report.reasons = {
            f"{r[REJECT_REASON]}:{r[REJECT_COLUMN]}": r["n"]
            for r in (rejected.groupBy(REJECT_REASON, REJECT_COLUMN)
                      .agg(F.count("*").alias("n")).collect())
        }

        rejects = to_reject_table(rejected, self.table, self.source,
                                  payload_columns or df.columns)
        valid = valid.drop(REJECT_REASON, REJECT_COLUMN)
        valid = valid.drop(*[c for c in valid.columns if c.startswith("_raw_")])
        return valid, rejects

    def drop_empty_rows(self, df: DataFrame) -> DataFrame:
        """Retire les lignes sans aucune mesure.

        Ce ne sont PAS des rejets : le consolide eco2mix ne publie les
        mesures qu'au pas de 30 min alors que les previsions sont au quart
        d'heure. Une ligne sans mesure est le format normal, elle n'a
        simplement rien a apporter a la table.
        """
        cols = [c for c in self.spec.drop_if_all_null if c in df.columns]
        if not cols:
            return df

        keep = F.lit(False)
        for col in cols:
            keep = keep | F.col(col).isNotNull()

        before = df.count()
        out = df.filter(keep)
        after = out.count()
        self.report.n_dropped_empty = before - after
        self.report.n_valid = after
        return out

    def enforce_ratio(self, max_ratio: float | None = None) -> None:
        limit = (max_ratio if max_ratio is not None
                 else float(self.mapping.rejects.get("max_reject_ratio", 1.0)))
        if self.report.n_input and self.report.reject_ratio > limit:
            raise ValidationError(
                f"{self.table} <- {self.source} : {100 * self.report.reject_ratio:.1f} % "
                f"de rejets (limite {100 * limit:.0f} %). "
                f"Motifs : {self.report.reasons}. "
                "Un taux pareil signale un mapping casse, pas des donnees sales."
            )


# ---------------------------------------------------------------------------
# Quarantaine
# ---------------------------------------------------------------------------

def to_reject_table(rejected: DataFrame, table: str, source: str,
                    payload_columns: list[str]) -> DataFrame:
    """Schema stable + charge utile complete serialisee en JSON.

    Conserver la ligne d'origine entiere est ce qui rend la quarantaine
    exploitable : on peut rejouer, corriger le mapping, et comparer.
    """
    def opt(col: str, dtype: str) -> Column:
        return (F.col(col).cast(dtype) if col in rejected.columns
                else F.lit(None).cast(dtype))

    payload = F.to_json(F.struct(*[F.col(f"`{c}`").alias(c)
                                   for c in payload_columns]))

    return rejected.select(
        F.current_timestamp().alias("rejected_at"),
        F.lit(table).alias("table_name"),
        F.lit(source).alias("source"),
        opt("quality", "string").alias("quality"),
        F.col(REJECT_REASON).alias("reject_reason"),
        F.col(REJECT_COLUMN).alias("reject_column"),
        opt("ts_utc", "timestamp").alias("ts_utc"),
        opt("zone_id", "string").alias("zone_id"),
        opt("source_file", "string").alias("source_file"),
        payload.alias("payload"),
    )


def write_rejects(df: DataFrame, path: str, run_window: str,
                  fmt: str = "parquet") -> int:
    """Ecrit la quarantaine, partitionnee par fenetre de traitement.

    Partitionner par `run_window` plutot que par date de rejet rend
    l'ecriture idempotente : rejouer la meme fenetre reecrit exactement la
    meme partition, rejouer une autre fenetre ne l'ecrase pas. C'est la
    meme regle que pour les tables Silver, appliquee a la quarantaine.
    """
    missing = [c for c in REJECT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Table de quarantaine incomplete, colonnes manquantes : {missing}. "
            "Passer par to_reject_table() plutot que de construire le "
            "DataFrame a la main."
        )

    n = df.count()
    if n == 0:
        return 0
    (df.withColumn("run_window", F.lit(run_window))
       .repartition(1)
       .write.mode("overwrite")
       .partitionBy("run_window")
       .format(fmt)
       .save(path))
    log.warning("%d ligne(s) en quarantaine : %s/run_window=%s", n, path, run_window)
    return n
