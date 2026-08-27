"""
Chargement et resolution de conf/silver_mapping.yml.

Ce module ne connait pas Spark : il transforme le YAML en objets Python et
repond a une seule question, celle que se posent tous les jobs Silver :

    "cette colonne du modele commun, quel champ de CETTE source la porte ?"

La resolution est tolerante par construction. Les noms de champs eco2mix
evoluent (ajout de stockage_batterie, d'eolien_offshore, decoupage par
technologie), et les deux flux n'ont deja pas le meme nombre de colonnes :
41 en temps reel, 37 en consolide. Un champ absent est signale puis mis a
null, il ne fait pas tomber le job. Un champ STRUCTURANT absent (declare
dans `required_fields`) le fait tomber, lui, et immediatement.
"""

from __future__ import annotations

import logging
import os
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import yaml

log = logging.getLogger(__name__)

# Par defaut le mapping est voisin de sources.yml : une seule variable
# d'environnement (DATALAKE_CONF) suffit donc a deplacer toute la conf.
DEFAULT_MAPPING = Path(
    os.environ.get(
        "DATALAKE_SILVER_MAPPING",
        str(Path(os.environ.get("DATALAKE_CONF",
                                "/opt/datalake/conf/sources.yml")).parent
            / "silver_mapping.yml"),
    )
)


# ---------------------------------------------------------------------------
# Normalisation des noms
# ---------------------------------------------------------------------------

def normalize(name: str) -> str:
    """Forme canonique d'un nom de champ.

    Minuscule, sans accent, ponctuation reduite a '_'. C'est ce qui permet
    de reconnaitre "Date et heure", "date_heure" et "DATE-HEURE" comme le
    meme champ, y compris quand un export CSV bascule des noms techniques
    aux libelles.

    >>> normalize("Consommation (MW)")
    'consommation_mw'
    >>> normalize("Donnees definitives") == normalize("Données définitives")
    True
    """
    txt = unicodedata.normalize("NFKD", str(name))
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    out = [c.lower() if c.isalnum() else "_" for c in txt]
    return "_".join(part for part in "".join(out).split("_") if part)


# ---------------------------------------------------------------------------
# Objets du mapping
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Measure:
    """Une colonne du modele commun et la facon de l'obtenir."""

    target: str
    candidates: tuple[str, ...]
    dtype: str = "double"
    unit: str | None = None
    source_unit: str | None = None
    factor: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    on_range_violation: str = "null_out"   # reject | null_out | keep

    @classmethod
    def from_yaml(cls, target: str, spec: dict[str, Any]) -> "Measure":
        return cls(
            target=target,
            candidates=tuple(spec.get("from", [target])),
            dtype=spec.get("type", "double"),
            unit=spec.get("unit"),
            source_unit=spec.get("source_unit"),
            factor=spec.get("factor"),
            minimum=spec.get("min"),
            maximum=spec.get("max"),
            on_range_violation=spec.get("on_range_violation", "null_out"),
        )

    def has_range(self) -> bool:
        return self.minimum is not None or self.maximum is not None


@dataclass(frozen=True)
class Dimension:
    """Colonne non numerique soumise a une liste de valeurs attendues."""

    target: str
    candidates: tuple[str, ...]
    dtype: str = "string"
    expect_values: tuple[str, ...] = ()
    on_violation: str = "reject"

    @classmethod
    def from_yaml(cls, target: str, spec: dict[str, Any]) -> "Dimension":
        return cls(
            target=target,
            candidates=tuple(spec.get("from", [target])),
            dtype=spec.get("type", "string"),
            expect_values=tuple(spec.get("expect_values", ())),
            on_violation=spec.get("on_violation", "reject"),
        )


@dataclass(frozen=True)
class Filiere:
    """Une filiere de production, avec sa place dans la hierarchie.

    `level` vaut aggregate ou detail. eolien_terrestre et eolien_offshore
    sont des `detail` d'eolien : leurs valeurs sont deja comprises dans
    celle du parent. Les emettre toutes les trois dans la meme table ferait
    double compte des que Gold somme la production totale, d'ou le filtre
    par defaut sur les seuls aggregats.
    """

    name: str
    level: str = "aggregate"
    category: str = "production"
    renewable: bool = False
    parent: str | None = None
    note: str | None = None

    @classmethod
    def from_yaml(cls, spec: dict[str, Any]) -> "Filiere":
        return cls(
            name=spec["name"],
            level=spec.get("level", "aggregate"),
            category=spec.get("category", "production"),
            renewable=bool(spec.get("renewable", False)),
            parent=spec.get("parent"),
            note=spec.get("note"),
        )


@dataclass(frozen=True)
class TableSpec:
    """Une table du modele commun."""

    name: str
    keys: tuple[str, ...]
    dedup_strategy: str = "priority"       # priority | merge
    zone_id: str | None = None
    description: str = ""
    measures: dict[str, Measure] = field(default_factory=dict)
    dimensions: dict[str, Dimension] = field(default_factory=dict)
    not_null: tuple[str, ...] = ()
    drop_if_all_null: tuple[str, ...] = ()
    unpivot: dict[str, Any] = field(default_factory=dict)

    def value_measure(self) -> Measure | None:
        """Mesure synthetique de la colonne produite par le depivotement.

        La valeur d'une table depivotee ne vient d'aucun champ source : elle
        nait du stack(). Ses bornes sont declarees dans le bloc `unpivot`,
        on les expose sous la meme forme que les autres mesures pour que le
        validateur n'ait qu'un seul chemin de code.
        """
        if not self.unpivot:
            return None
        target = self.unpivot.get("value_column", "value")
        return Measure(
            target=target,
            candidates=(target,),
            dtype=self.unpivot.get("type", "double"),
            unit=self.unpivot.get("unit"),
            minimum=self.unpivot.get("min"),
            maximum=self.unpivot.get("max"),
            on_range_violation=self.unpivot.get("on_range_violation", "reject"),
        )

    @classmethod
    def from_yaml(cls, name: str, spec: dict[str, Any]) -> "TableSpec":
        validation = spec.get("validation", {}) or {}
        return cls(
            name=name,
            keys=tuple(spec.get("keys", ())),
            dedup_strategy=spec.get("dedup_strategy", "priority"),
            zone_id=spec.get("zone_id"),
            description=" ".join(spec.get("description", "").split()),
            measures={k: Measure.from_yaml(k, v)
                      for k, v in (spec.get("measures") or {}).items()},
            dimensions={k: Dimension.from_yaml(k, v)
                        for k, v in (spec.get("dimensions") or {}).items()},
            not_null=tuple(validation.get("not_null", ())),
            drop_if_all_null=tuple(validation.get("drop_if_all_null", ())),
            unpivot=spec.get("unpivot", {}) or {},
        )


@dataclass(frozen=True)
class SourceSpec:
    """Une source Bronze et la facon de la lire."""

    name: str
    bronze: str
    fmt: str
    glob: str
    default_quality: str = "unknown"
    feeds: tuple[str, ...] = ()
    required_fields: tuple[str, ...] = ()
    options: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, name: str, spec: dict[str, Any]) -> "SourceSpec":
        known = {"bronze", "format", "glob", "default_quality", "feeds",
                 "required_fields"}
        return cls(
            name=name,
            bronze=spec["bronze"],
            fmt=spec["format"],
            glob=spec.get("glob", "*"),
            default_quality=spec.get("default_quality", "unknown"),
            feeds=tuple(spec.get("feeds", ())),
            required_fields=tuple(spec.get("required_fields", ())),
            options={k: v for k, v in spec.items() if k not in known},
        )


# ---------------------------------------------------------------------------
# Le mapping complet
# ---------------------------------------------------------------------------

class SilverMapping:
    """Vue objet de conf/silver_mapping.yml."""

    def __init__(self, raw: dict[str, Any]):
        self.raw = raw
        self.version = raw.get("version", 0)
        self.tables = {n: TableSpec.from_yaml(n, s)
                       for n, s in raw["tables"].items()}
        self.sources = {n: SourceSpec.from_yaml(n, s)
                        for n, s in raw["sources"].items()}
        self.filieres = {f["name"]: Filiere.from_yaml(f)
                         for f in raw.get("filieres", [])}
        self.time = raw.get("time", {})
        self.quality = raw.get("quality", {})
        self.lineage_columns = tuple(raw.get("lineage_columns", ()))
        self.partitioning = raw.get("partitioning", {})
        self.rejects = raw.get("rejects", {})

        # Index normalises, construits une fois.
        self._quality_values = {normalize(k): v
                                for k, v in self.quality.get("values", {}).items()}
        self._quality_ranks = self.quality.get("ranks", {})

    # -- Acces -------------------------------------------------------------

    def table(self, name: str) -> TableSpec:
        try:
            return self.tables[name]
        except KeyError as exc:
            raise KeyError(
                f"Table Silver inconnue : {name!r}. "
                f"Declarees : {', '.join(sorted(self.tables))}"
            ) from exc

    def source(self, name: str) -> SourceSpec:
        try:
            return self.sources[name]
        except KeyError as exc:
            raise KeyError(
                f"Source inconnue dans le mapping : {name!r}. "
                f"Declarees : {', '.join(sorted(self.sources))}"
            ) from exc

    # -- Resolution des noms de champs -------------------------------------

    @staticmethod
    def pick(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
        """Premier candidat present, compare sous forme normalisee.

        Retourne le nom REEL de la colonne (casse d'origine comprise), pour
        pouvoir l'utiliser tel quel dans un select Spark.
        """
        index: dict[str, str] = {}
        for col in columns:
            index.setdefault(normalize(col), col)
        for cand in candidates:
            hit = index.get(normalize(cand))
            if hit is not None:
                return hit
        return None

    def resolve_measures(
        self, table: str, columns: Iterable[str]
    ) -> tuple[dict[str, str], list[str]]:
        """(mesures resolues, mesures absentes) pour ces colonnes source."""
        cols = list(columns)
        found: dict[str, str] = {}
        missing: list[str] = []
        for target, measure in self.table(table).measures.items():
            src = self.pick(cols, measure.candidates)
            if src is None:
                missing.append(target)
            else:
                found[target] = src
        return found, missing

    def resolve_dimensions(
        self, table: str, columns: Iterable[str]
    ) -> dict[str, str]:
        cols = list(columns)
        out: dict[str, str] = {}
        for target, dim in self.table(table).dimensions.items():
            src = self.pick(cols, dim.candidates)
            if src is not None:
                out[target] = src
        return out

    def time_column(self, columns: Iterable[str]) -> str:
        """Colonne temporelle de la source, ou erreur explicite."""
        cands = self.time.get("candidates", ["date_heure"])
        col = self.pick(columns, cands)
        if col is None:
            sample = ", ".join(list(columns)[:20])
            raise ValueError(
                f"Aucune colonne temporelle parmi {cands}. Colonnes vues : {sample}"
            )
        return col

    def resolve_filieres(
        self, columns: Iterable[str], table: str = "grid_generation"
    ) -> tuple[dict[str, str], list[str]]:
        """Filieres reellement presentes dans ces colonnes.

        Retourne ({filiere: colonne source}, [filieres declarees absentes]),
        deja filtre par les niveaux et categories retenus pour la table.
        """
        cols = list(columns)
        spec = self.table(table).unpivot
        levels = set(spec.get("include_levels", ["aggregate"]))
        cats = set(spec.get("include_categories",
                            ["production", "stockage"]))

        found: dict[str, str] = {}
        missing: list[str] = []
        for name, fil in self.filieres.items():
            if fil.level not in levels or fil.category not in cats:
                continue
            src = self.pick(cols, [name])
            if src is None:
                missing.append(name)
            else:
                found[name] = src
        return found, missing

    # -- Qualite -----------------------------------------------------------

    def quality_of(self, raw_value: Any, default: str = "unknown") -> str:
        """Niveau de qualite normalise a partir de la valeur du champ nature."""
        if raw_value is None:
            return default
        return self._quality_values.get(normalize(raw_value), default)

    def quality_rank(self, level: str) -> int:
        return int(self._quality_ranks.get(level,
                                           self._quality_ranks.get("unknown", 9)))

    def quality_field(self, columns: Iterable[str]) -> str | None:
        return self.pick(columns, self.quality.get("field_candidates", []))

    @property
    def quality_pairs(self) -> dict[str, str]:
        """{valeur source normalisee: niveau} - pour construire un map Spark."""
        return dict(self._quality_values)

    @property
    def rank_pairs(self) -> dict[str, int]:
        return {k: int(v) for k, v in self._quality_ranks.items()}


# ---------------------------------------------------------------------------
# Chargement
# ---------------------------------------------------------------------------

@lru_cache(maxsize=4)
def _load_raw(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Mapping Silver introuvable : {p}. "
            "Definir DATALAKE_SILVER_MAPPING, ou placer silver_mapping.yml "
            "a cote de sources.yml."
        )
    with p.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    for key in ("tables", "sources"):
        if key not in raw:
            raise ValueError(f"Mapping invalide : section '{key}' absente de {p}.")
    return raw


def load_mapping(path: str | Path | None = None) -> SilverMapping:
    """Charge le mapping. Mis en cache : appelable librement."""
    return SilverMapping(_load_raw(str(path or DEFAULT_MAPPING)))
