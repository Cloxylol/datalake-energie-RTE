"""
Chargement de la configuration et construction des chemins du datalake.

Un seul endroit ou l'on decide de la forme des chemins : si la convention de
partitionnement change, elle change ici et nulle part ailleurs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONF = Path(
    os.environ.get("DATALAKE_CONF", "/opt/datalake/conf/sources.yml")
)


@lru_cache(maxsize=1)
def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Charge sources.yml. Mis en cache : appelable librement partout."""
    conf_path = Path(path) if path else DEFAULT_CONF
    if not conf_path.exists():
        raise FileNotFoundError(
            f"Configuration introuvable : {conf_path}. "
            "Definir DATALAKE_CONF ou passer le chemin explicitement."
        )
    with conf_path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def source_conf(name: str, path: str | Path | None = None) -> dict[str, Any]:
    """Retourne le bloc de configuration d'une source, avec erreur explicite."""
    cfg = load_config(path)
    try:
        return cfg["sources"][name]
    except KeyError as exc:
        known = ", ".join(sorted(cfg.get("sources", {})))
        raise KeyError(
            f"Source inconnue : {name!r}. Sources declarees : {known}"
        ) from exc


def zones(path: str | Path | None = None) -> list[dict[str, Any]]:
    return load_config(path)["zones"]


def weather_zones(path: str | Path | None = None) -> list[dict[str, Any]]:
    """Zones pour lesquelles on interroge reellement Open-Meteo."""
    return [z for z in zones(path) if z.get("weather_proxy")]


# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Layout:
    """Construit les chemins HDFS des trois couches.

    Deux conventions de partitionnement coexistent, volontairement :

    - batch   : partitionne sur la DATE METIER (year/month des donnees).
                C'est ce qui rend le rejeu idempotent : relancer le mois
                2024-03 ecrase exactement la meme partition.
    - stream  : partitionne sur la DATE D'INGESTION (ingest_date/ingest_hour).
                Au moment de l'ecriture on ne connait pas la date metier de
                chaque message sans payer un shuffle. Silver rebascule tout
                sur la date metier.
    """

    root: str = "/datalake"

    # -- Bronze ------------------------------------------------------------
    def bronze_batch(self, source: str, year: int, month: int, **extra: str) -> str:
        """/datalake/bronze/<source>/[cle=val/...]/year=YYYY/month=MM"""
        parts = [self.root, "bronze", source]
        parts += [f"{k}={v}" for k, v in extra.items()]
        parts += [f"year={year:04d}", f"month={month:02d}"]
        return "/".join(parts)

    def bronze_stream(self, source: str) -> str:
        """Racine du sink streaming. Spark ajoute ingest_date/ingest_hour."""
        return f"{self.root}/bronze/{source}"

    def success_marker(self, partition_path: str) -> str:
        return f"{partition_path}/_SUCCESS"

    # -- Silver / Gold -----------------------------------------------------
    def silver(self, table: str) -> str:
        return f"{self.root}/silver/{table}"

    def gold(self, table: str) -> str:
        return f"{self.root}/gold/{table}"

    def rejects(self, table: str) -> str:
        """Lignes invalides : jamais perdues silencieusement."""
        return f"{self.root}/silver/_rejects/{table}"

    # -- Technique ---------------------------------------------------------
    def checkpoint(self, name: str) -> str:
        """Hors de bronze/ : un rm -r de Bronze ne doit pas tuer les offsets."""
        return f"{self.root}/_checkpoints/{name}"


@lru_cache(maxsize=1)
def layout(path: str | Path | None = None) -> Layout:
    return Layout(root=load_config(path)["hdfs"]["root"])


def hdfs_client(path: str | Path | None = None):
    """Client WebHDFS. Import local pour ne pas imposer la dep aux jobs Spark."""
    from hdfs import InsecureClient

    conf = load_config(path)["hdfs"]
    return InsecureClient(conf["webhdfs_url"], user=conf["user"])


def require_env(var: str) -> str:
    val = os.environ.get(var)
    if not val:
        raise RuntimeError(
            f"Variable d'environnement {var} absente. "
            "La renseigner dans .env (cf. README)."
        )
    return val
