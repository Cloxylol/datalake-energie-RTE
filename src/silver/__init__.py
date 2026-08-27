"""
Couche Silver : du Bronze brut au modele commun.

Quatre operations, toutes pilotees par conf/silver_mapping.yml :

    validation de schema     src/silver/validation.py
    normalisation UTC        src/silver/transform.py
    deduplication            src/silver/transform.py
    depivotement filieres    src/silver/transform.py

    lecture des lots Bronze  src/silver/readers.py
    resolution du mapping    src/silver/mapping.py

Deux jobs les assemblent : silver_grid.py (eco2mix -> grid_load +
grid_generation) et silver_weather.py (Open-Meteo -> weather).

Ce module de tete n'importe volontairement PAS pyspark : le mapping doit
rester chargeable depuis un script de developpement ou un notebook sans
session Spark.
"""

from .mapping import (
    Dimension, Filiere, Measure, SilverMapping, SourceSpec, TableSpec,
    load_mapping, normalize,
)

__all__ = [
    "Dimension", "Filiere", "Measure", "SilverMapping", "SourceSpec",
    "TableSpec", "load_mapping", "normalize",
]
