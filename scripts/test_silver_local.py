#!/usr/bin/env python3
"""
Test local de Silver sans HDFS ni cluster.

Fabrique un Bronze factice (CSV consolide + JSON streaming) dans un
repertoire temporaire, lance la logique Silver dessus et verifie :
  - la conversion UTC, y compris la nuit du changement d'heure
  - la deduplication avec priorite au consolide
  - le depivotement des filieres
  - la tolerance a un champ manquant

    python scripts/test_silver_local.py
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from silver.silver_grid import (  # noqa: E402
    build_grid_generation, build_grid_load, dedupe, normalize_time,
)

OK, KO = "OK   ", "ECHEC"


def check(label: str, cond: bool) -> bool:
    print(f"  {OK if cond else KO} {label}")
    return cond


def make_csv(path: Path) -> None:
    """CSV consolide, avec la nuit du 27/10/2024 (passage a l'heure d'hiver :
    02:00 locale existe deux fois, en +02:00 puis en +01:00)."""
    rows = ["date_heure;consommation;prevision_j1;prevision_j;taux_co2;"
            "nucleaire;eolien;solaire;hydraulique;gaz;fioul;charbon;bioenergies"]

    # Journee normale
    t = datetime(2024, 3, 15, 0, 0)
    for i in range(96):  # 96 quarts d'heure
        ts = (t + timedelta(minutes=15 * i)).strftime("%Y-%m-%dT%H:%M:%S+01:00")
        rows.append(f"{ts};{50000 + i * 10};{50500};{50200};{45};"
                    f"{35000};{5000};{2000};{6000};{3000};{100};{200};{800}")

    # Nuit du changement d'heure : 02:00 et 02:30 en double offset
    for off, conso in (("+02:00", 41000), ("+01:00", 42000)):
        for mn in ("00", "15", "30", "45"):
            rows.append(f"2024-10-27T02:{mn}:00{off};{conso};{41500};{41200};"
                        f"{38};{33000};{6000};{0};{5000};{1500};{50};{100};{700}")

    path.write_text("\n".join(rows), encoding="utf-8")


def make_stream_json(path: Path) -> None:
    """JSON streaming : chevauche volontairement le CSV sur 2024-03-15 pour
    tester la priorite du consolide."""
    import json
    lines = []
    t = datetime(2024, 3, 15, 0, 0)
    for i in range(8):
        ts = (t + timedelta(minutes=15 * i)).strftime("%Y-%m-%dT%H:%M:%S+01:00")
        payload = {
            "date_heure": ts,
            "consommation": 99999,          # valeur bidon : doit etre ecrasee
            "prevision_j1": 50500, "prevision_j": 50200, "taux_co2": 45,
            "nucleaire": 35000, "eolien": 5000, "solaire": 2000,
            "hydraulique": 6000, "gaz": 3000, "fioul": 100,
            "charbon": 200, "bioenergies": 800,
        }
        lines.append(json.dumps({
            "raw_value": json.dumps({"_meta": {"source": "eco2mix_tr"},
                                     "payload": payload}),
            "bronze_ingested_at": "2024-03-15T01:00:00.000Z",
        }))
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="silver_test_"))
    print(f"Bronze factice dans {tmp}\n")

    (tmp / "batch").mkdir()
    (tmp / "stream").mkdir()
    make_csv(tmp / "batch" / "eco2mix.csv")
    make_stream_json(tmp / "stream" / "part-0.json")

    spark = (SparkSession.builder.appName("test-silver")
             .master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    results = []

    # --- Source batch -----------------------------------------------------
    print("[1] Lecture CSV consolide + conversion UTC")
    batch = (spark.read.option("header", True).option("sep", ";")
             .option("inferSchema", True).csv(str(tmp / "batch")))
    batch = (batch.withColumn("_quality", F.lit("consolidated"))
                  .withColumn("_source", F.lit("eco2mix_cons")))
    nb = normalize_time(batch)
    results.append(check(f"{nb.count()} lignes normalisees", nb.count() == 104))

    r = nb.filter(F.col("ts_local").cast("string").startswith("2024-03-15 00:00")) \
          .select("ts_utc").first()
    results.append(check(f"00:00 locale -> {r.ts_utc} UTC (attendu 23:00 la veille)",
                         str(r.ts_utc).endswith("23:00:00")))

    # --- Changement d'heure ----------------------------------------------
    print("\n[2] Nuit du changement d'heure (27/10/2024)")
    dst = (nb.filter(F.col("ts_local").cast("string").startswith("2024-10-27 02:"))
             .select("ts_utc", "ts_local", "consommation")
             .orderBy("ts_utc"))
    n_dst = dst.count()
    results.append(check(f"{n_dst} lignes sur 02:xx locale (attendu 8, pas 4)",
                         n_dst == 8))
    distinct_utc = dst.select("ts_utc").distinct().count()
    results.append(check(f"{distinct_utc} horodatages UTC distincts (aucun ecrase)",
                         distinct_utc == 8))

    # --- Dedup ------------------------------------------------------------
    print("\n[3] Deduplication : le consolide doit ecraser le temps reel")
    stream = spark.read.json(str(tmp / "stream"))
    inferred = spark.read.json(
        stream.select("raw_value").rdd.map(lambda x: x.raw_value)).schema
    st = (stream.withColumn("j", F.from_json("raw_value", inferred))
                .select("j.payload.*", "bronze_ingested_at")
                .withColumn("_quality", F.lit("realtime"))
                .withColumn("_source", F.lit("eco2mix_tr")))
    ns = normalize_time(st)

    load_all = build_grid_load(nb).unionByName(build_grid_load(ns))
    before = load_all.count()
    load = dedupe(load_all, ["ts_utc", "zone_id"])
    after = load.count()
    results.append(check(f"{before} lignes -> {after} apres dedup", after < before))

    bad = load.filter(F.col("consumption_mw") == 99999).count()
    results.append(check("aucune valeur temps reel n'a survecu au consolide",
                         bad == 0))

    dupes = (load.groupBy("ts_utc").count().filter(F.col("count") > 1).count())
    results.append(check("aucun doublon sur (ts_utc, zone_id)", dupes == 0))

    # --- Depivotement -----------------------------------------------------
    print("\n[4] Depivotement des filieres")
    gen = build_grid_generation(nb)
    filieres = sorted(r.filiere for r in gen.select("filiere").distinct().collect())
    results.append(check(f"{len(filieres)} filieres : {', '.join(filieres)}",
                         len(filieres) == 8))
    renew = gen.filter(F.col("is_renewable")).select("filiere").distinct().count()
    results.append(check(f"{renew} filieres renouvelables marquees", renew == 4))
    results.append(check(f"{gen.count()} lignes depivotees (104 x 8)",
                         gen.count() == 104 * 8))

    # --- Tolerance --------------------------------------------------------
    print("\n[5] Tolerance a un champ manquant")
    amputee = nb.drop("taux_co2")
    try:
        out = build_grid_load(amputee)
        nulls = out.filter(F.col("co2_rate_g_kwh").isNull()).count()
        results.append(check(f"champ absent -> colonne nulle ({nulls} lignes), "
                             "pas de crash", nulls == out.count()))
    except Exception as exc:  # noqa: BLE001
        results.append(check(f"crash sur champ manquant : {exc}", False))

    # --- Ecriture ---------------------------------------------------------
    print("\n[6] Ecriture Parquet partitionnee")
    out_dir = tmp / "silver" / "grid_load"
    final = (load.withColumn("year", F.year("ts_utc"))
                 .withColumn("month", F.month("ts_utc")))
    final.write.mode("overwrite").partitionBy("year", "month").parquet(str(out_dir))
    parts = sorted(p.name for p in out_dir.glob("year=*/month=*"))
    results.append(check(f"partitions creees : {parts}", len(parts) >= 2))

    reread = spark.read.parquet(str(out_dir))
    results.append(check(f"relecture : {reread.count()} lignes",
                         reread.count() == after))

    print("\n" + "-" * 60)
    print("Apercu de silver/grid_load :")
    reread.select("ts_utc", "ts_local", "consumption_mw", "source", "quality") \
          .orderBy("ts_utc").show(5, truncate=False)

    spark.stop()
    shutil.rmtree(tmp, ignore_errors=True)

    print("-" * 60)
    if all(results):
        print(f"Tous les controles passent ({len(results)}/{len(results)}).")
        return 0
    print(f"{sum(results)}/{len(results)} controles passent.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
