#!/usr/bin/env python3
"""
Test local de la couche Silver, sans HDFS ni cluster.

Fabrique un Bronze factice qui respecte exactement l'arborescence reelle
(year=/month=, ingest_date=/ingest_hour=, city=), lance les vrais lecteurs
et les vraies transformations dessus, et verifie ce que la couche Silver
doit garantir :

  [1] lecture Bronze pilotee par le mapping, tolerante aux schemas inegaux
  [2] normalisation UTC, y compris la nuit du changement d'heure
  [3] validation de schema : cast, bornes, valeurs attendues, cles nulles
  [4] quarantaine : aucune ligne fautive perdue en silence
  [5] deduplication par qualite, avec fusion mesure par mesure
  [6] depivotement des filieres, sans double compte aggregat / detail
  [7] meteo : tableaux paralleles, conversion d'unites, moyenne ponderee
  [8] ecriture partitionnee et idempotence du rejeu

Les valeurs de reference sont calculees a partir du jeu factice, pas
recopiees a la main : un changement du generateur ne fait pas passer un
test a cote de son sujet.

    python scripts/test_silver_local.py
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
os.environ.setdefault("DATALAKE_SILVER_MAPPING",
                      str(REPO / "conf" / "silver_mapping.yml"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from silver.mapping import load_mapping  # noqa: E402
from silver.readers import read_source  # noqa: E402
from silver.silver_grid import (  # noqa: E402
    build_grid_generation, build_grid_load, prepare,
)
from silver.silver_weather import (  # noqa: E402
    build_weather, check_timezone, check_units, explode_hourly,
    national_average,
)
from silver.transform import (  # noqa: E402
    add_partitions, dedupe, restrict_window,
)
from silver.validation import (  # noqa: E402
    SchemaValidator, ValidationError, check_required,
)

OK, KO = "OK   ", "ECHEC"
RESULTS: list[bool] = []

# Cardinalites du jeu factice, reutilisees dans les assertions.
N_NORMAL = 96          # 2024-03-15, un point tous les quarts d'heure
N_DST = 8              # 2024-10-27, 02:00 a 02:45 en double offset
N_EMPTY = 2            # lignes sans aucune mesure : format normal, pas rejet
N_BAD = 4              # hors bornes, cast impossible, perimetre, cle nulle
N_NULLED = 1           # prevision aberrante : mesure annulee, ligne gardee
N_ROWS = N_NORMAL + N_DST + N_EMPTY + N_BAD + N_NULLED


def check(label: str, cond: bool) -> bool:
    print(f"  {OK if cond else KO} {label}")
    RESULTS.append(bool(cond))
    return bool(cond)


# ---------------------------------------------------------------------------
# Bronze factice
# ---------------------------------------------------------------------------

CSV_HEADER = ("perimetre;nature;date;heure;date_heure;consommation;prevision_j1;"
              "prevision_j;taux_co2;ech_physiques;nucleaire;eolien;"
              "eolien_terrestre;eolien_offshore;solaire;hydraulique;gaz;fioul;"
              "charbon;bioenergies;pompage")


def csv_row(ts: str, conso, *, nature="Donnees definitives", perimetre="France",
            prev_j1=50500, prev_j=50200, co2=45, echanges=-9485,
            filieres=True) -> str:
    """Une ligne d'export eco2mix. Tout vide = ligne sans aucune mesure."""
    date, heure = (ts[:10], ts[11:16]) if ts else ("", "")
    prod = ("42431;9890;8500;1390;2000;6000;3000;100;200;800;-737"
            if filieres else ";" * 10)
    return (f"{perimetre};{nature};{date};{heure};{ts};{conso};{prev_j1};"
            f"{prev_j};{co2};{echanges};{prod}")


def make_csv(path: Path) -> None:
    rows = [CSV_HEADER]

    # Journee normale, offset d'hiver.
    t0 = datetime(2024, 3, 15, 0, 0)
    for i in range(N_NORMAL):
        ts = (t0 + timedelta(minutes=15 * i)).strftime("%Y-%m-%dT%H:%M:%S+01:00")
        # 05:00 sans taux_co2 : c'est le temps reel qui le fournira, et la
        # fusion mesure par mesure doit aller le chercher la-bas.
        co2 = "" if i == 20 else 45
        rows.append(csv_row(ts, 50000 + i * 10, co2=co2))

    # Nuit du changement d'heure : 02:00 locale existe deux fois.
    for off, conso in (("+02:00", 41000), ("+01:00", 42000)):
        for mn in ("00", "15", "30", "45"):
            rows.append(csv_row(f"2024-10-27T02:{mn}:00{off}", conso))

    # Lignes sans aucune mesure : le consolide ne publie les mesures qu'au
    # pas de 30 min. Ce n'est pas une anomalie, c'est le format.
    for mn in ("00", "15"):
        rows.append(csv_row(f"2024-03-16T00:{mn}:00+01:00", "", prev_j1="",
                            prev_j="", co2="", echanges="", filieres=False))

    # Anomalies, une par motif de rejet.
    rows.append(csv_row("2024-03-17T00:00:00+01:00", 200000))            # bornes
    rows.append(csv_row("2024-03-17T00:15:00+01:00", "ABC"))             # cast
    rows.append(csv_row("2024-03-17T00:30:00+01:00", 51000,
                        perimetre="Grand-Est"))                          # dimension
    rows.append(csv_row("", 52000))                                      # cle nulle
    # Prevision aberrante : la ligne survit, seule la prevision est annulee.
    rows.append(csv_row("2024-03-17T01:00:00+01:00", 53000, prev_j1=999999))

    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def make_stream(path: Path) -> None:
    """Sortie du job streaming : payload encapsule, valeur Kafka en chaine."""
    lines = []
    t0 = datetime(2024, 3, 15, 0, 0)

    def envelope(payload: dict, ingested: str) -> str:
        return json.dumps({
            "raw_value": json.dumps({"_meta": {"source": "eco2mix_tr"},
                                     "payload": payload}),
            "bronze_ingested_at": ingested,
            "kafka_offset": 1,
        })

    # Recouvrement volontaire avec le CSV : ces valeurs doivent perdre.
    for i in range(8):
        ts = (t0 + timedelta(minutes=15 * i)).strftime("%Y-%m-%dT%H:%M:%S+01:00")
        lines.append(envelope({
            "perimetre": "France", "nature": "Donnees temps reel",
            "date_heure": ts, "consommation": 99999, "prevision_j1": 50500,
            "prevision_j": 50200, "taux_co2": 45, "ech_physiques": -9000,
            "nucleaire": 35000, "eolien": 5000, "solaire": 2000,
            "hydraulique": 6000, "gaz": 3000, "fioul": 100, "charbon": 200,
            "bioenergies": 800, "pompage": -700,
            # Champs absents du consolide : le schema doit les absorber.
            "stockage_batterie": -269, "destockage_batterie": 8,
            "eolien_terrestre": 4300, "eolien_offshore": 700,
        }, "2024-03-15T01:00:00.000Z"))

    # 05:00 : le consolide n'a pas de taux_co2, le temps reel oui.
    ts_gap = (t0 + timedelta(minutes=15 * 20)).strftime("%Y-%m-%dT%H:%M:%S+01:00")
    lines.append(envelope({
        "perimetre": "France", "nature": "Donnees temps reel",
        "date_heure": ts_gap, "consommation": 99999, "taux_co2": 42,
        "nucleaire": 35000, "eolien": 5000,
    }, "2024-03-15T05:00:00.000Z"))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_meteo(path: Path, city: str, base_temp: float) -> None:
    """Archive Open-Meteo : tableaux paralleles, vent en km/h, fuseau GMT."""
    times = [f"2024-03-15T{h:02d}:00" for h in range(24)]
    doc = {
        "latitude": 45.8, "longitude": 4.83,
        "utc_offset_seconds": 0, "timezone": "GMT",
        "timezone_abbreviation": "GMT", "elevation": 184.0,
        "hourly_units": {"time": "iso8601", "temperature_2m": "°C",
                         "relative_humidity_2m": "%",
                         "wind_speed_10m": "km/h", "cloud_cover": "%"},
        "hourly": {
            "time": times,
            "temperature_2m": [base_temp + h * 0.1 for h in range(24)],
            "relative_humidity_2m": [80] * 24,
            # 36 km/h = 10 m/s exactement : la conversion se verifie a l'oeil.
            "wind_speed_10m": [36.0] * 24,
            "cloud_cover": [50] * 24,
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc), encoding="utf-8")


def build_bronze(root: Path) -> None:
    cons = root / "bronze" / "eco2mix_cons" / "year=2024" / "month=03"
    cons.mkdir(parents=True)
    make_csv(cons / "eco2mix_national_2024_03.csv")

    stream = (root / "bronze" / "eco2mix_tr"
              / "ingest_date=2024-03-15" / "ingest_hour=01")
    stream.mkdir(parents=True)
    make_stream(stream / "part-00000.json")

    for city, temp in (("lyon", 8.0), ("paris", 4.0)):
        make_meteo(root / "bronze" / "meteo_archive" / f"city={city}"
                   / "year=2024" / "month=03"
                   / f"open_meteo_{city}_2024_03.json", city, temp)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="silver_test_"))
    build_bronze(tmp)
    print(f"Bronze factice dans {tmp}\n")

    mapping = load_mapping()
    spark = (SparkSession.builder.appName("test-silver")
             .master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # --- [1] Lecture Bronze pilotee par le mapping ------------------------
    print("[1] Lecture Bronze")
    raw_cons = read_source(spark, mapping, "eco2mix_cons", "", str(tmp))
    raw_tr = read_source(spark, mapping, "eco2mix_tr", "", str(tmp))
    check(f"CSV consolide lu : {raw_cons.count()} ligne(s), "
          f"{len(raw_cons.columns)} colonne(s)", raw_cons.count() == N_ROWS)
    check(f"streaming lu et payload parse : {len(raw_tr.columns)} colonne(s)",
          "consommation" in raw_tr.columns and "date_heure" in raw_tr.columns)
    check("schemas inegaux absorbes (stockage_batterie cote temps reel seul)",
          "stockage_batterie" in raw_tr.columns
          and "stockage_batterie" not in raw_cons.columns)

    # --- [2] Normalisation UTC -------------------------------------------
    print("\n[2] Normalisation UTC")
    cons = prepare(raw_cons, mapping, "eco2mix_cons")
    tr = prepare(raw_tr, mapping, "eco2mix_tr")

    # PIEGE : .first() rend un datetime converti dans le fuseau du DRIVER,
    # pas dans celui de la session Spark. Sur une machine reglee a Paris, un
    # ts_utc de 23:00 remonte en Python affiche 00:00 et le test passerait a
    # cote de son sujet. On formate donc la date COTE SPARK et on compare
    # des chaines.
    r = (cons.filter(F.col("ts_local").cast("string").startswith("2024-03-15 00:00"))
             .select(F.date_format("ts_utc", "yyyy-MM-dd HH:mm:ss").alias("utc"))
             .first())
    check(f"00:00 locale -> {r.utc} UTC (attendu 2024-03-14 23:00:00)",
          r.utc == "2024-03-14 23:00:00")

    dst = cons.filter(F.col("ts_local").cast("string").startswith("2024-10-27 02:"))
    n_dst, n_utc = dst.count(), dst.select("ts_utc").distinct().count()
    check(f"changement d'heure : {n_dst} lignes sur 02:xx locale (attendu {N_DST})",
          n_dst == N_DST)
    check(f"{n_utc} horodatages UTC distincts : aucun quart d'heure ecrase",
          n_utc == N_DST)

    check("qualite lue dans le champ nature, pas deduite du chemin",
          cons.select("quality").distinct().collect()[0][0] == "definitive"
          and tr.select("quality").distinct().collect()[0][0] == "realtime")

    # --- [3] et [4] Validation et quarantaine -----------------------------
    print("\n[3] Validation de schema")
    win = restrict_window(cons, "2024-01-01", "2024-12-31", keep_null=True)
    load_cons = build_grid_load(win, mapping, "eco2mix_cons")
    rep = load_cons.report
    reasons = {k.split(":")[0] for k in rep.reasons}

    check(f"{rep.n_rejected} ligne(s) rejetee(s) (attendu {N_BAD})",
          rep.n_rejected == N_BAD)
    check(f"quatre motifs distincts : {sorted(reasons)}",
          reasons == {"out_of_range", "cast_failed", "unexpected_value",
                      "null_key"})
    check(f"{rep.n_dropped_empty} ligne(s) sans mesure ignoree(s) sans rejet "
          f"(attendu {N_EMPTY})", rep.n_dropped_empty == N_EMPTY)
    check(f"{rep.n_valid} ligne(s) valide(s) (attendu {N_ROWS - N_BAD - N_EMPTY})",
          rep.n_valid == N_ROWS - N_BAD - N_EMPTY)

    nulled = load_cons.df.filter(
        (F.col("consumption_mw") == 53000) & F.col("forecast_j1_mw").isNull())
    check("prevision hors bornes annulee, ligne conservee", nulled.count() == 1)

    # Garde-fous structurels : ce ne sont pas des lignes fautives mais un
    # lot ou un mapping faux. Le job doit tomber, pas produire une table
    # amputee que l'on decouvrirait en Gold.
    try:
        check_required([c for c in raw_cons.columns if c != "date_heure"],
                       ("date_heure", "consommation"), mapping, "eco2mix_cons")
        check("champ structurant absent : echec immediat attendu", False)
    except ValidationError as exc:
        check(f"champ structurant absent -> echec immediat ({str(exc)[:46]}...)",
              "date_heure" in str(exc))

    try:
        strict = SchemaValidator(mapping, "grid_load", "test")
        strict.report.n_input, strict.report.n_rejected = 100, 90
        strict.enforce_ratio()
        check("taux de rejet massif : echec attendu", False)
    except ValidationError as exc:
        check("90 % de rejets -> le job echoue au lieu de publier une table "
              "amputee", "mapping casse" in str(exc))

    print("\n[4] Quarantaine")
    rj = load_cons.rejects.cache()
    check(f"{rj.count()} ligne(s) en quarantaine, schema stable",
          rj.count() == N_BAD and "payload" in rj.columns)
    payload = rj.filter(F.col("reject_reason") == "out_of_range") \
                .select("payload").first()[0]
    check("charge utile complete conservee pour rejeu",
          '"consommation":"200000"' in payload.replace(" ", "")
          or '"consommation":200000' in payload.replace(" ", ""))
    check("motif et colonne fautive renseignes",
          rj.filter(F.col("reject_column").isNull()).count() == 0)

    # --- [5] Deduplication ------------------------------------------------
    print("\n[5] Deduplication et fusion")
    load_tr = build_grid_load(
        restrict_window(tr, "2024-01-01", "2024-12-31", keep_null=True),
        mapping, "eco2mix_tr")

    spec = mapping.table("grid_load")
    union = load_cons.df.unionByName(load_tr.df)
    before = union.count()
    load = dedupe(union, list(spec.keys), spec.dedup_strategy,
                  list(spec.measures)).cache()
    after = load.count()

    check(f"{before} ligne(s) -> {after} apres dedup", after < before)
    check("aucune valeur temps reel n'a survecu au consolide",
          load.filter(F.col("consumption_mw") == 99999).count() == 0)
    check("aucun doublon sur (ts_utc, zone_id)",
          load.groupBy("ts_utc", "zone_id").count()
              .filter(F.col("count") > 1).count() == 0)

    # 2024-03-15T05:00:00+01:00 cote CSV, soit 04:00 UTC : le consolide y a
    # une consommation mais pas de taux CO2, le temps reel a l'inverse.
    merged = load.filter(F.col("ts_utc") == F.lit("2024-03-15 04:00:00")
                         .cast("timestamp")).first()
    check(f"fusion mesure par mesure : consommation {merged.consumption_mw:.0f} "
          f"du consolide, taux CO2 {merged.co2_rate_g_kwh:.0f} du temps reel",
          merged.consumption_mw == 50200 and merged.co2_rate_g_kwh == 42)

    # --- [6] Depivotement -------------------------------------------------
    print("\n[6] Depivotement des filieres")
    gen_cons = build_grid_generation(win, mapping, "eco2mix_cons")
    gen_tr = build_grid_generation(
        restrict_window(tr, "2024-01-01", "2024-12-31", keep_null=True),
        mapping, "eco2mix_tr")
    gen = gen_cons.df.unionByName(gen_tr.df).cache()

    filieres = sorted(r.filiere for r in
                      gen.select("filiere").distinct().collect())
    check(f"{len(filieres)} filiere(s) : {', '.join(filieres)}",
          set(filieres) == {"nucleaire", "eolien", "solaire", "hydraulique",
                            "gaz", "fioul", "charbon", "bioenergies",
                            "pompage", "stockage_batterie",
                            "destockage_batterie"})
    check("aucune filiere de detail emise : pas de double compte en Gold",
          not {"eolien_terrestre", "eolien_offshore"} & set(filieres)
          and gen.filter(F.col("filiere_level") != "aggregate").count() == 0)
    check("stockage distingue de la production",
          gen.filter(F.col("filiere_category") == "stockage")
             .select("filiere").distinct().count() == 3)

    renew = sorted(r.filiere for r in gen.filter(F.col("is_renewable"))
                   .select("filiere").distinct().collect())
    check(f"{len(renew)} filiere(s) renouvelable(s) : {', '.join(renew)}",
          set(renew) == {"eolien", "solaire", "hydraulique", "bioenergies"})
    check("filiere absente d'une source simplement signalee, pas fatale",
          "stockage_batterie" in gen_cons.report.missing_filieres
          and "stockage_batterie" in gen_tr.report.present_filieres)

    # --- [7] Meteo --------------------------------------------------------
    print("\n[7] Meteo : tableaux paralleles et unites")
    raw_meteo = read_source(spark, mapping, "meteo_archive", "", str(tmp))
    check_timezone(raw_meteo, mapping)
    notes = check_units(raw_meteo, mapping)
    check(f"fuseau verifie sur utc_offset_seconds, libelle GMT accepte "
          f"({len(notes)} alerte d'unite)", not notes)

    weather, w_rejects, w_report = build_weather(
        explode_hourly(raw_meteo, mapping), mapping)
    weather = weather.cache()
    check(f"{weather.count()} ligne(s) : 2 villes x 24 h",
          weather.count() == 48)

    wind = weather.select("wind_speed_ms").first()[0]
    check(f"vent converti : 36 km/h -> {wind:.2f} m/s (attendu 10.00)",
          abs(wind - 10.0) < 1e-6)

    national = national_average(weather, {"lyon": 0.2, "paris": 0.35},
                               list(mapping.table("weather").measures))
    nat = national.orderBy("ts_utc").first()
    expected = (8.0 * 0.2 + 4.0 * 0.35) / 0.55
    check(f"moyenne nationale ponderee renormalisee : {nat.temperature_c:.3f} "
          f"degres (attendu {expected:.3f})",
          abs(nat.temperature_c - expected) < 1e-6)

    # --- [8] Ecriture et idempotence --------------------------------------
    print("\n[8] Ecriture partitionnee et idempotence")
    out_dir = tmp / "silver" / "grid_load"
    final = add_partitions(load, mapping)

    def write() -> None:
        (final.repartition("year", "month").write.mode("overwrite")
              .partitionBy("year", "month").parquet(str(out_dir)))

    write()
    parts = sorted(p.name for p in out_dir.glob("year=*/month=*"))
    n1 = spark.read.parquet(str(out_dir)).count()
    write()   # rejeu : ecrasement dynamique des memes partitions
    n2 = spark.read.parquet(str(out_dir)).count()

    check(f"partitions creees : {parts}", len(parts) >= 2)
    check(f"relecture : {n1} ligne(s)", n1 == after)
    check(f"rejeu du meme lot : {n2} ligne(s), aucune duplication", n1 == n2)

    # --- Apercu -----------------------------------------------------------
    print("\n" + "-" * 72)
    print("Apercu de silver/grid_load :")
    (spark.read.parquet(str(out_dir))
     .select("ts_utc", "ts_local", "consumption_mw", "co2_rate_g_kwh",
             "source", "quality")
     .orderBy("ts_utc").show(5, truncate=False))

    print("Apercu de silver/grid_generation :")
    gen.select("ts_utc", "filiere", "generation_mw", "is_renewable",
               "filiere_category", "quality").orderBy("ts_utc", "filiere") \
       .show(5, truncate=False)

    print("Apercu de silver/_rejects/grid_load :")
    rj.select("reject_reason", "reject_column", "source").show(5, truncate=False)

    print("Bilan de validation (extrait du rapport JSON) :")
    print(json.dumps(load_cons.report.as_dict(), ensure_ascii=False, indent=2))

    spark.stop()
    shutil.rmtree(tmp, ignore_errors=True)

    print("-" * 72)
    passed = sum(RESULTS)
    if passed == len(RESULTS):
        print(f"Tous les controles passent ({passed}/{len(RESULTS)}).")
        return 0
    print(f"{passed}/{len(RESULTS)} controles passent.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
