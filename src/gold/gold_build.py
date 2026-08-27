#!/usr/bin/env python3
"""
Gold : Silver -> mix_horaire, kpi_daily, ml_features.

C'est ici que les deux sources se croisent, sur la cle commune
(ts_utc, zone_id). Si cette jointure produit des lignes, le datalake tient
sa promesse : ce ne sont pas deux pipelines paralleles.

Trois tables :
  mix_horaire  : conso + production par filiere + meteo, au pas horaire
  kpi_daily    : agregats metier prets pour la restitution
  ml_features  : cible a H+24, lags, features calendaires et meteo

Fenetrage : cf. docs/decisions.md, et `windows.py` pour les derivations. Le
job lit `window_read` (elargi par les features), n'ecrit que `window_written`
(la fenetre demandee alignee sur le mois, unite de partition des trois
tables), et materialise cette derniere en colonnes pour qu'une partition
puisse dire quelle fenetre l'a produite.

    spark-submit gold_build.py --start 2024-03-01 --end 2024-03-31
"""

from __future__ import annotations

import argparse
import logging
import math
import sys

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, load_config  # noqa: E402
from gold.windows import (FeatureSpan, TimeWindow, align_to_month,  # noqa: E402
                          reading_window)
# Lecture seule : le mapping Silver est la source de verite pour la liste des
# filieres et l'echelle de qualite. Gold les lit, il ne les recopie pas.
from silver.mapping import SilverMapping, load_mapping, mapping_beside  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s [gold] %(message)s")
log = logging.getLogger(__name__)

# Seuil de confort : base des degres-jours. 18 C est la convention en France.
BASE_TEMP = 18.0

SECONDS_PER_HOUR = 3600

# La portee temporelle des features de ml_features. C'est la SEULE
# declaration : `window_read` en decoule (8 jours en amont, 1 en aval), et
# les fenetres glissantes ci-dessous s'y referent. Ajouter un lag ici suffit
# a allonger la lecture, cf. docs/decisions.md.
FEATURES = FeatureSpan(lags=(24, 48, 168), rolling=24, lead=24)


def to_hourly(df: DataFrame, ts_col: str = "ts_utc") -> DataFrame:
    """eco2mix est au quart d'heure, la meteo a l'heure. On aligne sur
    l'heure : c'est le plus petit denominateur commun, et il faut le
    justifier plutot que de le subir."""
    return df.withColumn("ts_hour", F.date_trunc("hour", F.col(ts_col)))


def expected_filieres(mapping: SilverMapping) -> list[str]:
    """Les filieres que Silver ecrit reellement, dans l'ordre.

    On interroge le mapping avec ses propres noms de filieres comme s'ils
    etaient des colonnes source : `resolve_filieres` applique alors ses
    filtres declares (include_levels, include_categories) et ne rend que ce
    que grid_generation contient. Les details — eolien_terrestre, gaz_tac —
    sont ecartes par le mapping lui-meme, sans qu'on redise ici lesquels.
    """
    found, _ = mapping.resolve_filieres(list(mapping.filieres))
    return sorted(found)


def quality_label(mapping: SilverMapping, rank: Column) -> Column:
    """Rang de qualite -> libelle, avec l'echelle declaree dans le mapping."""
    pairs: list[Column] = []
    for level, value in sorted(mapping.rank_pairs.items(), key=lambda kv: kv[1]):
        pairs += [F.lit(value), F.lit(level)]
    return F.coalesce(F.create_map(*pairs)[rank], F.lit("unknown"))


def build_mix_horaire(load: DataFrame, gen: DataFrame, weather: DataFrame,
                      mapping: SilverMapping) -> DataFrame:
    """La table de jointure : le produit du croisement des sources."""

    load_h = (to_hourly(load)
              .groupBy("ts_hour", "zone_id")
              .agg(F.avg("consumption_mw").alias("consumption_mw"),
                   F.avg("forecast_j1_mw").alias("forecast_j1_mw"),
                   F.avg("forecast_j_mw").alias("forecast_j_mw"),
                   F.avg("co2_rate_g_kwh").alias("co2_rate_g_kwh"),
                   # Solde des echanges aux frontieres, negatif a l'export.
                   # Silver le porte depuis le debut ; sans lui le bilan de
                   # l'heure ne boucle pas : conso != production nationale.
                   F.avg("physical_exchange_mw").alias("physical_exchange_mw"),
                   # Qualite de l'heure = la PIRE des lignes qui l'ont
                   # formee (rang croissant = qualite decroissante). Une
                   # heure moyennee sur du temps reel ne doit pas se
                   # presenter comme definitive.
                   F.max("quality_rank").alias("quality_rank_load"),
                   # Combien de points Silver sont tombes dans cette heure.
                   # 4 pour le temps reel (15 min), 2 pour le consolide
                   # (30 min) : une heure a 1 point est une moyenne sur un
                   # quart d'heure, et la moyenne ne le dit pas. Materialise
                   # plutot que filtre — c'est au lecteur de trancher.
                   F.count(F.lit(1)).alias("n_points")))

    # Production : pivot des filieres en colonnes + totaux.
    gen_h = to_hourly(gen).groupBy("ts_hour", "zone_id", "filiere") \
                          .agg(F.avg("generation_mw").alias("mw"),
                               F.first("is_renewable").alias("is_renew"),
                               F.first("filiere_category").alias("category"),
                               F.max("quality_rank").alias("quality_rank"))

    # Liste de filieres passee explicitement au pivot. Deux effets, et le
    # second est le vrai motif : Spark n'a plus besoin d'un scan distinct
    # prealable, et surtout le SCHEMA NE DEPEND PLUS DES DONNEES. Un mois
    # sans solaire produisait sinon une table sans colonne solaire, et deux
    # partitions aux colonnes differentes ne se relisent pas ensemble.
    filieres = expected_filieres(mapping)
    pivoted = (gen_h.groupBy("ts_hour", "zone_id")
               .pivot("filiere", filieres)
               .agg(F.first("mw")))

    # Production et stockage ne se somment pas ensemble : le pompage et la
    # charge batterie sont des filieres NEGATIVES (cf. la section filieres
    # de conf/silver_mapping.yml). Les additionner a la production revient a
    # soustraire du parc ce qu'il produit, et ecrase le denominateur de
    # renewable_share_pct. Le critere est `filiere_category`, porte par
    # Silver, pas une liste de noms recopiee ici.
    is_prod = F.col("category") == F.lit("production")
    is_storage = F.col("category") == F.lit("stockage")

    totals = (gen_h.groupBy("ts_hour", "zone_id")
              .agg(F.sum(F.when(is_prod, F.col("mw")))
                   .alias("generation_total_mw"),
                   F.sum(F.when(is_prod & F.col("is_renew"), F.col("mw")))
                   .alias("generation_renewable_mw"),
                   # Solde du stockage, signe : negatif = le parc absorbe
                   # (pompage, charge), positif = il restitue.
                   F.sum(F.when(is_storage, F.col("mw")))
                   .alias("storage_net_mw"),
                   F.max("quality_rank").alias("quality_rank_gen")))

    weather_h = (to_hourly(weather)
                 .filter(F.col("zone_id") == "fr")
                 .groupBy("ts_hour")
                 .agg(F.avg("temperature_c").alias("temperature_c"),
                      F.avg("humidity_pct").alias("humidity_pct"),
                      F.avg("wind_speed_ms").alias("wind_speed_ms"),
                      F.avg("cloud_cover_pct").alias("cloud_cover_pct")))

    out = (load_h
           .join(pivoted, ["ts_hour", "zone_id"], "left")
           .join(totals, ["ts_hour", "zone_id"], "left")
           .join(weather_h, ["ts_hour"], "left")
           .withColumnRenamed("ts_hour", "ts_utc")
           .withColumn("ts_local", F.from_utc_timestamp("ts_utc", "Europe/Paris"))
           .withColumn("renewable_share_pct",
                       F.when(F.col("generation_total_mw") > 0,
                              100 * F.col("generation_renewable_mw")
                              / F.col("generation_total_mw")))
           .withColumn("forecast_error_mw",
                       F.col("consumption_mw") - F.col("forecast_j1_mw"))
           # Qualite de l'heure : le pire des deux flux electriques. La
           # meteo n'entre pas dans l'indicateur — c'est la qualite de la
           # mesure RESEAU qu'il annonce, pas celle de la temperature.
           # Rang ET libelle, comme Silver : le rang se compare, le libelle
           # se lit dans le notebook.
           .withColumn("quality_rank",
                       F.coalesce(F.greatest("quality_rank_load",
                                             "quality_rank_gen"),
                                  F.lit(mapping.quality_rank("unknown"))))
           .withColumn("quality", quality_label(mapping, F.col("quality_rank")))
           .drop("quality_rank_load", "quality_rank_gen"))

    return out.withColumn("year", F.year("ts_utc")) \
              .withColumn("month", F.month("ts_utc"))


def build_kpi_daily(mix: DataFrame) -> DataFrame:
    """KPIs metier, directement lisibles par le notebook."""
    day = mix.withColumn("date_local", F.to_date("ts_local"))

    # La journee est la cle METIER, mais elle n'est pas la cle : c'est
    # (date_local, zone_id). Sans zone_id, le pic d'une journee est celui de
    # la zone la plus consommatrice et les autres disparaissent, puis la
    # jointure ci-dessous duplique chaque ligne. Une seule zone aujourd'hui
    # ne rend pas la maille juste, elle rend l'erreur invisible.
    peak = (day.withColumn(
        "_rk", F.row_number().over(
            Window.partitionBy("date_local", "zone_id")
                  .orderBy(F.col("consumption_mw").desc_nulls_last())))
        .filter(F.col("_rk") == 1)
        .select("date_local", "zone_id",
                F.col("consumption_mw").alias("peak_mw"),
                F.hour("ts_local").alias("peak_hour_local")))

    agg = (day.groupBy("date_local", "zone_id").agg(
        (F.sum("consumption_mw") / 1000).alias("consumption_gwh"),
        F.avg("consumption_mw").alias("consumption_avg_mw"),
        F.avg("co2_rate_g_kwh").alias("co2_avg_g_kwh"),
        # Ratio des totaux, pas moyenne des ratios horaires : les deux ne
        # donnent le meme chiffre que si la production est constante sur la
        # journee. Une heure creuse tres renouvelable pese autant qu'une
        # pointe fossile dans une moyenne de pourcentages, alors qu'elle
        # pese ses MWh dans le mix reel. Les sommes sont au pas horaire,
        # donc des MWh : c'est bien la part d'energie de la journee.
        F.when(F.sum("generation_total_mw") > 0,
               100 * F.sum("generation_renewable_mw")
               / F.sum("generation_total_mw")).alias("renewable_share_pct"),
        F.avg("physical_exchange_mw").alias("physical_exchange_avg_mw"),
        F.avg("temperature_c").alias("temperature_avg_c"),
        F.min("temperature_c").alias("temperature_min_c"),
        F.max("temperature_c").alias("temperature_max_c"),
        F.avg(F.abs("forecast_error_mw")).alias("forecast_mae_mw"),
        F.count("*").alias("n_hours"),
        # Le KPI du jour ne vaut pas mieux que la pire heure qui le compose.
        F.max("quality_rank").alias("quality_rank")))

    return (agg.join(peak, ["date_local", "zone_id"], "left")
            # Degres-jours : la variable qui explique le mieux la conso.
            .withColumn("hdd", F.greatest(F.lit(BASE_TEMP) - F.col("temperature_avg_c"),
                                          F.lit(0.0)))
            .withColumn("cdd", F.greatest(F.col("temperature_avg_c") - F.lit(BASE_TEMP),
                                          F.lit(0.0)))
            # year/month, pas year seul : l'unite d'alignement d'une table
            # est son unite de partition. Avec year seul, ecrire un mois
            # remplacerait l'annee entiere par ce mois.
            .withColumn("year", F.year("date_local"))
            .withColumn("month", F.month("date_local")))


def at_offset(col: str, hours: int, w: Window) -> Column:
    """Valeur de `col` exactement `hours` heures avant (negatif) ou apres.

    F.lag(col, 24) compte des LIGNES. Sur une serie horaire complete c'est la
    meme chose, mais mix_horaire a des trous : une heure sans donnee source
    n'y produit pas de ligne. Un lag en lignes va alors chercher H-23 ou
    H-22 et l'appelle lag_24h — l'erreur est silencieuse et elle contamine
    l'apprentissage.

    Le frame `rangeBetween` compte des SECONDES sur ts_utc : la fenetre est
    reduite au seul point situe exactement a l'offset demande. S'il n'existe
    pas, elle est vide et la valeur est nulle. Un trou reste un trou.
    """
    offset = hours * SECONDS_PER_HOUR
    return F.first(col).over(w.rangeBetween(offset, offset))


def build_ml_features(mix: DataFrame, holidays: list[str] | None = None) -> DataFrame:
    """Cible : consommation a H+24. Features : lags, calendrier, meteo.

    La prevision J-1 de RTE est conservee comme colonne de reference :
    c'est le benchmark contre lequel comparer le modele, et il est deja
    dans les donnees.
    """
    w = Window.partitionBy("zone_id").orderBy(F.col("ts_utc").cast("long"))
    # Fenetre glissante en SECONDES, pas en lignes : les 24 dernieres heures
    # revolues, quel que soit le nombre de points qu'elles contiennent.
    last_24h = w.rangeBetween(-(FEATURES.rolling - 1) * SECONDS_PER_HOUR, 0)

    df = mix.withColumn("target_consumption_h24",
                        at_offset("consumption_mw", +FEATURES.lead, w))
    for hours in FEATURES.lags:
        df = df.withColumn(f"lag_{hours}h",
                           at_offset("consumption_mw", -hours, w))

    df = (df
          .withColumn("roll_mean_24h",
                      F.avg("consumption_mw").over(last_24h))
          .withColumn("roll_std_24h",
                      F.stddev("consumption_mw").over(last_24h))
          # Calendrier sur l'heure LOCALE : la conso suit le rythme humain,
          # pas UTC.
          .withColumn("hour", F.hour("ts_local"))
          .withColumn("dow", F.dayofweek("ts_local"))
          .withColumn("month_of_year", F.month("ts_local"))
          .withColumn("is_weekend", F.col("dow").isin(1, 7).cast("int"))
          # Encodage cyclique : 23h et 0h sont voisines. math.pi plutot
          # qu'une troncature a 8 chiffres : hour_sin(6h) doit valoir 1
          # exactement, pas 1 - 4e-9.
          .withColumn("hour_sin", F.sin(2 * F.lit(math.pi) * F.col("hour") / 24))
          .withColumn("hour_cos", F.cos(2 * F.lit(math.pi) * F.col("hour") / 24))
          .withColumn("hdd", F.greatest(F.lit(BASE_TEMP) - F.col("temperature_c"),
                                        F.lit(0.0)))
          .withColumn("cdd", F.greatest(F.col("temperature_c") - F.lit(BASE_TEMP),
                                        F.lit(0.0))))

    if holidays:
        df = df.withColumn(
            "is_holiday",
            F.to_date("ts_local").cast("string").isin(holidays).cast("int"))
    else:
        df = df.withColumn("is_holiday", F.lit(0))

    return (df.filter(F.col("target_consumption_h24").isNotNull())
              .select("ts_utc", "ts_local", "zone_id",
                      "target_consumption_h24", "consumption_mw",
                      *[f"lag_{h}h" for h in FEATURES.lags],
                      "roll_mean_24h", "roll_std_24h", "n_points",
                      "hour", "dow", "month_of_year", "is_weekend",
                      "hour_sin", "hour_cos", "is_holiday",
                      "temperature_c", "hdd", "cdd", "wind_speed_ms",
                      "cloud_cover_pct",
                      # Benchmark RTE : le modele doit se comparer a ca.
                      F.col("forecast_j1_mw").alias("rte_forecast_j1_mw"),
                      "year", "month"))


# ---------------------------------------------------------------------------
# Fenetrage cote Spark
# ---------------------------------------------------------------------------

def prune_partitions(df: DataFrame, window: TimeWindow) -> DataFrame:
    """Filtre sur les colonnes de PARTITION, pas sur ts_utc.

    C'est ce predicat-la que Spark pousse jusqu'a l'elagage de repertoires :
    un filtre sur ts_utc apres la jointure lit d'abord toute la table. Sans
    lui, calculer un mois lit les trois ans d'historique.
    """
    if not {"year", "month"} <= set(df.columns):
        return df
    keys = [year * 100 + month for year, month in window.months()]
    return df.filter((F.col("year") * 100 + F.col("month")).isin(keys))


def within_ts(df: DataFrame, window: TimeWindow,
              col: str = "ts_utc") -> DataFrame:
    """Borne un horodatage. Borne haute exclusive : `stop`, pas `end`."""
    return df.filter((F.col(col) >= F.lit(str(window.start)).cast("timestamp"))
                     & (F.col(col) < F.lit(str(window.stop)).cast("timestamp")))


def within_date(df: DataFrame, window: TimeWindow,
                col: str = "date_local") -> DataFrame:
    """Borne une date LOCALE, bornes incluses.

    kpi_daily est partitionnee sur date_local : ses partitions sont des mois
    locaux, pas des mois UTC. Les deux ne coincident pas — le 31 mars 23:00
    UTC est deja le 1er avril a Paris. Borner kpi_daily sur ts_utc y ferait
    donc entrer une journee d'avril isolee, et l'ecrasement dynamique
    remplacerait tout le mois d'avril par ce seul jour.
    """
    return df.filter((F.col(col) >= F.lit(str(window.start)).cast("date"))
                     & (F.col(col) <= F.lit(str(window.end)).cast("date")))


def stamp_window(df: DataFrame, window: TimeWindow) -> DataFrame:
    """Materialise window_written dans la donnee.

    Sur le modele des metadonnees du marker _SUCCESS de Bronze : une
    partition doit pouvoir dire quelle fenetre l'a produite sans qu'on aille
    lire les logs du run. Deux colonnes seulement, et pas d'horodatage de
    build : rejouer un mois clos doit redonner le meme fichier.
    """
    return (df.withColumn("window_start", F.lit(str(window.start)).cast("date"))
              .withColumn("window_end", F.lit(str(window.end)).cast("date")))


def write_gold(df: DataFrame, path: str, label: str, window: TimeWindow,
               partitions: tuple[str, ...] = ("year", "month")) -> int:
    """Ecrasement DYNAMIQUE de partition, borne a window_written.

    L'appelant a deja restreint `df` a la fenetre : ce qui arrive ici couvre
    des partitions entieres, et les remplacer est donc legitime.
    """
    out = stamp_window(df, window)
    n = out.count()
    if n == 0:
        log.warning("%s : 0 ligne sur %s, ecriture ignoree. Les partitions "
                    "existantes sont laissees intactes.", label, window)
        return 0

    (out.repartition(*partitions)
        .write.mode("overwrite")
        .partitionBy(*partitions)
        .parquet(path))
    log.info("%s : %d ligne(s) ecrite(s) sur %s (partitions %s).",
             label, n, window, "/".join(partitions))
    return n


def fetch_holidays() -> list[str]:
    """Jours feries francais. API publique sans cle. Echec non bloquant."""
    try:
        import requests
        r = requests.get("https://calendrier.api.gouv.fr/jours-feries/metropole.json",
                         timeout=15)
        r.raise_for_status()
        days = sorted(r.json().keys())
        log.info("%d jours feries recuperes.", len(days))
        return days
    except Exception as exc:  # noqa: BLE001
        log.warning("Jours feries indisponibles (%s), feature mise a 0.", exc)
        return []


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--conf", default=None)
    ap.add_argument("--mapping", default=None,
                    help="chemin de silver_mapping.yml ; par defaut, "
                         "le voisin de --conf")
    ap.add_argument("--no-holidays", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    fs = cfg["hdfs"]["fs_uri"]
    # Meme convention que les jobs Silver : le mapping suit --conf.
    mapping = load_mapping(args.mapping or mapping_beside(args.conf))
    filieres = expected_filieres(mapping)
    log.info("Mapping v%s : %d filiere(s) attendue(s) au pivot (%s).",
             mapping.version, len(filieres), ", ".join(filieres))

    # Les trois fenetres, derivees ici et nulle part ailleurs. Le job les
    # calcule lui-meme plutot que de faire confiance a son appelant.
    requested = TimeWindow.of(args.start, args.end)
    written = align_to_month(requested)
    to_read = reading_window(written, FEATURES)

    log.info("window_requested : %s", requested)
    log.info("window_written   : %s (aligne sur le mois, unite de partition "
             "des trois tables)", written)
    log.info("window_read      : %s (-%d j / +%d j, derives des features : "
             "lags %s, roulant %d h, cible a +%d h)",
             to_read, FEATURES.lookback_days, FEATURES.lookahead_days,
             "/".join(f"{h}h" for h in FEATURES.lags),
             FEATURES.rolling, FEATURES.lead)
    if (written.start, written.end) != (requested.start, requested.end):
        log.info("La fenetre demandee a ete elargie au mois entier : ecrire "
                 "un demi-mois supprimerait l'autre moitie.")

    spark = (SparkSession.builder.appName("gold-build")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def read_silver(table: str) -> DataFrame:
        df = spark.read.parquet(f"{fs}{layout.silver(table)}")
        return within_ts(prune_partitions(df, to_read), to_read)

    load = read_silver("grid_load")
    gen = read_silver("grid_generation")
    try:
        weather = read_silver("weather")
    except Exception:  # noqa: BLE001
        log.warning("silver/weather absent : Gold sans meteo.")
        weather = spark.createDataFrame(
            [], "ts_utc timestamp, zone_id string, temperature_c double, "
                "humidity_pct double, wind_speed_ms double, cloud_cover_pct double")

    # Calcule sur window_read : les lags du 1er du mois ont besoin des huit
    # jours qui le precedent, la cible du 31 a besoin du 1er du mois suivant.
    mix_wide = build_mix_horaire(load, gen, weather, mapping)
    mix_wide.cache()

    # Ecrit sur window_written, et sur elle seule.
    mix = within_ts(mix_wide, written)
    n_mix = mix.count()
    n_joined = mix.filter(F.col("temperature_c").isNotNull()).count()
    log.info("mix_horaire : %d ligne(s), dont %d avec meteo jointe (%.0f%%).",
             n_mix, n_joined, 100 * n_joined / max(n_mix, 1))
    if n_mix and not n_joined:
        log.warning("AUCUNE jointure meteo : verifier que les deux sources "
                    "couvrent bien la meme periode.")

    write_gold(mix, f"{fs}{layout.gold('mix_horaire')}", "mix_horaire", written)

    # kpi_daily agrege par journee LOCALE : bornee sur date_local, et
    # calculee depuis mix_wide pour que le 1er et le dernier jour du mois
    # soient des journees completes.
    kpi = within_date(build_kpi_daily(mix_wide), written)
    write_gold(kpi, f"{fs}{layout.gold('kpi_daily')}", "kpi_daily", written)

    holidays = [] if args.no_holidays else fetch_holidays()
    feats = within_ts(build_ml_features(mix_wide, holidays), written)
    write_gold(feats, f"{fs}{layout.gold('ml_features')}", "ml_features", written)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
