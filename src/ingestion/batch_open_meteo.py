#!/usr/bin/env python3
"""
Ingestion batch : archive horaire Open-Meteo -> Bronze (JSON brut).

Deuxieme source, veritablement heterogene de la premiere :
  - format JSON contre CSV,
  - granularite horaire contre quart d'heure,
  - schema en colonnes paralleles (time[], temperature_2m[], ...) contre
    lignes tabulaires,
  - dimension supplementaire : la ville.

Un lot = une ville x un mois :
    /datalake/bronze/meteo_archive/city=lyon/year=2024/month=03/
L'idempotence est donc par (ville, mois) : une ville qui echoue n'invalide
pas les autres.

Aucune cle d'API. L'archive Open-Meteo accuse environ 5 jours de retard sur
le temps present, ce que le code borne explicitement.

Usage :
    python batch_open_meteo.py --start 2024-01-01 --end 2024-03-31
    python batch_open_meteo.py --start 2024-01-01 --end 2024-01-31 --city lyon
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from calendar import monthrange
from datetime import date, timedelta

import requests

sys.path.insert(0, "/opt/datalake/src")
from common.config import (  # noqa: E402
    Layout, hdfs_client, load_config, source_conf, weather_zones,
)
from common.hdfs_io import BronzeWriter, partition_months  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [open-meteo] %(message)s",
)
log = logging.getLogger(__name__)

SOURCE = "meteo_archive"
ARCHIVE_LAG_DAYS = 5  # l'archive n'est pas disponible avant ~J-5


def month_bounds(year: int, month: int) -> tuple[str, str]:
    last = monthrange(year, month)[1]
    start = date(year, month, 1)
    end = date(year, month, last)
    # On ne demande jamais au-dela de la disponibilite de l'archive.
    cap = date.today() - timedelta(days=ARCHIVE_LAG_DAYS)
    if end > cap:
        end = cap
    if start > end:
        raise ValueError(
            f"{year}-{month:02d} hors archive (disponible jusqu'au {cap})."
        )
    return start.isoformat(), end.isoformat()


def download(conf: dict, zone: dict, year: int, month: int) -> dict:
    start, end = month_bounds(year, month)
    params = {
        "latitude": zone["lat"],
        "longitude": zone["lon"],
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(conf["hourly_vars"]),
        "timezone": conf.get("timezone", "UTC"),
    }

    for attempt in range(1, 4):
        resp = requests.get(conf["api_base"], params=params, timeout=120)
        if resp.status_code == 429:
            wait = 20 * attempt
            log.warning("429 sur %s, attente %d s.", zone["zone_id"], wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"Echec Open-Meteo pour {zone['zone_id']} {year}-{month:02d}")


def sanity_check(doc: dict, year: int, month: int) -> int:
    hourly = doc.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        raise ValueError(f"Reponse sans serie horaire pour {year}-{month:02d}.")

    temps = hourly.get("temperature_2m") or []
    if len(temps) != len(times):
        raise ValueError(
            f"Series desalignees : {len(times)} horodatages, {len(temps)} temperatures."
        )

    # Tolerance : un mois peut etre tronque par ARCHIVE_LAG_DAYS.
    missing = sum(1 for t in temps if t is None)
    if missing > len(temps) * 0.1:
        raise ValueError(
            f"Trop de trous : {missing}/{len(temps)} temperatures nulles."
        )
    return len(times)


def ingest(client, layout: Layout, conf: dict, zone: dict, year: int,
           month: int, force: bool) -> str:
    part = layout.bronze_batch(SOURCE, year, month, city=zone["zone_id"])

    with BronzeWriter(client, part) as w:
        if not force and w.skip_if_done():
            return "skipped"

        doc = download(conf, zone, year, month)
        n = sanity_check(doc, year, month)

        # Bronze reste brut : on ecrit la reponse telle quelle. Les
        # metadonnees de zone vont dans _SUCCESS, pas dans le payload.
        filename = f"open_meteo_{zone['zone_id']}_{year:04d}_{month:02d}.json"
        w.write_json(filename, doc)

        w.commit({
            "source": SOURCE,
            "quality": conf["quality"],
            "zone_id": zone["zone_id"],
            "parent_zone_id": zone.get("parent_zone_id"),
            "lat": zone["lat"],
            "lon": zone["lon"],
            "year": year,
            "month": month,
            "file": filename,
            "hourly_records": n,
            "variables": conf["hourly_vars"],
            "timezone": doc.get("timezone"),
            "format": "json",
        })
        log.info("%s %04d-%02d : %d heures.", zone["zone_id"], year, month, n)
        return "ingested"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--city", default=None, help="limiter a une zone")
    ap.add_argument("--conf", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.conf)
    conf = source_conf(SOURCE if SOURCE in cfg["sources"] else "open_meteo", args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    client = hdfs_client(args.conf)

    targets = weather_zones(args.conf)
    if args.city:
        targets = [z for z in targets if z["zone_id"] == args.city]
        if not targets:
            log.error("Zone %s inconnue ou non marquee weather_proxy.", args.city)
            return 2

    months = partition_months(args.start, args.end)
    log.info("%d ville(s) x %d mois = %d lot(s).",
             len(targets), len(months), len(targets) * len(months))

    stats = {"ingested": 0, "skipped": 0, "failed": 0}
    for year, month in months:
        for zone in targets:
            try:
                stats[ingest(client, layout, conf, zone, year, month, args.force)] += 1
            except Exception as exc:  # noqa: BLE001
                log.error("Echec %s %04d-%02d : %s", zone["zone_id"], year, month, exc)
                stats["failed"] += 1
            time.sleep(0.5)  # courtoisie envers une API gratuite

    log.info("Bilan : %(ingested)d ingere(s), %(skipped)d ignore(s), "
             "%(failed)d en echec.", stats)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
