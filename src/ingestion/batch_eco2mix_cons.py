#!/usr/bin/env python3
"""
Ingestion batch : eco2mix-national-cons-def (CSV) -> Bronze.

Un lot = un mois. Le lot est ecrit dans
    /datalake/bronze/eco2mix_cons/year=YYYY/month=MM/
puis valide par _SUCCESS. Un lot deja marque n'est jamais retelecharge :
c'est la reponse a "un DAG interrompu doit pouvoir etre relance sans
dupliquer les donnees deja traitees".

Le CSV est stocke tel quel, delimiteur d'origine compris. Aucune
transformation en Bronze.

Usage :
    python batch_eco2mix_cons.py --start 2024-01-01 --end 2024-03-31
    python batch_eco2mix_cons.py --start 2024-01-01 --end 2024-01-31 --force
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from calendar import monthrange

import requests

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, hdfs_client, load_config, source_conf  # noqa: E402
from common.hdfs_io import BronzeWriter, partition_months  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [eco2mix-cons] %(message)s",
)
log = logging.getLogger(__name__)

SOURCE = "eco2mix_cons"


def month_bounds(year: int, month: int) -> tuple[str, str]:
    last = monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last:02d}"


def download_month(conf: dict, year: int, month: int) -> bytes:
    """Export CSV d'un mois via l'endpoint /exports de l'API v2.1.

    Le filtre `where` utilise la fonction date_heure sur l'intervalle du mois.
    On demande le CSV plutot que le JSON : c'est le format d'origine publie
    par RTE, et le TP demande de conserver le format brut.
    """
    day_start, day_end = month_bounds(year, month)
    url = f"{conf['api_base']}/{conf['dataset_id']}/exports/csv"
    params = {
        "where": f"date_heure >= date'{day_start}' AND date_heure <= date'{day_end}T23:59:59'",
        "delimiter": conf.get("csv_delimiter", ";"),
        "list_separator": ",",
        "quote_all": "false",
        "with_bom": "false",
    }

    for attempt in range(1, 4):
        resp = requests.get(url, params=params, timeout=300, stream=True)
        if resp.status_code == 429:
            wait = 30 * attempt
            log.warning("Quota atteint (429), nouvelle tentative dans %d s.", wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.content

    raise RuntimeError(f"Telechargement impossible pour {year}-{month:02d}")


def sanity_check(payload: bytes, year: int, month: int) -> int:
    """Refus d'un lot vide ou tronque : mieux vaut pas de _SUCCESS qu'un
    _SUCCESS mensonger sur un fichier de 12 octets."""
    if len(payload) < 1024:
        raise ValueError(
            f"Lot {year}-{month:02d} suspect : {len(payload)} octets seulement."
        )
    lines = payload.count(b"\n")
    # 4 points par heure * 24 h * 28 jours minimum = 2688 lignes + entete
    if lines < 2000:
        raise ValueError(
            f"Lot {year}-{month:02d} incomplet : {lines} lignes (attendu >= 2000)."
        )
    return lines


def ingest_month(client, layout: Layout, conf: dict, year: int, month: int,
                 force: bool) -> str:
    part = layout.bronze_batch(SOURCE, year, month)

    with BronzeWriter(client, part) as w:
        if not force and w.skip_if_done():
            return "skipped"

        log.info("Telechargement %04d-%02d ...", year, month)
        payload = download_month(conf, year, month)
        lines = sanity_check(payload, year, month)

        filename = f"eco2mix_national_{year:04d}_{month:02d}.csv"
        w.write_bytes(filename, payload)

        w.commit({
            "source": SOURCE,
            "dataset_id": conf["dataset_id"],
            "quality": conf["quality"],
            "zone_id": conf["zone_id"],
            "year": year,
            "month": month,
            "file": filename,
            "bytes": len(payload),
            "lines": lines,
            "format": "csv",
            "delimiter": conf.get("csv_delimiter", ";"),
        })
        return "ingested"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True, help="AAAA-MM-JJ")
    ap.add_argument("--end", required=True, help="AAAA-MM-JJ")
    ap.add_argument("--conf", default=None)
    ap.add_argument("--force", action="store_true",
                    help="reingere meme si _SUCCESS present")
    args = ap.parse_args()

    cfg = load_config(args.conf)
    conf = source_conf(SOURCE, args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])
    client = hdfs_client(args.conf)

    months = partition_months(args.start, args.end)
    log.info("%d partition(s) mensuelle(s) a traiter.", len(months))

    stats = {"ingested": 0, "skipped": 0, "failed": 0}
    for year, month in months:
        try:
            stats[ingest_month(client, layout, conf, year, month, args.force)] += 1
        except Exception as exc:  # noqa: BLE001
            log.error("Echec %04d-%02d : %s", year, month, exc)
            stats["failed"] += 1

    log.info("Bilan : %(ingested)d ingere(s), %(skipped)d ignore(s), "
             "%(failed)d en echec.", stats)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
