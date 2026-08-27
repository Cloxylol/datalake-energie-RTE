#!/usr/bin/env python3
"""
Ingestion batch : prix spot day-ahead ENTSO-E (XML) -> Bronze.

Troisieme format, et le plus interessant pour justifier Silver : du XML
imbrique (Publication_MarketDocument > TimeSeries > Period > Point) qu'il
faudra aplatir, la ou les deux autres sources sont deja tabulaires ou
semi-tabulaires.

SOURCE OPTIONNELLE : elle exige un token gratuit obtenu en ecrivant a
transparency@entsoe.eu depuis un compte cree sur le portail. Si
ENTSOE_TOKEN est absent, le script sort en code 0 avec un avertissement
pour ne pas faire echouer le DAG. Le pipeline reste valide sans, avec deux
sources au lieu de trois.

Usage :
    export ENTSOE_TOKEN=...
    python batch_entsoe.py --start 2024-01-01 --end 2024-03-31
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from calendar import monthrange
from xml.etree import ElementTree as ET

import requests

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, hdfs_client, load_config, source_conf  # noqa: E402
from common.hdfs_io import BronzeWriter, partition_months  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [entsoe] %(message)s",
)
log = logging.getLogger(__name__)

SOURCE = "entsoe_prices"


def period_bounds(year: int, month: int) -> tuple[str, str]:
    """Format ENTSO-E : AAAAMMJJHHMM en UTC, borne de fin exclusive."""
    last = monthrange(year, month)[1]
    start = f"{year:04d}{month:02d}010000"
    end = f"{year:04d}{month:02d}{last:02d}2300"
    return start, end


def download(conf: dict, token: str, year: int, month: int) -> bytes:
    start, end = period_bounds(year, month)
    params = {
        "securityToken": token,
        "documentType": conf["document_type"],
        "in_Domain": conf["domain"],
        "out_Domain": conf["domain"],
        "periodStart": start,
        "periodEnd": end,
    }

    for attempt in range(1, 4):
        resp = requests.get(conf["api_base"], params=params, timeout=180)

        if resp.status_code == 429:
            wait = 60 * attempt
            log.warning("429 ENTSO-E, attente %d s.", wait)
            time.sleep(wait)
            continue

        # ENTSO-E renvoie 400 avec un Acknowledgement_MarketDocument quand
        # l'intervalle est vide. Ce n'est pas une panne, mais ce n'est pas
        # un lot valide non plus : on remonte l'erreur sans poser _SUCCESS.
        if resp.status_code == 400 and b"Acknowledgement" in resp.content:
            reason = extract_reason(resp.content)
            raise ValueError(f"ENTSO-E refuse l'intervalle : {reason}")

        resp.raise_for_status()
        return resp.content

    raise RuntimeError(f"Echec ENTSO-E {year}-{month:02d}")


def extract_reason(payload: bytes) -> str:
    try:
        root = ET.fromstring(payload)
        texts = [
            el.text for el in root.iter()
            if el.tag.endswith("text") and el.text
        ]
        return texts[0] if texts else "motif non precise"
    except ET.ParseError:
        return "reponse illisible"


def sanity_check(payload: bytes, year: int, month: int) -> int:
    """Compte les Point du document. Un mois complet en horaire fait ~720."""
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise ValueError(f"XML invalide : {exc}") from exc

    points = [el for el in root.iter() if el.tag.endswith("}Point")]
    if len(points) < 500:
        raise ValueError(
            f"Lot {year}-{month:02d} incomplet : {len(points)} points (attendu ~720)."
        )
    return len(points)


def ingest_month(client, layout: Layout, conf: dict, token: str, year: int,
                 month: int, force: bool) -> str:
    part = layout.bronze_batch(SOURCE, year, month, zone="FR")

    with BronzeWriter(client, part) as w:
        if not force and w.skip_if_done():
            return "skipped"

        payload = download(conf, token, year, month)
        n_points = sanity_check(payload, year, month)

        filename = f"entsoe_dayahead_FR_{year:04d}_{month:02d}.xml"
        w.write_bytes(filename, payload)

        w.commit({
            "source": SOURCE,
            "quality": conf["quality"],
            "zone_id": "fr",
            "domain": conf["domain"],
            "document_type": conf["document_type"],
            "year": year,
            "month": month,
            "file": filename,
            "bytes": len(payload),
            "points": n_points,
            "format": "xml",
        })
        log.info("%04d-%02d : %d points de prix.", year, month, n_points)
        return "ingested"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--conf", default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.conf)
    conf = source_conf("entsoe", args.conf)
    token = os.environ.get(conf["token_env"], "").strip()

    if not token:
        log.warning(
            "%s absent : source optionnelle ignoree. Le pipeline reste valide "
            "avec eco2mix + open-meteo. Voir README pour obtenir un token.",
            conf["token_env"],
        )
        return 0

    layout = Layout(root=cfg["hdfs"]["root"])
    client = hdfs_client(args.conf)
    months = partition_months(args.start, args.end)

    stats = {"ingested": 0, "skipped": 0, "failed": 0}
    for year, month in months:
        try:
            stats[ingest_month(client, layout, conf, token, year, month,
                               args.force)] += 1
        except Exception as exc:  # noqa: BLE001
            log.error("Echec %04d-%02d : %s", year, month, exc)
            stats["failed"] += 1
        time.sleep(1)

    log.info("Bilan : %(ingested)d ingere(s), %(skipped)d ignore(s), "
             "%(failed)d en echec.", stats)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
