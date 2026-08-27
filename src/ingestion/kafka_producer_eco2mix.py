#!/usr/bin/env python3
"""
Producteur Kafka : polling de l'API eco2mix-national-tr -> topic Kafka.

Deux points de conception qui comptent pour la note :

1. CLE DE PARTITION = date_heure.
   Kafka garantit l'ordre au sein d'une partition et la compaction se fait
   par cle. En prenant l'horodatage metier comme cle, deux publications du
   meme quart d'heure (inevitable : on poll toutes les 2 min une API qui se
   rafraichit toutes les 15 min) atterrissent dans la meme partition et sont
   deduplicables en aval de facon deterministe.

2. FENETRE GLISSANTE COTE PRODUCTEUR.
   On ne republie pas ce qu'on vient de publier : un cache des N derniers
   date_heure vus evite de saturer le topic. Ce n'est PAS la garantie
   d'unicite (un redemarrage vide le cache) : la vraie dedup est en Silver.
   Ici c'est juste de l'hygiene de debit.

Usage :
    python kafka_producer_eco2mix.py                 # boucle infinie
    python kafka_producer_eco2mix.py --once          # un seul poll (tests)
    python kafka_producer_eco2mix.py --max-polls 5
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from collections import OrderedDict
from typing import Any, Iterator

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, "/opt/datalake/src")
from common.config import load_config, source_conf  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s [producer] %(message)s",
)
log = logging.getLogger(__name__)

_STOP = False


def _handle_signal(signum, frame) -> None:
    """Arret propre : on finit le flush en cours avant de sortir."""
    global _STOP
    log.info("Signal %s recu, arret apres le poll courant.", signum)
    _STOP = True


class Eco2mixPoller:
    """Interroge l'API Opendatasoft v2.1 et rend les records les plus recents."""

    def __init__(self, conf: dict[str, Any]):
        self.url = f"{conf['api_base']}/{conf['dataset_id']}/records"
        self.page_size = conf.get("page_size", 100)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "tp-datalake-energie/1.0"})

    def fetch_latest(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Recupere les derniers records, tries du plus recent au plus ancien.

        L'API v2.1 plafonne limit a 100 par appel. On pagine si besoin.
        """
        target = limit or self.page_size
        collected: list[dict[str, Any]] = []
        offset = 0

        while len(collected) < target:
            batch_size = min(100, target - len(collected))
            params = {
                "limit": batch_size,
                "offset": offset,
                "order_by": "date_heure desc",
                # On ecarte les lignes futures sans mesure : l'API publie les
                # quarts d'heure de la journee avec consommation nulle.
                "where": "consommation IS NOT NULL",
            }
            resp = self.session.get(self.url, params=params, timeout=30)

            if resp.status_code == 429:
                log.warning("Quota API atteint (429), pause 60 s.")
                time.sleep(60)
                continue
            resp.raise_for_status()

            payload = resp.json()
            results = payload.get("results", [])
            if not results:
                break

            collected.extend(results)
            offset += batch_size
            if len(results) < batch_size:
                break

        return collected


class SeenCache:
    """Cache LRU borne des horodatages deja publies."""

    def __init__(self, maxsize: int = 500):
        self.maxsize = maxsize
        self._d: OrderedDict[str, None] = OrderedDict()

    def add_if_new(self, key: str) -> bool:
        if key in self._d:
            self._d.move_to_end(key)
            return False
        self._d[key] = None
        if len(self._d) > self.maxsize:
            self._d.popitem(last=False)
        return True


def build_producer(cfg: dict[str, Any]) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=cfg["kafka"]["bootstrap_servers"].split(","),
        # Enveloppe JSON : Bronze conserve le format brut d'origine.
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(),
        key_serializer=lambda k: k.encode() if k else None,
        acks="all",              # pas de perte silencieuse
        retries=5,
        linger_ms=200,           # petit batching, le debit est faible
        compression_type="gzip",
        max_in_flight_requests_per_connection=1,  # preserve l'ordre
    )


def envelope(record: dict[str, Any], source: str, quality: str) -> dict[str, Any]:
    """Enveloppe le record brut avec ses metadonnees de tracabilite.

    Le payload d'origine n'est jamais modifie : Bronze doit rester fidele.
    Les metadonnees vivent a cote, dans _meta.
    """
    return {
        "_meta": {
            "source": source,
            "quality": quality,
            "ingested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "producer": "kafka_producer_eco2mix",
        },
        "payload": record,
    }


def poll_once(
    poller: Eco2mixPoller,
    producer: KafkaProducer,
    topic: str,
    cache: SeenCache,
    conf: dict[str, Any],
    limit: int | None = None,
) -> int:
    """Un cycle de poll. Retourne le nombre de messages publies."""
    try:
        records = poller.fetch_latest(limit)
    except requests.RequestException as exc:
        log.error("Appel API en echec : %s", exc)
        return 0

    published = 0
    for rec in records:
        key = rec.get("date_heure")
        if not key:
            log.warning("Record sans date_heure ignore : %s", list(rec)[:5])
            continue
        if not cache.add_if_new(key):
            continue

        msg = envelope(rec, source="eco2mix_tr", quality=conf["quality"])
        try:
            producer.send(topic, key=key, value=msg)
            published += 1
        except KafkaError as exc:
            log.error("Envoi Kafka en echec pour %s : %s", key, exc)

    if published:
        producer.flush(timeout=30)
        newest = max(r["date_heure"] for r in records if r.get("date_heure"))
        log.info(
            "%d message(s) publie(s) sur %s (plus recent : %s)",
            published, topic, newest,
        )
    else:
        log.info("Rien de nouveau (%d record(s) deja vus).", len(records))

    return published


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conf", default=None, help="chemin de sources.yml")
    ap.add_argument("--once", action="store_true", help="un seul poll puis sortie")
    ap.add_argument("--max-polls", type=int, default=0, help="0 = infini")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    cfg = load_config(args.conf)
    conf = source_conf("eco2mix_tr", args.conf)
    topic = cfg["kafka"]["topics"]["eco2mix_tr"]
    interval = conf["poll_interval_s"]

    poller = Eco2mixPoller(conf)
    producer = build_producer(cfg)
    cache = SeenCache(maxsize=conf.get("lookback_records", 200) * 3)

    # Premier poll plus large : on amorce avec un historique de recouvrement
    # pour ne pas laisser de trou si le producteur a ete arrete un moment.
    log.info("Amorcage sur %d records.", conf["lookback_records"])
    poll_once(poller, producer, topic, cache, conf, conf["lookback_records"])

    if args.once:
        producer.close(timeout=10)
        return 0

    polls = 1
    while not _STOP:
        if args.max_polls and polls >= args.max_polls:
            log.info("max-polls atteint.")
            break
        for _ in range(interval):
            if _STOP:
                break
            time.sleep(1)
        if _STOP:
            break
        poll_once(poller, producer, topic, cache, conf)
        polls += 1

    log.info("Fermeture du producteur.")
    producer.close(timeout=30)
    return 0


if __name__ == "__main__":
    sys.exit(main())
