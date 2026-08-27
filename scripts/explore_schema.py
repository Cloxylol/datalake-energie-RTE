#!/usr/bin/env python3
"""
Decouverte des schemas reels des sources.

A LANCER EN PREMIER, avant d'ecrire le job Silver. Les noms de champs
d'eco2mix evoluent (ajout de stockage_batterie, eolien_offshore, decoupage
par technologie...). Plutot que de les figer dans le code a partir d'une
doc, on les lit sur l'API et on genere le mapping.

    python scripts/explore_schema.py --source eco2mix_tr
    python scripts/explore_schema.py --all --out conf/schema_discovered.json

Le fichier produit sert de reference pour ecrire conf/silver_mapping.yml.
Ce script n'ecrit rien dans le datalake : c'est un outil de developpement.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from common.config import load_config, weather_zones  # noqa: E402

CONF = str(Path(__file__).resolve().parents[1] / "conf" / "sources.yml")


def classify(name: str) -> str:
    """Range un champ eco2mix dans une categorie du modele Silver cible."""
    n = name.lower()
    if n in {"date", "heure", "date_heure"}:
        return "temps"
    if n in {"perimetre", "nature"}:
        return "dimension"
    if n.startswith("prevision"):
        return "prevision"
    if "consommation" in n:
        return "consommation"
    if n.startswith("ech_"):
        return "echange"
    if "taux_co2" in n or n.startswith("co2"):
        return "emission"
    if n.startswith(("tco_", "tch_")):
        return "taux"
    if n.startswith(("stockage", "destockage")):
        return "stockage"
    return "production"


def explore_eco2mix(cfg: dict, key: str) -> dict:
    conf = cfg["sources"][key]
    url = f"{conf['api_base']}/{conf['dataset_id']}/records"
    resp = requests.get(url, params={"limit": 1}, timeout=60)
    resp.raise_for_status()

    results = resp.json().get("results", [])
    if not results:
        return {"dataset": conf["dataset_id"], "error": "aucun record"}

    rec = results[0]
    by_cat: dict[str, list[str]] = {}
    for field, value in rec.items():
        by_cat.setdefault(classify(field), []).append(field)

    return {
        "dataset": conf["dataset_id"],
        "total_fields": len(rec),
        "by_category": {k: sorted(v) for k, v in sorted(by_cat.items())},
        "production_fields": sorted(by_cat.get("production", [])),
        "sample": {k: rec[k] for k in list(rec)[:12]},
    }


def explore_open_meteo(cfg: dict) -> dict:
    conf = cfg["sources"]["open_meteo"]
    zone = weather_zones(CONF)[0]
    resp = requests.get(
        conf["api_base"],
        params={
            "latitude": zone["lat"],
            "longitude": zone["lon"],
            "start_date": "2024-03-01",
            "end_date": "2024-03-02",
            "hourly": ",".join(conf["hourly_vars"]),
            "timezone": conf.get("timezone", "UTC"),
        },
        timeout=60,
    )
    resp.raise_for_status()
    doc = resp.json()
    hourly = doc.get("hourly", {})

    return {
        "top_level_keys": sorted(doc),
        "hourly_arrays": sorted(hourly),
        "units": doc.get("hourly_units", {}),
        "timezone": doc.get("timezone"),
        "utc_offset_seconds": doc.get("utc_offset_seconds"),
        "n_hours": len(hourly.get("time", [])),
        "first_timestamps": hourly.get("time", [])[:3],
        "note": (
            "Structure en tableaux paralleles : hourly.time[i] correspond a "
            "hourly.temperature_2m[i]. Silver devra faire un arrays_zip + "
            "explode pour passer en lignes."
        ),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default=None,
                    choices=["eco2mix_tr", "eco2mix_cons", "open_meteo"])
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    cfg = load_config(CONF)
    out: dict = {}

    targets = (
        ["eco2mix_tr", "eco2mix_cons", "open_meteo"]
        if args.all or not args.source
        else [args.source]
    )

    for t in targets:
        print(f"--- {t} ---", file=sys.stderr)
        try:
            out[t] = (
                explore_open_meteo(cfg) if t == "open_meteo"
                else explore_eco2mix(cfg, t)
            )
        except Exception as exc:  # noqa: BLE001
            out[t] = {"error": str(exc)}
            print(f"    echec : {exc}", file=sys.stderr)

    text = json.dumps(out, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"Ecrit dans {args.out}", file=sys.stderr)
    else:
        print(text)

    # Aide a la redaction du mapping Silver.
    for key in ("eco2mix_tr", "eco2mix_cons"):
        prod = out.get(key, {}).get("production_fields")
        if prod:
            print(
                f"\n# Filieres detectees dans {key}, a reporter dans "
                f"conf/silver_mapping.yml :\n"
                + "\n".join(f"  - {f}" for f in prod),
                file=sys.stderr,
            )
            break

    return 0


if __name__ == "__main__":
    sys.exit(main())
