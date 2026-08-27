#!/usr/bin/env python3
"""
Bonus ML : prevision de la consommation electrique a H+24.

L'argument fort de ce modele : la prevision J-1 de RTE est DEJA dans les
donnees. On dispose donc d'un benchmark professionnel gratuit. Un modele
qui bat RTE serait suspect ; un modele qui s'en approche est un bon
resultat, et le dire honnetement vaut mieux que d'annoncer un R2 de 0.99
obtenu par fuite de donnees.

DEUX PIEGES EVITES, a mentionner en soutenance :

1. PAS DE SPLIT ALEATOIRE. Sur une serie temporelle, un train_test_split
   aleatoire laisse le modele voir le futur : les lags d'une ligne de test
   apparaissent dans le train. Le score explose et ne veut rien dire. On
   coupe donc chronologiquement.

2. PAS DE consumption_mw BRUTE COMME FEATURE. La cible est la consommation
   a H+24 ; utiliser la consommation courante est legitime (on la connait),
   mais toute agregation qui melangerait des valeurs futures serait une
   fuite. Les lags sont tous strictement passes.

    python train_forecast.py --out ./models
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, "/opt/datalake/src")
from common.config import Layout, load_config  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-7s [ml] %(message)s")
log = logging.getLogger(__name__)

FEATURES = [
    "consumption_mw", "lag_24h", "lag_48h", "lag_168h",
    "roll_mean_24h", "roll_std_24h",
    "hour_sin", "hour_cos", "dow", "month_of_year",
    "is_weekend", "is_holiday",
    "temperature_c", "hdd", "cdd", "wind_speed_ms", "cloud_cover_pct",
]
TARGET = "target_consumption_h24"


def load_features(cfg: dict, layout: Layout) -> pd.DataFrame:
    """Lecture directe du Parquet Gold depuis HDFS via pyarrow."""
    path = layout.gold("ml_features")
    host = cfg["hdfs"]["fs_uri"].replace("hdfs://", "").split(":")[0]
    port = int(cfg["hdfs"]["fs_uri"].rsplit(":", 1)[1])

    try:
        import pyarrow.fs as pafs
        fs = pafs.HadoopFileSystem(host=host, port=port)
        df = pd.read_parquet(path, filesystem=fs)
    except Exception as exc:  # noqa: BLE001
        log.warning("HDFS via pyarrow indisponible (%s), essai en local.", exc)
        df = pd.read_parquet(path.lstrip("/"))

    return df.sort_values("ts_utc").reset_index(drop=True)


def metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    err = y_pred - y_true
    return {
        "mae_mw": float(np.mean(np.abs(err))),
        "rmse_mw": float(np.sqrt(np.mean(err ** 2))),
        "mape_pct": float(np.mean(np.abs(err / y_true)) * 100),
        "bias_mw": float(np.mean(err)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--conf", default=None)
    ap.add_argument("--out", default="./models")
    ap.add_argument("--test-frac", type=float, default=0.2)
    args = ap.parse_args()

    cfg = load_config(args.conf)
    layout = Layout(root=cfg["hdfs"]["root"])

    df = load_features(cfg, layout)
    log.info("%d lignes, du %s au %s", len(df), df.ts_utc.min(), df.ts_utc.max())

    feats = [f for f in FEATURES if f in df.columns]
    missing = set(FEATURES) - set(feats)
    if missing:
        log.warning("Features absentes, ignorees : %s", sorted(missing))

    data = df.dropna(subset=feats + [TARGET])
    log.info("%d lignes exploitables apres suppression des NaN.", len(data))
    if len(data) < 200:
        log.error("Trop peu de donnees pour entrainer. Ingerer plus de mois.")
        return 1

    # --- Split CHRONOLOGIQUE, jamais aleatoire ---------------------------
    cut = int(len(data) * (1 - args.test_frac))
    train, test = data.iloc[:cut], data.iloc[cut:]
    log.info("Train : %s -> %s (%d lignes)",
             train.ts_utc.min(), train.ts_utc.max(), len(train))
    log.info("Test  : %s -> %s (%d lignes)",
             test.ts_utc.min(), test.ts_utc.max(), len(test))

    X_tr, y_tr = train[feats].values, train[TARGET].values
    X_te, y_te = test[feats].values, test[TARGET].values

    results: dict[str, dict] = {}

    # --- Baseline 1 : persistance (conso d'il y a 24 h) ------------------
    if "lag_24h" in test.columns:
        results["baseline_persistance"] = metrics(y_te, test["lag_24h"].values)

    # --- Baseline 2 : la prevision J-1 de RTE ----------------------------
    if "rte_forecast_j1_mw" in test.columns:
        mask = test["rte_forecast_j1_mw"].notna()
        if mask.sum() > 10:
            results["benchmark_rte_j1"] = metrics(
                y_te[mask.values], test.loc[mask, "rte_forecast_j1_mw"].values)

    # --- Modeles ---------------------------------------------------------
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler().fit(X_tr)
    ridge = Ridge(alpha=1.0).fit(scaler.transform(X_tr), y_tr)
    results["ridge"] = metrics(y_te, ridge.predict(scaler.transform(X_te)))

    gbr = GradientBoostingRegressor(
        n_estimators=300, max_depth=5, learning_rate=0.05,
        subsample=0.9, random_state=42,
    ).fit(X_tr, y_tr)
    pred = gbr.predict(X_te)
    results["gradient_boosting"] = metrics(y_te, pred)

    # --- Restitution -----------------------------------------------------
    print("\n" + "=" * 66)
    print(f"{'Modele':<26}{'MAE (MW)':>12}{'RMSE (MW)':>12}{'MAPE (%)':>12}")
    print("-" * 66)
    for name, m in sorted(results.items(), key=lambda kv: kv[1]["mae_mw"]):
        print(f"{name:<26}{m['mae_mw']:>12.0f}{m['rmse_mw']:>12.0f}"
              f"{m['mape_pct']:>12.2f}")
    print("=" * 66)

    if "benchmark_rte_j1" in results:
        ratio = results["gradient_boosting"]["mae_mw"] / results["benchmark_rte_j1"]["mae_mw"]
        verdict = ("le modele bat RTE, verifier une fuite de donnees"
                   if ratio < 1 else
                   f"RTE reste {1/ratio:.0%} meilleur, ce qui est attendu")
        print(f"\nRapport au benchmark RTE : {ratio:.2f}x  ({verdict})")

    imp = sorted(zip(feats, gbr.feature_importances_),
                 key=lambda kv: -kv[1])[:8]
    print("\nFeatures les plus importantes :")
    for name, val in imp:
        print(f"  {name:<22} {val:.3f}  {'#' * int(val * 60)}")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "metrics.json").write_text(
        json.dumps({"results": results,
                    "features": feats,
                    "n_train": len(train), "n_test": len(test),
                    "importances": dict(imp)},
                   indent=2), encoding="utf-8")

    try:
        import joblib
        joblib.dump({"model": gbr, "features": feats}, out / "gbr_h24.joblib")
        log.info("Modele sauvegarde dans %s", out)
    except ImportError:
        log.warning("joblib absent, metriques seules sauvegardees.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
