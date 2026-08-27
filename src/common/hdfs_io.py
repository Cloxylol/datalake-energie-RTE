"""
Ecriture Bronze idempotente via WebHDFS.

Le contrat, qui est le coeur de l'exigence "un DAG interrompu doit pouvoir
etre relance sans dupliquer les donnees" :

  1. On teste _SUCCESS AVANT de telecharger quoi que ce soit.
  2. On ecrit dans un fichier temporaire .part.
  3. On renomme en atomique vers le nom final.
  4. On ecrit _SUCCESS EN DERNIER, jamais avant.

Si le job meurt entre 2 et 4, il n'y a pas de _SUCCESS : la relance
recommence proprement et ecrase le .part orphelin. Un lot n'est donc
jamais compte comme ingere a moitie.
"""

from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger(__name__)


class BronzeWriter:
    """Ecrit un lot batch en Bronze avec marker d'idempotence."""

    def __init__(self, client, partition_path: str):
        self.client = client
        self.path = partition_path.rstrip("/")
        self.marker = f"{self.path}/_SUCCESS"

    # -- Idempotence -------------------------------------------------------

    def already_ingested(self) -> bool:
        """True si le lot est deja present et complet."""
        return self.client.status(self.marker, strict=False) is not None

    def skip_if_done(self) -> bool:
        if self.already_ingested():
            log.info("Lot deja ingere, on passe : %s", self.path)
            return True
        return False

    # -- Ecriture ----------------------------------------------------------

    def write_bytes(self, filename: str, payload: bytes) -> str:
        """Ecrit un fichier brut. Temporaire puis rename atomique."""
        self.client.makedirs(self.path)
        tmp = f"{self.path}/.{filename}.part"
        final = f"{self.path}/{filename}"

        # Un .part orphelin d'un run precedent est ecrase sans etat d'ame.
        with self.client.write(tmp, overwrite=True) as writer:
            writer.write(payload)

        # rename echoue si la cible existe : on nettoie d'abord.
        if self.client.status(final, strict=False) is not None:
            self.client.delete(final)
        self.client.rename(tmp, final)

        log.info("Ecrit %s (%.1f Ko)", final, len(payload) / 1024)
        return final

    def write_text(self, filename: str, text: str) -> str:
        return self.write_bytes(filename, text.encode("utf-8"))

    def write_json(self, filename: str, obj: Any) -> str:
        return self.write_text(
            filename, json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        )

    # -- Cloture -----------------------------------------------------------

    def commit(self, metadata: dict[str, Any] | None = None) -> None:
        """Pose _SUCCESS. A n'appeler qu'apres ecriture complete du lot."""
        body = json.dumps(metadata or {}, ensure_ascii=False, indent=2)
        with self.client.write(self.marker, overwrite=True, encoding="utf-8") as w:
            w.write(body)
        log.info("Lot valide : %s", self.marker)

    # -- Contexte ----------------------------------------------------------

    def __enter__(self) -> "BronzeWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        # Sur exception : pas de commit, donc pas de _SUCCESS. La relance
        # retraitera le lot. On ne masque pas l'erreur.
        if exc_type is not None:
            log.error("Lot %s en echec, _SUCCESS non pose : %s", self.path, exc)
        return False


def partition_months(start: str, end: str) -> list[tuple[int, int]]:
    """Liste des (annee, mois) couvrant l'intervalle, bornes incluses.

    >>> partition_months("2024-11-01", "2025-01-15")
    [(2024, 11), (2024, 12), (2025, 1)]
    """
    from datetime import date

    def parse(s: str) -> date:
        y, m, d = (int(p) for p in s[:10].split("-"))
        return date(y, m, d)

    a, b = parse(start), parse(end)
    if a > b:
        raise ValueError(f"Intervalle invalide : {start} > {end}")

    out: list[tuple[int, int]] = []
    y, m = a.year, a.month
    while (y, m) <= (b.year, b.month):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out
