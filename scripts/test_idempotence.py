#!/usr/bin/env python3
"""
Test de la garantie d'idempotence sans cluster HDFS.

Simule le client WebHDFS en memoire et rejoue les scenarios que le
correcteur va tester : double execution, interruption en plein lot,
relance apres interruption.

    python scripts/test_idempotence.py
"""

from __future__ import annotations

import io
import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.hdfs_io import BronzeWriter  # noqa: E402


class FakeHDFS:
    """Client WebHDFS en memoire, API minimale mais fidele."""

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.write_count = 0

    def status(self, path, strict=True):
        if path in self.files:
            return {"length": len(self.files[path])}
        if strict:
            raise FileNotFoundError(path)
        return None

    def makedirs(self, path):
        pass

    def delete(self, path, recursive=False):
        self.files.pop(path, None)

    def rename(self, src, dst):
        if dst in self.files:
            raise FileExistsError(dst)
        self.files[dst] = self.files.pop(src)

    @contextmanager
    def write(self, path, overwrite=False, encoding=None):
        if path in self.files and not overwrite:
            raise FileExistsError(path)
        buf = io.StringIO() if encoding else io.BytesIO()
        yield buf
        val = buf.getvalue()
        self.files[path] = val.encode(encoding) if encoding else val
        self.write_count += 1


PART = "/datalake/bronze/eco2mix_cons/year=2024/month=03"
PAYLOAD = b"date_heure;consommation\n2024-03-01T00:00:00+01:00;54000\n" * 100


def check(label: str, condition: bool) -> bool:
    print(f"  {'OK  ' if condition else 'ECHEC'} {label}")
    return condition


def scenario_premier_run(fs) -> bool:
    print("\n[1] Premiere ingestion")
    with BronzeWriter(fs, PART) as w:
        assert not w.skip_if_done()
        w.write_bytes("data.csv", PAYLOAD)
        w.commit({"lines": 100})

    ok = check("le fichier est ecrit", f"{PART}/data.csv" in fs.files)
    ok &= check("_SUCCESS est pose", f"{PART}/_SUCCESS" in fs.files)
    ok &= check("aucun .part residuel",
                not any(".part" in p for p in fs.files))
    return ok


def scenario_double_run(fs) -> bool:
    print("\n[2] Relance du meme lot (le cas du DAG rejoue)")
    before = fs.write_count
    with BronzeWriter(fs, PART) as w:
        skipped = w.skip_if_done()

    ok = check("le lot est detecte comme deja fait", skipped)
    ok &= check("aucune ecriture supplementaire", fs.write_count == before)
    ok &= check("un seul fichier de donnees",
                sum(1 for p in fs.files if p.endswith("data.csv")) == 1)
    return ok


def scenario_interruption(fs) -> bool:
    print("\n[3] Interruption en plein lot (kill du worker)")
    part2 = "/datalake/bronze/eco2mix_cons/year=2024/month=04"

    try:
        with BronzeWriter(fs, part2) as w:
            w.skip_if_done()
            w.write_bytes("data.csv", PAYLOAD)
            raise KeyboardInterrupt("worker tue avant commit")
    except KeyboardInterrupt:
        pass

    ok = check("les donnees sont la mais orphelines",
               f"{part2}/data.csv" in fs.files)
    ok &= check("_SUCCESS n'est PAS pose",
                f"{part2}/_SUCCESS" not in fs.files)

    print("\n[4] Relance apres interruption")
    with BronzeWriter(fs, part2) as w:
        skipped = w.skip_if_done()
        ok2 = check("le lot n'est PAS considere comme fait", not skipped)
        w.write_bytes("data.csv", PAYLOAD)
        w.commit({"lines": 100})

    ok &= ok2
    ok &= check("_SUCCESS pose apres relance", f"{part2}/_SUCCESS" in fs.files)
    ok &= check("toujours un seul fichier de donnees",
                sum(1 for p in fs.files if p == f"{part2}/data.csv") == 1)
    ok &= check("contenu non duplique",
                len(fs.files[f"{part2}/data.csv"]) == len(PAYLOAD))
    return ok


def scenario_echec_validation(fs) -> bool:
    print("\n[5] Lot rejete par le controle de coherence")
    part3 = "/datalake/bronze/eco2mix_cons/year=2024/month=05"

    try:
        with BronzeWriter(fs, part3) as w:
            w.skip_if_done()
            payload = b"tronque"
            if len(payload) < 1024:
                raise ValueError("lot suspect : 7 octets")
            w.write_bytes("data.csv", payload)
            w.commit()
    except ValueError:
        pass

    return check("aucun _SUCCESS mensonger",
                 f"{part3}/_SUCCESS" not in fs.files)


def main() -> int:
    fs = FakeHDFS()
    results = [
        scenario_premier_run(fs),
        scenario_double_run(fs),
        scenario_interruption(fs),
        scenario_echec_validation(fs),
    ]

    print("\n" + "-" * 58)
    print("Etat final du systeme de fichiers :")
    for path in sorted(fs.files):
        print(f"  {len(fs.files[path]):>7} o  {path}")

    print("-" * 58)
    if all(results):
        print("Tous les scenarios passent.")
        return 0
    print("Au moins un scenario a echoue.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
