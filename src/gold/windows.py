"""
Les trois fenetres de docs/decisions.md, en dates pures.

Ce module ne connait pas Spark : il ne manipule que des `date`. C'est
volontaire — le contrat de fenetrage est la partie du job qu'on veut pouvoir
verifier sans cluster, et il se verifie ici en une seconde :

    python src/gold/windows.py

Les trois fenetres, nommees comme dans le doc :

    window_requested  ce que l'appelant demande (DAG ou ligne de commande)
    window_written    window_requested aligne sur l'unite de partition de la
                      table cible : le mois, pour les trois tables Gold
    window_read       ce qu'il faut lire pour calculer juste, derive des
                      features et jamais d'une constante

L'invariant qu'elles servent : *un job n'ecrit jamais une partition qu'il n'a
pas entierement recalculee.* L'ecrasement dynamique remplace la partition
ENTIERE ; ecrire un demi-mois supprime donc l'autre moitie.

Bornes de JOUR INCLUSES aux deux bouts, comme `silver.transform.restrict_window`
et comme `common.hdfs_io.partition_months` : une seule convention dans le depot.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta


def parse_day(value: str | date) -> date:
    """'2024-03-01' -> date(2024, 3, 1). Tolere un timestamp ISO en entree."""
    if isinstance(value, date):
        return value
    y, m, d = (int(p) for p in str(value)[:10].split("-"))
    return date(y, m, d)


def _first_of_month(day: date) -> date:
    return day.replace(day=1)


def _last_of_month(day: date) -> date:
    nxt = day.replace(year=day.year + day.month // 12,
                      month=day.month % 12 + 1, day=1)
    return nxt - timedelta(days=1)


# ---------------------------------------------------------------------------
# Une fenetre
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TimeWindow:
    """Intervalle de jours, bornes incluses."""

    start: date
    end: date

    @classmethod
    def of(cls, start: str | date, end: str | date) -> "TimeWindow":
        win = cls(parse_day(start), parse_day(end))
        if win.end < win.start:
            raise ValueError(f"Fenetre inversee : {win}")
        return win

    @property
    def stop(self) -> date:
        """Borne haute EXCLUSIVE. Sur un timestamp, `< stop` prend le dernier
        jour en entier, ce que `<= end` ne fait pas."""
        return self.end + timedelta(days=1)

    def months(self) -> list[tuple[int, int]]:
        """(annee, mois) couverts, pour elaguer les partitions a la lecture.

        >>> TimeWindow.of("2024-11-20", "2025-01-02").months()
        [(2024, 11), (2024, 12), (2025, 1)]
        """
        out: list[tuple[int, int]] = []
        cur = _first_of_month(self.start)
        while cur <= self.end:
            out.append((cur.year, cur.month))
            cur = _last_of_month(cur) + timedelta(days=1)
        return out

    def shift(self, before_days: int = 0, after_days: int = 0) -> "TimeWindow":
        return TimeWindow(self.start - timedelta(days=before_days),
                          self.end + timedelta(days=after_days))

    def __str__(self) -> str:
        return f"{self.start} -> {self.end}"


# ---------------------------------------------------------------------------
# Ce que les features exigent
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FeatureSpan:
    """La portee temporelle des features, en heures.

    C'est d'ici que sort `window_read`, et de nulle part ailleurs. Declarer
    un lag de 336 h dans `lags` allonge la lecture de lui-meme : il n'y a pas
    de constante « 8 jours » a maintenir en parallele du code des features.
    """

    lags: tuple[int, ...] = ()
    rolling: int = 0
    lead: int = 0

    @property
    def lookback_days(self) -> int:
        """Amont. +1 jour de marge : les lags sont des offsets horaires, la
        fenetre est en jours, et l'heure locale decale la frontiere."""
        hours = max((*self.lags, self.rolling, 0))
        return math.ceil(hours / 24) + 1

    @property
    def lookahead_days(self) -> int:
        """Aval : de quoi construire la cible du dernier point ecrit."""
        return math.ceil(self.lead / 24)


# ---------------------------------------------------------------------------
# Les deux derivations
# ---------------------------------------------------------------------------

def align_to_month(requested: TimeWindow) -> TimeWindow:
    """window_requested -> window_written.

    Le job aligne lui-meme plutot que de faire confiance a son appelant : un
    spark-submit manuel doit etre aussi sur qu'un declenchement Airflow.
    """
    return TimeWindow(_first_of_month(requested.start),
                      _last_of_month(requested.end))


def reading_window(written: TimeWindow, span: FeatureSpan) -> TimeWindow:
    """window_written -> window_read.

    Elargie autour de window_WRITTEN, pas de window_requested : c'est le mois
    entier qu'il faut recalculer, donc le mois entier dont il faut pouvoir
    calculer les lags.
    """
    return written.shift(span.lookback_days, span.lookahead_days)


# ---------------------------------------------------------------------------
# Verification sans Spark
# ---------------------------------------------------------------------------

def _self_test() -> int:
    ok = True

    def check(label: str, got, want) -> None:
        nonlocal ok
        good = got == want
        ok &= good
        print(f"  {'OK   ' if good else 'ECHEC'} {label} : {got}"
              + ("" if good else f"   (attendu {want})"))

    span = FeatureSpan(lags=(24, 48, 168), rolling=24, lead=24)
    check("lookback derive des lags", span.lookback_days, 8)
    check("lookahead derive du lead", span.lookahead_days, 1)
    check("un lag plus long allonge la lecture",
          FeatureSpan(lags=(24, 336), rolling=24, lead=24).lookback_days, 15)

    requested = TimeWindow.of("2024-03-11", "2024-03-14")
    written = align_to_month(requested)
    check("window_written couvre le mois entier",
          (written.start, written.end), (date(2024, 3, 1), date(2024, 3, 31)))
    check("window_written contient window_requested",
          written.start <= requested.start and written.end >= requested.end, True)

    read = reading_window(written, span)
    check("window_read = written -8j / +1j",
          (read.start, read.end), (date(2024, 2, 22), date(2024, 4, 1)))
    check("window_read contient window_written",
          read.start < written.start and read.end > written.end, True)
    check("mois elagues a la lecture", read.months(),
          [(2024, 2), (2024, 3), (2024, 4)])
    check("borne haute exclusive",
          TimeWindow.of("2024-03-01", "2024-03-31").stop, date(2024, 4, 1))

    check("decembre s'aligne sans deborder sur janvier",
          align_to_month(TimeWindow.of("2024-12-05", "2024-12-05")).end,
          date(2024, 12, 31))
    check("fevrier bissextile",
          align_to_month(TimeWindow.of("2024-02-10", "2024-02-10")).end,
          date(2024, 2, 29))
    check("plusieurs mois demandes",
          align_to_month(TimeWindow.of("2024-01-15", "2024-03-02")).months(),
          [(2024, 1), (2024, 2), (2024, 3)])

    print("\nContrat de fenetrage respecte." if ok
          else "\nAu moins une verification a echoue.")
    return 0 if ok else 1


if __name__ == "__main__":
    import sys

    sys.exit(_self_test())
