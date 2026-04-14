#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
utils.py — wspolne funkcje normalizacji, parsowania i konfiguracji dla Price-Bot.

Centralizuje zduplikowane funkcje z automat1.py, manual.py i selektor_csv.py.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

_CONFIG_CACHE: dict | None = None


def load_config(path: Path | None = None) -> dict:
    """Wczytuje config.json i cachuje wynik. Zwraca domyslne wartosci jesli plik nie istnieje."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    if path is None:
        path = Path(__file__).resolve().parent / "config.json"

    defaults = {
        "PRICE_FLOOR_PLN_M2": 2000.0,
        "PRICE_CEIL_PLN_M2": 40000.0,
        "POP_MARGIN_RULES": [
            [0, 6000, 20.0, 10.0],
            [6000, 20000, 15.0, 10.0],
            [20000, 50000, 12.0, 10.0],
            [50000, 200000, 10.0, 8.0],
            [200000, None, 8.0, 5.0],
        ],
        "DELAY_MIN": 4.0,
        "DELAY_MAX": 6.0,
        "RETRIES": 3,
        "SOFT_STOP_MORE": 10,
        "BDL_API_KEY_DEFAULT": "c804c054-f519-45b3-38f3-08de375a07dc",
    }

    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as f:
                user_cfg = json.load(f)
            defaults.update(user_cfg)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Nie udalo sie wczytac config.json: %s", e)

    _CONFIG_CACHE = defaults
    return _CONFIG_CACHE


def reset_config_cache() -> None:
    """Resetuje cache konfiguracji (przydatne w testach)."""
    global _CONFIG_CACHE
    _CONFIG_CACHE = None


# ---------------------------------------------------------------------------
# Normalizacja tekstu
# ---------------------------------------------------------------------------

def norm(s: str) -> str:
    """Normalizacja do porownywania kolumn: lowercase, bez spacji/tabow."""
    return (s or "").strip().lower().replace(" ", "").replace("\xa0", "").replace("\t", "")


def plain(x) -> str:
    """Usun diakrytyki, lowercase, znormalizuj spacje."""
    if x is None:
        return ""
    try:
        if isinstance(x, float) and np.isnan(x):
            return ""
    except (ValueError, TypeError):
        pass

    s = str(x).strip().lower()
    # ł/Ł nie rozkłada się w NFKD — zamieniamy ręcznie
    s = s.replace("ł", "l").replace("Ł", "l")
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = " ".join(s.split())
    return s


def strip_parentheses(s: str) -> str:
    """Usun nawiasy i ich zawartosc."""
    return re.sub(r"\([^)]*\)", " ", s).strip()


def canon_admin(part: str, kind: str) -> str:
    """Kanonizacja nazw jednostek administracyjnych.

    kind: woj/pow/gmi/mia/dzl
    Ujednolica teksty: usuwa nawiasy, interpunkcje, tokeny typu 'powiat', 'gmina' itd.
    """
    s = plain(part)
    if not s:
        return ""
    s = strip_parentheses(s)

    s = s.replace("-", " ").replace("/", " ")
    s = re.sub(r"[^0-9a-z ]+", " ", s)
    s = " ".join(s.split())

    drop_common = {
        "woj", "woj.", "wojewodztwo",
        "pow", "pow.", "powiat",
        "gmina", "gm", "gm.",
        "miasto", "m", "m.",
        "osiedle", "dzielnica",
        "miejska", "wiejska", "miejskowiejska", "miejsko", "wiejsko",
        "na", "prawach", "powiatu",
    }

    tokens = [t for t in s.split() if t not in drop_common]

    if not tokens:
        tokens = s.split()

    return " ".join(tokens).strip()


# ---------------------------------------------------------------------------
# Wyszukiwanie kolumn
# ---------------------------------------------------------------------------

def find_col(cols: Sequence[str], candidates: Sequence[str]) -> str | None:
    """Zwroc istniejaca kolumne dopasowana do listy kandydatow (po normalizacji / zawieraniu)."""
    norm_map = {norm(c): c for c in cols}
    for cand in candidates:
        key = norm(cand)
        if key in norm_map:
            return norm_map[key]
    for c in cols:
        if any(norm(x) in norm(c) for x in candidates):
            return c
    return None


# ---------------------------------------------------------------------------
# Parsowanie wartosci
# ---------------------------------------------------------------------------

def trim_after_semicolon(val) -> str:
    """Obcina tekst po sredniku."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (ValueError, TypeError):
        pass
    s = str(val)
    if ";" in s:
        s = s.split(";", 1)[0]
    return s.strip()


def to_float_maybe(x) -> Optional[float]:
    """Parsuje liczby typu '101,62 m2', '52 m2', '11 999 zl/m2' itd."""
    if x is None:
        return None
    try:
        if isinstance(x, float) and np.isnan(x):
            return None
    except (ValueError, TypeError):
        pass

    try:
        if pd.isna(x):
            return None
    except (ValueError, TypeError):
        pass

    s = str(x)
    for unit in ["m²", "m2", "zł/m²", "zł/m2", "zł"]:
        s = s.replace(unit, "")
    s = s.replace(" ", "").replace("\xa0", "").replace(",", ".")
    s = "".join(ch for ch in s if (ch.isdigit() or ch == "." or ch == "-"))
    try:
        return float(s) if s else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Filtrowanie outlierow
# ---------------------------------------------------------------------------

def filter_outliers_df(df: pd.DataFrame, price_col: str, config: dict | None = None):
    """Filtruje outlier'y cenowe: absolutne limity + percentyle 10-90.

    Zwraca (df_filtered, prices_array).
    """
    if config is None:
        config = load_config()

    price_floor = config.get("PRICE_FLOOR_PLN_M2", 2000.0)
    price_ceil = config.get("PRICE_CEIL_PLN_M2", 40000.0)

    if df is None or len(df.index) == 0:
        return df, np.array([], dtype=float)

    prices_all = df[price_col].astype(float).replace([np.inf, -np.inf], np.nan)
    valid = prices_all.dropna()
    n = int(len(valid))

    if n <= 2:
        return df, valid.to_numpy(dtype=float)

    mask_abs = (prices_all >= price_floor) & (prices_all <= price_ceil)
    df_abs = df[mask_abs].copy()
    valid_abs = df_abs[price_col].astype(float).replace([np.inf, -np.inf], np.nan).dropna()

    if len(valid_abs) < 2:
        df_abs = df.copy()
        valid_abs = valid.copy()

    p10 = np.nanpercentile(valid_abs, 10)
    p90 = np.nanpercentile(valid_abs, 90)

    prices_abs = df_abs[price_col].astype(float).replace([np.inf, -np.inf], np.nan)
    mask_pct = (prices_abs >= p10) & (prices_abs <= p90)
    df2 = df_abs[mask_pct].copy()
    prices2 = df2[price_col].astype(float).replace([np.inf, -np.inf], np.nan).dropna()

    if len(prices2) < 2:
        df2 = df_abs.copy()
        prices2 = valid_abs.copy()

    return df2, prices2.to_numpy(dtype=float)


# ---------------------------------------------------------------------------
# Progi ludnosci
# ---------------------------------------------------------------------------

def get_pop_margin_rules(config: dict | None = None) -> list:
    """Zwraca POP_MARGIN_RULES z konfiguracji."""
    if config is None:
        config = load_config()
    return config.get("POP_MARGIN_RULES", [
        [0, 6000, 20.0, 10.0],
        [6000, 20000, 15.0, 10.0],
        [20000, 50000, 12.0, 10.0],
        [50000, 200000, 10.0, 8.0],
        [200000, None, 8.0, 5.0],
    ])


def rules_for_population(pop, config: dict | None = None) -> tuple[float, float]:
    """Zwraca (margin_m2, margin_pct) wg POP_MARGIN_RULES."""
    rules = get_pop_margin_rules(config)
    if pop is None:
        return float(rules[-1][2]), float(rules[-1][3])
    try:
        p = float(pop)
    except (ValueError, TypeError):
        return float(rules[-1][2]), float(rules[-1][3])

    for low, high, m2, pct in rules:
        if p >= low and (high is None or p < high):
            return float(m2), float(pct)
    return float(rules[-1][2]), float(rules[-1][3])


def bucket_for_population(pop: float | None, config: dict | None = None) -> tuple[float | None, float | None]:
    """Zwraca (bucket_low, bucket_high) wg POP_MARGIN_RULES."""
    rules = get_pop_margin_rules(config)
    if pop is None:
        return (None, None)
    try:
        p = float(pop)
    except (ValueError, TypeError):
        return (None, None)

    for low, high, _, _ in rules:
        if p >= low and (high is None or p < high):
            return (float(low), float(high) if high is not None else None)

    low, high, _, _ = rules[-1]
    return (float(low), float(high) if high is not None else None)


# ---------------------------------------------------------------------------
# Konfiguracja logowania
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path | None = None, level: int = logging.INFO) -> None:
    """Konfiguruje logowanie do pliku i konsoli."""
    handlers = [logging.StreamHandler()]

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "pricebot.log"
        handlers.append(logging.FileHandler(str(log_file), encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )
