#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

"""
manual.py — reczne liczenie (GUI: Selektor CSV).

Algorytm doboru identyczny z automat1.py (_process_row):
- dynamiczne kroki marginesu m2 (co 3m2 az do max)
- etapy: pow+gmi+miasto -> gmi+miasto -> pow+miasto -> miasto -> gmi(pop) -> pow(pop) -> woj(pop)
- preferencja dzielnicy na kazdym etapie
- klasyfikacja lokalizacji (warsaw_city, voiv_capital, warsaw_aglo, normal)
- zaokraglanie do 2 miejsc po przecinku

Roznice vs automat:
- zapisuje plik (Nr KW).xlsx z wybranymi ofertami
- zwraca slownik z detalami
- wymagane minimum adresu: woj + miejscowosc (nie woj+pow+gmi+mia)
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from utils import (
    canon_admin as _canon_admin,
    find_col as _find_col,
    filter_outliers_df as _filter_outliers_df,
    load_config,
    to_float_maybe as _to_float_maybe,
    trim_after_semicolon as _trim_after_semicolon,
)

logger = logging.getLogger(__name__)


# =========================
# Bledy
# =========================

class ManualUserError(RuntimeError):
    """Blad, ktory uzytkownik moze naprawic (brak plikow/kolumn, itp.)."""


# =========================
# Progi ludnosci (jak w automacie)
# =========================

_cfg = load_config()
POP_MARGIN_RULES: List[Tuple[int, Optional[int], float, float]] = [
    tuple(r) for r in _cfg.get("POP_MARGIN_RULES", [
        [0, 6000, 20.0, 10.0],
        [6000, 20000, 15.0, 10.0],
        [20000, 50000, 12.0, 10.0],
        [50000, 200000, 10.0, 8.0],
        [200000, None, 8.0, 5.0],
    ])
]


def rules_for_population(pop: float) -> Tuple[float, float]:
    """Zwraca (margin_m2, margin_pct) wg POP_MARGIN_RULES."""
    try:
        p = float(pop)
    except (ValueError, TypeError):
        return 15.0, 15.0
    for lo, hi, m2, pct in POP_MARGIN_RULES:
        if hi is None:
            if p >= lo:
                return float(m2), float(pct)
        else:
            if lo <= p < hi:
                return float(m2), float(pct)
    return 15.0, 15.0


def bucket_for_population(pop: Optional[float]) -> Tuple[Optional[int], Optional[int]]:
    """Zwraca (bucket_low, bucket_high) wg POP_MARGIN_RULES."""
    if pop is None:
        return None, None
    try:
        p = float(pop)
    except (ValueError, TypeError):
        return None, None
    for lo, hi, _, _ in POP_MARGIN_RULES:
        if hi is None:
            if p >= lo:
                return int(lo), None
        else:
            if lo <= p < hi:
                return int(lo), int(hi)
    return None, None


# =========================
# Ludnosc (lokalny CSV)
# =========================

def _find_ludnosc_csv(base_dir: Path) -> Optional[Path]:
    candidates = [
        base_dir / "ludnosc.csv",
        base_dir / "Ludnosc.csv",
        base_dir / "ludnosc_miejscowosci.csv",
        base_dir / "ludnosc_miejscowosci_uzupelnione_2025.csv",
        Path(__file__).resolve().parent / "ludnosc.csv",
        Path.cwd() / "ludnosc.csv",
    ]
    for p in candidates:
        try:
            if p.exists():
                return p.resolve()
        except OSError:
            logger.debug("Nie mozna sprawdzic istnienia pliku: %s", p)
    return None


class PopulationResolver:
    """Prosty resolver ludnosci na bazie lokalnego ludnosc.csv."""
    def __init__(self, local_csv: Optional[Path]):
        self.local_csv = local_csv
        self._local: Dict[str, float] = {}
        if local_csv:
            self._load_local()

    def _make_key(self, woj="", powiat="", gmina="", miejscowosc="", dzielnica="") -> str:
        w = _canon_admin(woj, "woj")
        p = _canon_admin(powiat, "pow")
        g = _canon_admin(gmina, "gmi")
        m = _canon_admin(miejscowosc, "mia")
        d = _canon_admin(dzielnica, "dzl")
        return "|".join([w, p, g, m, d])

    def _candidate_keys(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> List[str]:
        return [
            self._make_key(woj, powiat, gmina, miejscowosc, dzielnica),
            self._make_key(woj, powiat, gmina, miejscowosc, ""),
            self._make_key(woj, powiat, gmina, "", ""),
            self._make_key(woj, powiat, "", "", ""),
            self._make_key(woj, "", "", "", ""),
        ]

    def _load_local(self) -> None:
        try:
            df = pd.read_csv(self.local_csv, sep=";", encoding="utf-8", engine="python")
        except (UnicodeDecodeError, pd.errors.ParserError):
            logger.debug("Probuje ponownie wczytac ludnosc.csv z kodowaniem utf-8-sig")
            df = pd.read_csv(self.local_csv, sep=";", encoding="utf-8-sig", engine="python")

        c_woj = _find_col(df.columns, ["Wojewodztwo", "Województwo"])
        c_pow = _find_col(df.columns, ["Powiat"])
        c_gmi = _find_col(df.columns, ["Gmina"])
        c_mia = _find_col(df.columns, ["Miejscowosc", "Miejscowość", "Miasto"])
        c_dzl = _find_col(df.columns, ["Dzielnica", "Osiedle"])
        c_pop = _find_col(df.columns, ["ludnosc", "ludność", "Ludnosc", "Ludność"])

        if not c_woj or not c_mia or not c_pop:
            raise ManualUserError(f"ludnosc.csv ma nieoczekiwany format (brak kolumn: woj/miejscowosc/ludnosc): {self.local_csv}")

        for _, r in df.iterrows():
            woj = r[c_woj] if c_woj else ""
            powiat = r[c_pow] if c_pow else ""
            gmina = r[c_gmi] if c_gmi else ""
            mia = r[c_mia] if c_mia else ""
            dzl = r[c_dzl] if c_dzl else ""
            pop = _to_float_maybe(r[c_pop])
            if pop is None:
                continue
            key = self._make_key(str(woj), str(powiat), str(gmina), str(mia), str(dzl))
            self._local[key] = float(pop)

    def get_population(self, woj="", powiat="", gmina="", miejscowosc="", dzielnica="") -> Optional[float]:
        if not self._local:
            return None
        for k in self._candidate_keys(woj, powiat, gmina, miejscowosc, dzielnica):
            if k in self._local:
                return float(self._local[k])
        return None


# =========================
# Polska.xlsx indeks
# =========================

@dataclass
class PolskaIndex:
    df: pd.DataFrame
    col_area: str
    col_price: str
    c_area_num: str
    c_price_num: str
    col_woj: Optional[str]
    col_pow: Optional[str]
    col_gmi: Optional[str]
    col_mia: Optional[str]
    col_dzl: Optional[str]
    c_woj: Optional[str]
    c_pow: Optional[str]
    c_gmi: Optional[str]
    c_mia: Optional[str]
    c_dzl: Optional[str]
    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]]
    by_powiat: Dict[Tuple[str, str], Dict[str, str]]
    by_woj: Dict[str, Dict[str, str]]


def build_polska_index(df_pl: pd.DataFrame, col_area: str, col_price: str) -> PolskaIndex:
    df = df_pl.copy()

    c_area_num = "_area_num"
    c_price_num = "_price_num"
    df[c_area_num] = df[col_area].map(_to_float_maybe)
    df[c_price_num] = df[col_price].map(_to_float_maybe)

    col_woj = _find_col(df.columns, ["wojewodztwo", "województwo"])
    col_pow = _find_col(df.columns, ["powiat"])
    col_gmi = _find_col(df.columns, ["gmina"])
    col_mia = _find_col(df.columns, ["miejscowosc", "miejscowość", "miasto"])
    col_dzl = _find_col(df.columns, ["dzielnica", "osiedle"])

    c_woj = c_pow = c_gmi = c_mia = c_dzl = None
    if col_woj:
        c_woj = "_woj_c"
        df[c_woj] = df[col_woj].map(lambda x: _canon_admin(x, "woj"))
    if col_pow:
        c_pow = "_pow_c"
        df[c_pow] = df[col_pow].map(lambda x: _canon_admin(x, "pow"))
    if col_gmi:
        c_gmi = "_gmi_c"
        df[c_gmi] = df[col_gmi].map(lambda x: _canon_admin(x, "gmi"))
    if col_mia:
        c_mia = "_mia_c"
        df[c_mia] = df[col_mia].map(lambda x: _canon_admin(x, "mia"))
    if col_dzl:
        c_dzl = "_dzl_c"
        df[c_dzl] = df[col_dzl].map(lambda x: _canon_admin(x, "dzl"))

    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    by_powiat: Dict[Tuple[str, str], Dict[str, str]] = {}
    by_woj: Dict[str, Dict[str, str]] = {}

    if c_woj and c_mia and col_mia:
        for w, gdf in df.groupby(c_woj, dropna=False):
            if not w:
                continue
            mp: Dict[str, str] = {}
            for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                if not mia_c:
                    continue
                try:
                    ex = sub[col_mia].dropna().iloc[0]
                    mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                except (IndexError, KeyError):
                    mp[mia_c] = str(mia_c)
            by_woj[str(w)] = mp

        if c_pow and col_pow:
            for (w, p), gdf in df.groupby([c_woj, c_pow], dropna=False):
                if not w or not p:
                    continue
                mp: Dict[str, str] = {}
                for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                    if not mia_c:
                        continue
                    try:
                        ex = sub[col_mia].dropna().iloc[0]
                        mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                    except (IndexError, KeyError):
                        mp[mia_c] = str(mia_c)
                by_powiat[(str(w), str(p))] = mp

        if c_pow and c_gmi and col_pow and col_gmi:
            for (w, p, g), gdf in df.groupby([c_woj, c_pow, c_gmi], dropna=False):
                if not w or not p or not g:
                    continue
                mp: Dict[str, str] = {}
                for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                    if not mia_c:
                        continue
                    try:
                        ex = sub[col_mia].dropna().iloc[0]
                        mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                    except (IndexError, KeyError):
                        mp[mia_c] = str(mia_c)
                by_gmina[(str(w), str(p), str(g))] = mp

    return PolskaIndex(
        df=df,
        col_area=col_area,
        col_price=col_price,
        c_area_num=c_area_num,
        c_price_num=c_price_num,
        col_woj=col_woj,
        col_pow=col_pow,
        col_gmi=col_gmi,
        col_mia=col_mia,
        col_dzl=col_dzl,
        c_woj=c_woj,
        c_pow=c_pow,
        c_gmi=c_gmi,
        c_mia=c_mia,
        c_dzl=c_dzl,
        by_gmina=by_gmina,
        by_powiat=by_powiat,
        by_woj=by_woj,
    )


# =========================
# Algorytm z automat1 — importy
# =========================

from automat1 import (
    classify_location,
    _build_stage_masks,
    _select_candidates_dynamic_margin,
    _mask_eq_canon,
    PRICE_FLOOR_PLN_M2,
    PRICE_CEIL_PLN_M2,
)


# =========================
# Cache
# =========================

_CACHE: Dict[str, Any] = {
    "polska_path": None,
    "polska_mtime": None,
    "df_pl": None,
    "pl_index": None,
    "ludnosc_path": None,
    "ludnosc_mtime": None,
    "pop_resolver": None,
}


def _get_cached_index_and_pop(base_dir: Path) -> Tuple[PolskaIndex, Optional[PopulationResolver]]:
    polska_path = (base_dir / "Polska.xlsx").resolve()
    if not polska_path.exists():
        raise ManualUserError(f"Nie znaleziono pliku: {polska_path}")

    polska_mtime = polska_path.stat().st_mtime

    ludnosc_path = _find_ludnosc_csv(base_dir)
    ludnosc_mtime = ludnosc_path.stat().st_mtime if ludnosc_path and ludnosc_path.exists() else None

    need_reload_polska = (_CACHE["polska_path"] != str(polska_path)) or (_CACHE["polska_mtime"] != polska_mtime)
    if need_reload_polska:
        logger.info("Wczytywanie Polska.xlsx z: %s", polska_path)
        df_pl = pd.read_excel(polska_path)
        col_area_pl = _find_col(df_pl.columns, ["metry", "powierzchnia", "Obszar", "obszar"])
        col_price_pl = _find_col(df_pl.columns, ["cena_za_metr", "cena za metr", "cena za m\u00b2", "cena za m2", "cena/m2", "cena_za_m2"])
        if not col_area_pl or not col_price_pl:
            raise ManualUserError("Polska.xlsx nie zawiera wymaganych kolumn metrazu i/lub ceny za m2.")
        pl_index = build_polska_index(df_pl, col_area_pl, col_price_pl)
        _CACHE.update({
            "polska_path": str(polska_path),
            "polska_mtime": polska_mtime,
            "df_pl": df_pl,
            "pl_index": pl_index,
        })

    need_reload_ludnosc = False
    if ludnosc_path is None:
        if _CACHE.get("pop_resolver") is None:
            need_reload_ludnosc = True
    else:
        if (_CACHE.get("ludnosc_path") != str(ludnosc_path)) or (_CACHE.get("ludnosc_mtime") != ludnosc_mtime):
            need_reload_ludnosc = True

    if need_reload_ludnosc:
        pop_resolver = PopulationResolver(ludnosc_path) if ludnosc_path else None
        _CACHE.update({
            "ludnosc_path": str(ludnosc_path) if ludnosc_path else None,
            "ludnosc_mtime": ludnosc_mtime,
            "pop_resolver": pop_resolver,
        })

    return _CACHE["pl_index"], _CACHE.get("pop_resolver")


# =========================
# Glowna funkcja dla selektor_csv.py
# =========================

def compute_and_save_row(
    df_report: pd.DataFrame,
    idx: int,
    base_dir: Path,
    out_dir: Path,
    margin_m2_default: float = 15.0,
    margin_pct_default: float = 15.0,
    min_hits: int = 5,
) -> Dict[str, Any]:
    """
    Liczy i zapisuje wybrany wiersz raportu.
    Algorytm identyczny z automat1._process_row:
    - dynamiczne kroki marginesu m2
    - etapy z _build_stage_masks
    - preferencja dzielnicy
    - klasyfikacja lokalizacji

    Dodatkowo (vs automat):
    - zapisuje plik (Nr KW).xlsx z wybranymi ofertami
    - zaokragla do 2 miejsc po przecinku
    """
    if df_report is None or idx is None:
        raise ManualUserError("Brak raportu lub indeksu wiersza.")
    if idx < 0 or idx >= len(df_report.index):
        raise ManualUserError("Nieprawidlowy indeks wiersza.")

    row = df_report.iloc[idx]

    # --- pola z raportu ---
    kw_col = _find_col(df_report.columns, ["Nr KW", "nr_kw", "nrksiegi", "nr ksi\u0119gi", "nr_ksiegi", "numer ksi\u0119gi"])
    kw_value = (str(row[kw_col]).strip() if (kw_col and pd.notna(row[kw_col]) and str(row[kw_col]).strip()) else f"WIERSZ_{idx+1}")

    area_col = _find_col(df_report.columns, ["Obszar", "metry", "powierzchnia"])
    area_val = _to_float_maybe(_trim_after_semicolon(row[area_col])) if area_col else None
    if area_val is None:
        raise ManualUserError("Nie znalazlem wartosci obszaru/metry w raporcie (dla tego wiersza).")

    def _get(cands):
        c = _find_col(df_report.columns, cands)
        return _trim_after_semicolon(row[c]) if c else ""

    woj_r = _get(["Wojew\u00f3dztwo", "Wojewodztwo", "wojewodztwo", "woj"])
    pow_r = _get(["Powiat"])
    gmi_r = _get(["Gmina"])
    mia_r = _get(["Miejscowo\u015b\u0107", "Miejscowosc", "Miasto"])
    dzl_r = _get(["Dzielnica", "Osiedle"])

    woj_c = _canon_admin(woj_r, "woj")
    pow_c = _canon_admin(pow_r, "pow")
    gmi_c = _canon_admin(gmi_r, "gmi")
    mia_c = _canon_admin(mia_r, "mia")
    dzl_c = _canon_admin(dzl_r, "dzl")

    # kolumny wynikowe w raporcie
    mean_col = _find_col(df_report.columns, ["\u015arednia cena za m2 ( z bazy)", "Srednia cena za m2 ( z bazy)", "\u015arednia cena za m\u00b2 ( z bazy)"])
    corr_col = _find_col(df_report.columns, ["\u015arednia skorygowana cena za m2", "Srednia skorygowana cena za m2"])
    val_col = _find_col(df_report.columns, ["Statystyczna warto\u015b\u0107 nieruchomo\u015bci", "Statystyczna wartosc nieruchomosci"])

    if mean_col is None:
        mean_col = "\u015arednia cena za m2 ( z bazy)"
        if mean_col not in df_report.columns:
            df_report[mean_col] = ""
    if corr_col is None:
        corr_col = "\u015arednia skorygowana cena za m2"
        if corr_col not in df_report.columns:
            df_report[corr_col] = ""
    if val_col is None:
        val_col = "Statystyczna warto\u015b\u0107 nieruchomo\u015bci"
        if val_col not in df_report.columns:
            df_report[val_col] = ""

    # minimalne dane: woj + pow + gmi + mia (jak automat)
    STRICT_MSG = "BRAK LUB NIEPELNY ADRESU \u2013 WPISZ ADRES MANUALNIE"
    if not woj_c or not pow_c or not gmi_c or not mia_c:
        df_report.at[idx, mean_col] = STRICT_MSG
        df_report.at[idx, corr_col] = STRICT_MSG
        df_report.at[idx, val_col] = STRICT_MSG
        return {
            "kw": kw_value, "avg": None, "corrected": None, "value": None,
            "out_path": None, "stage": "strict", "hits": 0,
            "pop": None, "bucket": None,
        }

    # cache: Polska.xlsx index + ludnosc
    pl_index, pop_resolver = _get_cached_index_and_pop(base_dir)

    # ludnosc + progi
    pop_target = pop_resolver.get_population(woj_r, pow_r, gmi_r, mia_r, dzl_r) if pop_resolver else None

    if pop_target is None:
        margin_m2, margin_pct = float(margin_m2_default), float(margin_pct_default)
    else:
        margin_m2, margin_pct = rules_for_population(pop_target)

    if not (isinstance(margin_m2, (int, float)) and float(margin_m2) > 0):
        margin_m2 = float(margin_m2_default)
    if not isinstance(margin_pct, (int, float)):
        margin_pct = float(margin_pct_default)

    # klasyfikacja lokalizacji (jak automat)
    loc_class = classify_location(mia_c, pow_c, woj_c)

    # preferencja dzielnicy
    prefer_mask = None
    if dzl_c and pl_index.c_dzl:
        prefer_mask = _mask_eq_canon(pl_index.df, pl_index.c_dzl, dzl_c)

    # bucket populacyjny — uzyj ludnosci miasta (bez dzielnicy) do bucketu
    pop_for_bucket = pop_target
    if pop_resolver is not None and dzl_r:
        try:
            pop_city = pop_resolver.get_population(woj_r, pow_r, gmi_r, mia_r, "")
            if pop_city is not None:
                pop_for_bucket = pop_city
        except (ValueError, TypeError):
            pass

    bucket_low, bucket_high = bucket_for_population(pop_for_bucket)
    pop_cache_local: Dict[Tuple[str, str], float | None] = {}

    step_m2 = 3.0

    # buduj maski etapow (algorytm z automat1)
    stage_masks = _build_stage_masks(
        pl_index, woj_c, pow_c, gmi_c, mia_c, loc_class,
        bucket_low=bucket_low,
        bucket_high=bucket_high,
        pop_resolver=pop_resolver,
        woj_raw=woj_r,
        pow_raw=pow_r,
        gmi_raw=gmi_r,
        pop_cache=pop_cache_local,
    )

    # iteruj etapy z dynamicznym marginesem m2 (jak automat)
    best_df = pl_index.df.iloc[0:0].copy()
    best_hits = 0
    best_used_m = float(margin_m2)
    best_used_dzl = False
    best_stage_name = stage_masks[-1][0] if stage_masks else "woj"
    best_stage_req = stage_masks[-1][2] if stage_masks else int(min_hits)
    best_stage_meta = stage_masks[-1][3] if stage_masks else ""

    for stage_name, base_mask, stage_min_hits, stage_meta in stage_masks:
        stage_min_hits = int(stage_min_hits) if stage_min_hits is not None else int(min_hits)

        cand_df, used_m, used_dzl = _select_candidates_dynamic_margin(
            pl=pl_index,
            base_mask=base_mask,
            area_target=float(area_val),
            max_margin_m2=float(margin_m2),
            step_m2=float(step_m2),
            prefer_mask=prefer_mask,
            min_hits=stage_min_hits,
        )
        cand_n = int(len(cand_df.index)) if cand_df is not None else 0

        if cand_n > best_hits:
            best_df, best_hits = cand_df, cand_n
            best_used_m, best_used_dzl = float(used_m), bool(used_dzl)
            best_stage_name = stage_name
            best_stage_req = stage_min_hits
            best_stage_meta = stage_meta or ""

        if cand_n >= stage_min_hits:
            best_df, best_hits = cand_df, cand_n
            best_used_m, best_used_dzl = float(used_m), bool(used_dzl)
            best_stage_name = stage_name
            best_stage_req = stage_min_hits
            best_stage_meta = stage_meta or ""
            break

    cand_df = best_df
    cand_n = int(len(cand_df.index)) if cand_df is not None else 0
    req_min = int(best_stage_req) if best_stage_req is not None else int(min_hits)
    meta = best_stage_meta or ""
    stage = f"{loc_class}:{best_stage_name}{meta}|m={float(best_used_m):g}|{'dzielnica' if best_used_dzl else 'bez_dzielnicy'}"

    # brak wynikow
    NO_OFFERS_MSG = "BRAK OGLOSZEN W BAZIE DLA TEGO ZAKRESU"
    if cand_df is None or cand_n == 0:
        df_report.at[idx, mean_col] = NO_OFFERS_MSG
        df_report.at[idx, corr_col] = NO_OFFERS_MSG
        df_report.at[idx, val_col] = NO_OFFERS_MSG
        return {
            "kw": kw_value, "avg": None, "corrected": None, "value": None,
            "out_path": None, "stage": stage, "hits": 0,
            "pop": pop_target, "bucket": (bucket_low, bucket_high),
        }

    # outliers
    cand_df2, prices = _filter_outliers_df(cand_df, "_price_num")
    avg = float(np.nanmean(prices)) if prices is not None and len(prices) else None
    if avg is None:
        df_report.at[idx, mean_col] = NO_OFFERS_MSG
        df_report.at[idx, corr_col] = NO_OFFERS_MSG
        df_report.at[idx, val_col] = NO_OFFERS_MSG
        return {
            "kw": kw_value, "avg": None, "corrected": None, "value": None,
            "out_path": None, "stage": f"{stage}|no_price", "hits": cand_n,
            "pop": pop_target, "bucket": (bucket_low, bucket_high),
        }

    # zaokraglanie do 2 miejsc
    mean_rounded = round(avg, 2)
    corrected = round(mean_rounded * (1.0 - float(margin_pct) / 100.0), 2)
    value = round(corrected * float(area_val), 2)

    # wpis do raportu
    df_report.at[idx, mean_col] = mean_rounded
    df_report.at[idx, corr_col] = corrected
    df_report.at[idx, val_col] = value

    # zapis pliku (Nr KW).xlsx z wybranymi rekordami
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_kw = "".join(ch for ch in str(kw_value) if ch not in "\\/:*?\"<>|")
    out_path = out_dir / f"({safe_kw}).xlsx"

    df_out = cand_df2.copy()

    # wiersz podsumowania
    summary = {c: "" for c in df_out.columns}
    try:
        summary[pl_index.col_price] = mean_rounded
    except (KeyError, TypeError):
        logger.debug("Nie udalo sie wpisac sredniej do kolumny ceny: %s", pl_index.col_price)

    df_out = pd.concat([df_out, pd.DataFrame([summary])], ignore_index=True)
    df_out.loc[len(df_out) - 1, "\u015aREDNIA_CENA_M2"] = mean_rounded
    df_out.loc[len(df_out) - 1, "ETAP_DOBORU"] = stage
    df_out.loc[len(df_out) - 1, "HITS"] = int(len(cand_df2))

    # kolumny w sensownej kolejnosci
    premium_cols = [
        "cena", "cena_za_metr", "cena_za_m2", "metry", "powierzchnia", "liczba_pokoi", "pietro",
        "rynek", "rok_budowy", "material",
        "wojewodztwo", "powiat", "gmina", "miejscowosc", "miejscowo\u015b\u0107", "dzielnica", "ulica",
        "link",
        "\u015aREDNIA_CENA_M2", "ETAP_DOBORU", "HITS",
    ]
    existing = [c for c in premium_cols if c in df_out.columns]
    if existing:
        df_out = df_out[existing + [c for c in df_out.columns if c not in existing]]

    df_out.to_excel(out_path, index=False)

    return {
        "kw": kw_value,
        "avg": mean_rounded,
        "corrected": corrected,
        "value": value,
        "out_path": out_path,
        "stage": stage,
        "hits": int(len(cand_df2)),
        "pop": pop_target,
        "bucket": (bucket_low, bucket_high),
        "margins": (margin_m2, margin_pct),
    }
