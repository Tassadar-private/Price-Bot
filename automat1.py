from __future__ import annotations

'''
automat1.py — Wersja C (mniejszy rozrzut cen)

FIX (17.12.2025+):
- ludnosc.csv jest wczytywane OK (logi), ale brak trafień wynikał z różnic w nazwach (pow./powiat, gmina miejska..., nawiasy)
- dodano kanonizację nazw jednostek (usuwa prefiksy/skrótowce/nawiasy)
- dodano fallback dopasowania po (woj + miejscowosc) + preferencja dzielnicy
- zapis XLSX: openpyxl, tylko arkusz 'raport' (bez kasowania innych arkuszy)

POPRAWKI ROZRZUTU CEN (2025+):
- PRICE_FLOOR_PLN_M2 / PRICE_CEIL_PLN_M2: absolutne limity cen dla PL rynku (2000–40000 zł/m²)
  → odrzuca oczywiste błędy danych zanim trafią do średniej
- _filter_outliers_df: percentyle 10–90 zamiast IQR 1.5 → węższa, bardziej jednorodna próba
  → usunięto też martwy kod (duplicate return po linii 70)
- POP_MARGIN_RULES: zmniejszone marginesy m² (8–20 zamiast 10–25)
  → baza dopasowań jest powierzchniowo bardziej jednorodna
- _min_hits: wyższe progi (6/12/30 zamiast 5/10/20)
  → ostateczny fallback na woj wymaga solid próby zamiast garści obserwacji
- krok m² zmieniony z 3.0 na 2.0 → dokładniejsze stopniowe poszerzanie zakresu
'''

import csv
import datetime
import importlib.util
import logging
import os
import re
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import requests

from utils import (
    canon_admin as _canon_admin,
    find_col as _find_col,
    filter_outliers_df as _filter_outliers_df,
    load_config,
    norm as _norm,
    plain as _plain,
    strip_parentheses as _strip_parentheses,
    to_float_maybe as _to_float_maybe,
    trim_after_semicolon as _trim_after_semicolon,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Konfiguracja z config.json
# ---------------------------------------------------------------------------

_cfg = load_config()
PRICE_FLOOR_PLN_M2 = _cfg["PRICE_FLOOR_PLN_M2"]
PRICE_CEIL_PLN_M2 = _cfg["PRICE_CEIL_PLN_M2"]

def import_local_automat():
    here = Path(__file__).resolve().parent
    p = here / "automat.py"
    spec = importlib.util.spec_from_file_location("automat", str(p))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def _find_ludnosc_csv(baza_folder: Path, raport_path: Path, polska_path: Path) -> Path | None:
    """Szuka pliku ludnosc.csv w kilku znanych lokalizacjach."""
    env = os.getenv("LUDNOSC_CSV_PATH")
    candidates: List[Path] = []
    if env:
        candidates.append(Path(env))
    candidates += [
        raport_path.parent / "ludnosc.csv",
        polska_path.parent / "ludnosc.csv",
        baza_folder / "ludnosc.csv",
    ]

    def _looks_full(p: Path) -> bool:
        try:
            with p.open("r", encoding="utf-8-sig", errors="ignore") as f:
                n = sum(1 for _ in f) - 1
            return n >= 50000
        except OSError:
            return True  # jesli nie mozna otworzyc, nie dyskwalifikuj

    for p in candidates:
        try:
            if p.exists() and p.is_file():
                if _looks_full(p):
                    return p.resolve()
        except OSError as e:
            logger.debug("Nie udalo sie sprawdzic sciezki %s: %s", p, e)
    return None

# ---------------------------------------------------------------------------
# Aglomeracja warszawska
# ---------------------------------------------------------------------------

AGLO_WARSZAWA_DEFAULT = {
    'piaseczno','konstancin jeziorna','gora kalwaria','lesznowola','prazmow','jozefow','otwock','celestynow','karczew','kolbiel','wiazowna',
    'pruszkow','piastow','brwinow','michalowice','nadarzyn','raszyn',
    'blonie','izabelin','kampinos','leszno','stare babice','lomianki','ozarow mazowiecki',
    'marki','zabki','zielonka','wolomin','kobylka','radzymin','tluszcz','jadow','dabrowka','poswietne',
    'legionowo','jablonna','nieporet','serock','wieliszew','nowy dwor mazowiecki','czosnow','leoncin','pomiechowek','zakroczym',
    'grodzisk mazowiecki','milanowek','podkowa lesna'
}

VOIVODE_CAPITALS = {
    'bialystok','bydgoszcz','torun','gdansk','gorzow wielkopolski','katowice','kielce','krakow',
    'lublin','lodz','olsztyn','opole','poznan','rzeszow','szczecin','warszawa','wroclaw','zielona gora'
}

def _canon_local(_s: str) -> str:
    s = str(_s or '').strip().lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = s.replace('-', ' ').replace('/', ' ')
    s = ''.join(ch for ch in s if ch.isalnum() or ch.isspace())
    s = ' '.join(s.split())
    return s

def load_warsaw_agglomeration(hint_path: Path | None = None) -> Set[str]:
    """Wczytuje liste miejscowosci aglomeracji warszawskiej z pliku XLSX."""
    candidates: list[Path] = []
    here = Path(__file__).resolve().parent
    candidates.append(here / 'aglomeracja_warszawska.xlsx')
    if hint_path:
        candidates.append(hint_path.parent / 'aglomeracja_warszawska.xlsx')
    try:
        for p in candidates:
            if not p.exists():
                continue
            xls = pd.ExcelFile(p, engine='openpyxl')
            sheet = None
            for nm in xls.sheet_names:
                if 'reszta' in nm.lower() or 'aglo' in nm.lower():
                    sheet = nm
                    break
            if sheet is None:
                sheet = xls.sheet_names[0]
            df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')
            cols = {str(c).lower(): c for c in df.columns}
            mia_col = None
            for key in ['miejsc', 'miejscowosc', 'miejscowość', 'miasto']:
                if key in cols:
                    mia_col = cols[key]
                    break
            if mia_col is None:
                mia_col = list(df.columns)[-1]
            vals: set[str] = set()
            for v in df[mia_col].dropna().astype(str):
                c = _canon_local(v)
                if c and c != 'warszawa':
                    vals.add(c)
            if vals:
                return vals
    except (OSError, KeyError, ValueError) as e:
        logger.debug("Nie udalo sie wczytac aglomeracji warszawskiej: %s", e)
    return set(AGLO_WARSZAWA_DEFAULT)

# ---------------------------------------------------------------------------
# Reguly marginesow populacyjnych
# ---------------------------------------------------------------------------

POP_MARGIN_RULES = [tuple(r) for r in _cfg.get("POP_MARGIN_RULES", [
    [0, 6000, 20.0, 10.0],
    [6000, 20000, 15.0, 10.0],
    [20000, 50000, 12.0, 10.0],
    [50000, 200000, 10.0, 8.0],
    [200000, None, 8.0, 5.0],
])]

def rules_for_population(pop):
    if pop is None:
        return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])
    try:
        p = float(pop)
    except (ValueError, TypeError):
        return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])

    for low, high, m2, pct in POP_MARGIN_RULES:
        if p >= low and (high is None or p < high):
            return float(m2), float(pct)
    return float(POP_MARGIN_RULES[-1][2]), float(POP_MARGIN_RULES[-1][3])

def _eq_mask(df: pd.DataFrame, col_candidates, value: str) -> pd.Series:
    col = _find_col(df.columns, col_candidates)
    if col is None or not str(value).strip():
        return pd.Series(True, index=df.index)
    s = df[col].astype(str).str.strip().str.lower()
    v = str(value).strip().lower()
    return s == v

# ---------------------------------------------------------------------------
# BDL API
# ---------------------------------------------------------------------------

BDL_BASE_URL = "https://bdl.stat.gov.pl/api/v1"
BDL_API_KEY_DEFAULT = _cfg.get("BDL_API_KEY_DEFAULT", "c804c054-f519-45b3-38f3-08de375a07dc")

def _bdl_headers() -> dict:
    api_key = os.getenv("BDL_API_KEY") or os.getenv("GUS_BDL_API_KEY") or BDL_API_KEY_DEFAULT
    if not api_key:
        return {}
    return {"X-ClientId": api_key, "Accept": "application/json"}

def _pick_latest_year():
    return datetime.date.today().year - 1

class PopulationResolver:
    """Rozwiazuje liczbe ludnosci dla jednostek administracyjnych.

    Zrodla danych (w kolejnosci priorytetow):
    1. Lokalny plik ludnosc.csv
    2. Cache z API (population_cache.csv)
    3. GUS BDL API (fallback online)
    """

    def __init__(self, local_csv: Path | None, api_cache_csv: Path | None, use_api: bool = True):
        self.local_csv = local_csv
        self.api_cache_csv = api_cache_csv
        self.use_api = bool(use_api)
        self._local: Dict[str, float] = {}
        self._api_cache: Dict[str, float] = {}
        self._dirty = False
        self._debug_miss = 0
        self._bdl_pop_var_id: str | None = None
        self._BDL_NOT_FOUND = "__NOT_FOUND__"
        self._load_local()
        self._load_api_cache()

    def _make_key(self, woj: str = "", powiat: str = "", gmina: str = "", miejscowosc: str = "", dzielnica: str = "") -> str:
        w = _canon_admin(woj, "woj")
        p = _canon_admin(powiat, "pow")
        g = _canon_admin(gmina, "gmi")
        m = _canon_admin(miejscowosc, "mia")
        d = _canon_admin(dzielnica, "dzl")
        return "|".join([w, p, g, m, d])

    def _split_key(self, key: str) -> Tuple[str, str, str, str, str]:
        parts = (key.split("|") + ["", "", "", "", ""])[:5]
        return parts[0], parts[1], parts[2], parts[3], parts[4]

    def _candidate_keys(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> List[str]:
        keys = [
            self._make_key(woj, powiat, gmina, miejscowosc, dzielnica),
            self._make_key(woj, powiat, gmina, miejscowosc, ""),
            self._make_key(woj, powiat, gmina, "", ""),
            self._make_key(woj, powiat, "", "", ""),
            self._make_key(woj, "", "", "", ""),
        ]

        keys += [
            self._make_key(woj, "", gmina, miejscowosc, dzielnica),
            self._make_key(woj, "", gmina, miejscowosc, ""),
            self._make_key(woj, "", gmina, "", ""),
            self._make_key(woj, powiat, "", miejscowosc, dzielnica),
            self._make_key(woj, powiat, "", miejscowosc, ""),
            self._make_key(woj, "", "", miejscowosc, dzielnica),
            self._make_key(woj, "", "", miejscowosc, ""),
        ]

        out, seen = [], set()
        for k in keys:
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(k)
        return out

    def _read_local_csv_any_sep(self, path: Path) -> pd.DataFrame:
        for sep in [";", ",", "\t"]:
            try:
                return pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8-sig", engine="python")
            except (pd.errors.ParserError, ValueError) as e:
                logger.debug("Proba odczytu CSV sep=%r nie powiodla sie: %s", sep, e)
                continue
        return pd.read_csv(path, sep=None, dtype=str, encoding="utf-8-sig", engine="python")

    def _load_local(self):
        if not self.local_csv:
            logger.info("[PopulationResolver] local_csv=None (nie podano sciezki).")
            return
        if not self.local_csv.exists():
            logger.warning("[PopulationResolver] local ludnosc.csv: NIE ISTNIEJE -> %s", self.local_csv)
            return

        logger.info("[PopulationResolver] Wczytuje local ludnosc.csv -> %s", self.local_csv)

        try:
            df = self._read_local_csv_any_sep(self.local_csv)
            logger.info("[PopulationResolver] local rows=%d cols=%s", len(df), list(df.columns))

            col_woj = _find_col(df.columns, ["Wojewodztwo", "Województwo"])
            col_pow = _find_col(df.columns, ["Powiat"])
            col_gmi = _find_col(df.columns, ["Gmina"])
            col_mia = _find_col(df.columns, ["Miejscowosc", "Miejscowość", "Miasto"])
            col_dzl = _find_col(df.columns, ["Dzielnica", "Osiedle"])
            col_pop = _find_col(df.columns, ["ludnosc", "Ludnosc", "Liczba mieszkancow", "Liczba mieszkańców", "population"])

            logger.info("[PopulationResolver] map cols: woj=%s pow=%s gmi=%s mia=%s dzl=%s pop=%s",
                        col_woj, col_pow, col_gmi, col_mia, col_dzl, col_pop)

            if not col_pop:
                logger.warning("[PopulationResolver] local ludnosc.csv: brak kolumny ludnosc/population -> nie uzyje pliku.")
                return

            loaded = 0
            for _, r in df.iterrows():
                pop_f = _to_float_maybe(r.get(col_pop))
                if pop_f is None:
                    continue

                woj = r.get(col_woj, "") if col_woj else ""
                powiat = r.get(col_pow, "") if col_pow else ""
                gmina = r.get(col_gmi, "") if col_gmi else ""
                miejsc = r.get(col_mia, "") if col_mia else ""
                dziel = r.get(col_dzl, "") if col_dzl else ""

                key = self._make_key(woj, powiat, gmina, miejsc, dziel)
                if key:
                    self._local[key] = float(pop_f)
                    loaded += 1

            logger.info("[PopulationResolver] local loaded keys=%d (unikalne=%d)", loaded, len(self._local))

        except (OSError, pd.errors.ParserError) as e:
            logger.error("[PopulationResolver] Nie udalo sie wczytac local ludnosc.csv: %s", e)

    def _load_api_cache(self):
        if not self.api_cache_csv or not self.api_cache_csv.exists():
            return
        try:
            with self.api_cache_csv.open("r", encoding="utf-8-sig", newline="") as f:
                rd = csv.DictReader(f)
                for row in rd:
                    pop = _to_float_maybe(row.get("population", ""))
                    if pop is None:
                        continue
                    key = row.get("key") or self._make_key(
                        row.get("woj", ""), row.get("powiat", ""), row.get("gmina", ""),
                        row.get("miejscowosc", ""), row.get("dzielnica", "")
                    )
                    if key:
                        self._api_cache[key] = float(pop)
        except OSError as e:
            logger.error("[PopulationResolver] Nie udalo sie wczytac cache API: %s", e)

    def _save_api_cache(self):
        if not self._dirty or not self.api_cache_csv:
            return
        try:
            self.api_cache_csv.parent.mkdir(parents=True, exist_ok=True)
            with self.api_cache_csv.open("w", encoding="utf-8-sig", newline="") as f:
                fieldnames = ["key", "woj", "powiat", "gmina", "miejscowosc", "dzielnica", "population"]
                wr = csv.DictWriter(f, fieldnames=fieldnames)
                wr.writeheader()
                for key, pop in self._api_cache.items():
                    parts = (key.split("|") + ["", "", "", "", ""])[:5]
                    woj, pow, gmi, mia, dzl = parts
                    wr.writerow({
                        "key": key,
                        "woj": woj,
                        "powiat": pow,
                        "gmina": gmi,
                        "miejscowosc": mia,
                        "dzielnica": dzl,
                        "population": pop,
                    })
            self._dirty = False
        except OSError as e:
            logger.error("[PopulationResolver] Nie udalo sie zapisac cache API: %s", e)

    def _get_population_var_id(self) -> str | None:
        if self._bdl_pop_var_id == self._BDL_NOT_FOUND:
            return None
        if self._bdl_pop_var_id:
            return self._bdl_pop_var_id

        headers = _bdl_headers()
        if not headers:
            return None

        try:
            url = f"{BDL_BASE_URL}/variables"
            params = {"name": "ludność ogółem", "page-size": 50, "format": "json"}
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                for v in data.get("results", []):
                    name = (v.get("name") or "").lower()
                    if "ludność ogółem" in name or "ludnosc ogolem" in name or "population total" in name:
                        self._bdl_pop_var_id = str(v.get("id"))
                        logger.info("[PopulationResolver] Zmienna ludnosci: id=%s (%s)", self._bdl_pop_var_id, name)
                        return self._bdl_pop_var_id
        except (requests.RequestException, ValueError, KeyError) as e:
            logger.debug("[PopulationResolver] Blad przy pobieraniu zmiennej BDL: %s", e)

        logger.info("[PopulationResolver] Nie znalazlem zmiennej 'ludnosc ogolem' w BDL (cache).")
        self._bdl_pop_var_id = self._BDL_NOT_FOUND
        return None

    def _fetch_population_from_api(self, woj: str, powiat: str, gmina: str, miejscowosc: str) -> Optional[float]:
        headers = _bdl_headers()
        if not headers:
            return None

        name_search = miejscowosc or gmina
        if not name_search:
            return None

        try:
            url_units = f"{BDL_BASE_URL}/units"
            params_units = {"name": name_search, "level": "6", "page-size": 50, "format": "json"}
            ru = requests.get(url_units, headers=headers, params=params_units, timeout=15)
            if ru.status_code != 200:
                return None
            ju = ru.json()
            units = ju.get("results", []) or []
            if not units:
                return None

            def score(u):
                nm = _plain(u.get("name") or "")
                sc = 0
                if _plain(name_search) == nm:
                    sc += 5
                elif _plain(name_search) in nm:
                    sc += 3
                if powiat and _plain(powiat) in nm:
                    sc += 1
                if woj and _plain(woj) in nm:
                    sc += 1
                return sc

            units.sort(key=score, reverse=True)
            unit_id = units[0].get("id")
            if not unit_id:
                return None
        except (requests.RequestException, ValueError, KeyError) as e:
            logger.debug("[PopulationResolver] Blad przy wyszukiwaniu jednostek BDL: %s", e)
            return None

        var_id = self._get_population_var_id()
        if not var_id:
            return None

        year = _pick_latest_year()
        try:
            url_data = f"{BDL_BASE_URL}/data/by-unit/{unit_id}"
            params_data = {"var-id": var_id, "year": str(year), "format": "json"}
            rd = requests.get(url_data, headers=headers, params=params_data, timeout=20)
            if rd.status_code != 200:
                return None

            jd = rd.json()
            results = jd.get("results") or []
            if not results:
                return None

            vals = results[0].get("values") or []
            for v in vals:
                raw = v[0] if isinstance(v, list) and len(v) >= 1 else v
                pop = _to_float_maybe(raw)
                if pop is not None:
                    return float(pop)
        except (requests.RequestException, ValueError, KeyError, IndexError) as e:
            logger.debug("[PopulationResolver] Blad przy pobieraniu danych populacji BDL: %s", e)
            return None

        return None

    def _fallback_by_woj_mia(self, woj: str, miejscowosc: str, dzielnica: str) -> Optional[float]:
        """Fallback: szuka dopasowania po (woj + miejscowosc), preferuje dzielnice."""
        woj_c = _canon_admin(woj, "woj")
        mia_c = _canon_admin(miejscowosc, "mia")
        dzl_c = _canon_admin(dzielnica, "dzl")

        if not woj_c or not mia_c:
            return None

        best_with_dzl = None
        best_any = None

        for key, pop in self._local.items():
            w, p, g, m, d = self._split_key(key)
            if w != woj_c or m != mia_c:
                continue
            if dzl_c and d == dzl_c:
                best_with_dzl = pop if (best_with_dzl is None or pop > best_with_dzl) else best_with_dzl
            else:
                best_any = pop if (best_any is None or pop > best_any) else best_any

        return best_with_dzl if best_with_dzl is not None else best_any

    def get_population(self, woj: str, powiat: str, gmina: str, miejscowosc: str, dzielnica: str) -> Optional[float]:
        for key in self._candidate_keys(woj, powiat, gmina, miejscowosc, dzielnica):
            if key in self._local:
                return self._local[key]
            if key in self._api_cache:
                return self._api_cache[key]

        pop = self._fallback_by_woj_mia(woj, miejscowosc, dzielnica)
        if pop is not None:
            return float(pop)

        if self.use_api:
            pop = self._fetch_population_from_api(woj, powiat, gmina, miejscowosc)
            if pop is not None:
                key4 = self._make_key(woj, powiat, gmina, miejscowosc, "")
                self._api_cache[key4] = float(pop)
                self._dirty = True
                self._save_api_cache()
                return float(pop)

        if self._debug_miss < 3:
            self._debug_miss += 1
            logger.info("[PopulationResolver][MISS] szukalem dla: woj=%s pow=%s gmi=%s mia=%s dzl=%s canon_key=%s",
                        woj, powiat, gmina, miejscowosc, dzielnica,
                        self._make_key(woj, powiat, gmina, miejscowosc, dzielnica))

        return None

# ---------------------------------------------------------------------------
# Bucket populacyjny
# ---------------------------------------------------------------------------

def _bucket_for_population(pop: float | None) -> tuple[float | None, float | None]:
    if pop is None:
        return (None, None)
    try:
        p = float(pop)
    except (ValueError, TypeError):
        return (None, None)

    for low, high, _, _ in POP_MARGIN_RULES:
        if p >= low and (high is None or p < high):
            return (float(low), float(high) if high is not None else None)

    low, high, _, _ = POP_MARGIN_RULES[-1]
    return (float(low), float(high) if high is not None else None)

def _pop_in_bucket(pop: float | None, low: float | None, high: float | None) -> bool:
    if low is None and high is None:
        return True
    if pop is None:
        return False
    try:
        p = float(pop)
    except (ValueError, TypeError):
        return False
    if high is None:
        return p >= float(low)
    return p >= float(low) and p < float(high)

@dataclass
class PolskaIndex:
    df: pd.DataFrame
    col_area: str
    col_price: str

    col_woj: str | None
    col_pow: str | None
    col_gmi: str | None
    col_mia: str | None
    col_dzl: str | None

    c_woj: str | None
    c_pow: str | None
    c_gmi: str | None
    c_mia: str | None
    c_dzl: str | None

    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]]
    by_powiat: Dict[Tuple[str, str], Dict[str, str]]
    by_woj: Dict[str, Dict[str, str]]

def build_polska_index(df_pl: pd.DataFrame, col_area_pl: str, col_price_pl: str) -> PolskaIndex:
    """Buduje indeks bazy Polska.xlsx do szybkiego wyszukiwania porownawczych ofert.

    Tworzy kanonizowane kolumny (_woj_c, _pow_c, ...) oraz mapy miejscowosci
    zgrupowane po gminie, powiecie i wojewodztwie.
    """
    col_woj = _find_col(df_pl.columns, ["wojewodztwo", "województwo", "woj"])
    col_pow = _find_col(df_pl.columns, ["powiat"])
    col_gmi = _find_col(df_pl.columns, ["gmina"])
    col_mia = _find_col(df_pl.columns, ["miejscowosc", "miejscowość", "miasto"])
    col_dzl = _find_col(df_pl.columns, ["dzielnica", "osiedle"])

    if "_area_num" not in df_pl.columns:
        df_pl["_area_num"] = df_pl[col_area_pl].map(_to_float_maybe)
    if "_price_num" not in df_pl.columns:
        df_pl["_price_num"] = df_pl[col_price_pl].map(_to_float_maybe)

    c_woj = c_pow = c_gmi = c_mia = c_dzl = None
    if col_woj:
        c_woj = "_woj_c"
        df_pl[c_woj] = df_pl[col_woj].map(lambda x: _canon_admin(x, "woj"))
    if col_pow:
        c_pow = "_pow_c"
        df_pl[c_pow] = df_pl[col_pow].map(lambda x: _canon_admin(x, "pow"))
    if col_gmi:
        c_gmi = "_gmi_c"
        df_pl[c_gmi] = df_pl[col_gmi].map(lambda x: _canon_admin(x, "gmi"))
    if col_mia:
        c_mia = "_mia_c"
        df_pl[c_mia] = df_pl[col_mia].map(lambda x: _canon_admin(x, "mia"))
    if col_dzl:
        c_dzl = "_dzl_c"
        df_pl[c_dzl] = df_pl[col_dzl].map(lambda x: _canon_admin(x, "dzl"))

    by_gmina: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    by_powiat: Dict[Tuple[str, str], Dict[str, str]] = {}
    by_woj: Dict[str, Dict[str, str]] = {}

    if c_woj and c_mia:
        for w, gdf in df_pl.groupby(c_woj, dropna=False):
            if not w:
                continue
            mp: Dict[str, str] = {}
            if col_mia and c_mia:
                for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                    if not mia_c:
                        continue
                    try:
                        ex = sub[col_mia].dropna().iloc[0]
                        mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                    except (KeyError, IndexError):
                        mp[mia_c] = str(mia_c)
            by_woj[str(w)] = mp

        if c_pow:
            for (w, p), gdf in df_pl.groupby([c_woj, c_pow], dropna=False):
                if not w or not p:
                    continue
                mp: Dict[str, str] = {}
                if col_mia:
                    for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                        if not mia_c:
                            continue
                        try:
                            ex = sub[col_mia].dropna().iloc[0]
                            mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                        except (KeyError, IndexError):
                            mp[mia_c] = str(mia_c)
                by_powiat[(str(w), str(p))] = mp

        if c_pow and c_gmi:
            for (w, p, g), gdf in df_pl.groupby([c_woj, c_pow, c_gmi], dropna=False):
                if not w or not p or not g:
                    continue
                mp: Dict[str, str] = {}
                if col_mia:
                    for mia_c, sub in gdf.groupby(c_mia, dropna=False):
                        if not mia_c:
                            continue
                        try:
                            ex = sub[col_mia].dropna().iloc[0]
                            mp[mia_c] = str(ex) if pd.notna(ex) else str(mia_c)
                        except (KeyError, IndexError):
                            mp[mia_c] = str(mia_c)
                by_gmina[(str(w), str(p), str(g))] = mp

    return PolskaIndex(
        df=df_pl,
        col_area=col_area_pl,
        col_price=col_price_pl,
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

def _mask_eq_canon(df: pd.DataFrame, canon_col: str | None, value_canon: str) -> pd.Series:
    if canon_col is None or not value_canon:
        return pd.Series(True, index=df.index)
    return df[canon_col].astype(str) == str(value_canon)

def _filter_miejscowosci_by_bucket(
    candidates: Dict[str, str],
    bucket_low: float | None,
    bucket_high: float | None,
    pop_resolver: PopulationResolver | None,
    woj_raw: str,
    pow_raw: str,
    gmi_raw: str,
    scope: str,
    pop_cache: Dict[Tuple[str, str], float | None],
) -> List[str]:
    """Filtruje miejscowosci wg bucketu populacyjnego."""
    if not candidates:
        return []
    if bucket_low is None and bucket_high is None:
        return list(candidates.keys())

    out: List[str] = []
    for mia_c, mia_original in candidates.items():
        cache_key = (scope, mia_c)
        if cache_key in pop_cache:
            pop = pop_cache[cache_key]
        else:
            pop = None
            if pop_resolver is not None:
                pop = pop_resolver.get_population(woj_raw, pow_raw, gmi_raw, mia_original, "")
            pop_cache[cache_key] = pop
        if _pop_in_bucket(pop, bucket_low, bucket_high):
            out.append(mia_c)
    return out

# ---------------------------------------------------------------------------
# Klasyfikacja lokalizacji
# ---------------------------------------------------------------------------

def classify_location(mia_c: str, pow_c: str, woj_c: str) -> str:
    if (mia_c or '') == 'warszawa':
        return 'warsaw_city'
    if (mia_c or '') in VOIVODE_CAPITALS:
        return 'voiv_capital'
    aglo = getattr(classify_location, '_aglo_cache', None)
    if aglo is None:
        try:
            aglo = load_warsaw_agglomeration()
        except (OSError, ValueError) as e:
            logger.debug("Nie udalo sie wczytac aglomeracji: %s", e)
            aglo = set(AGLO_WARSZAWA_DEFAULT)
        setattr(classify_location, '_aglo_cache', aglo)
    mia_local = _canon_local(mia_c or '')
    if mia_local in aglo and (woj_c or '') == 'mazowieckie':
        return 'warsaw_aglo'
    return 'normal'

# ---------------------------------------------------------------------------
# Kolumny raportu
# ---------------------------------------------------------------------------

VALUE_COLS = [
    "Średnia cena za m2 ( z bazy)",
    "Średnia skorygowana cena za m2",
    "Statystyczna wartość nieruchomości",
]

HITS_COL = "hits"
STAGE_COL = "stage"

ANCHOR_COL = "Czy udziały?"

MISSING_ADDR_TEXT = "brak adresu"
NO_OFFERS_TEXT = "brak ogłoszeń w zakresie"
MISSING_AREA_TEXT = "brak metrażu"

def ensure_report_columns(df_report: pd.DataFrame) -> None:
    """Upewnia sie, ze kolumny raportu istnieja."""
    if df_report is None:
        return
    for c in [HITS_COL, STAGE_COL, *VALUE_COLS]:
        if c not in df_report.columns:
            df_report[c] = np.nan

def reorder_report_columns(df_report: pd.DataFrame) -> pd.DataFrame:
    """Zmienia kolejnosc kolumn raportu."""
    if df_report is None or df_report.empty:
        return df_report
    ensure_report_columns(df_report)

    cols = list(df_report.columns)

    if ANCHOR_COL not in cols:
        return df_report

    desired = [HITS_COL, STAGE_COL, VALUE_COLS[0]]

    for c in desired:
        if c in cols:
            cols.remove(c)

    pos = cols.index(ANCHOR_COL) + 1
    cols[pos:pos] = desired

    for c in VALUE_COLS[1:]:
        if c not in cols:
            cols.append(c)

    return df_report.reindex(columns=cols)

def _ensure_value_cols(df_report: pd.DataFrame) -> None:
    ensure_report_columns(df_report)

def _iter_m2_steps(max_margin: float, step: float = 2.0) -> List[float]:
    """Generuje liste krokow marginesu m2."""
    try:
        max_m = float(max_margin)
    except (ValueError, TypeError):
        max_m = 0.0
    try:
        st = float(step)
    except (ValueError, TypeError):
        st = 3.0
    if st <= 0:
        st = 3.0
    if max_m <= 0:
        return []
    steps: List[float] = []
    k = 1
    while k * st < max_m - 1e-9:
        steps.append(round(k * st, 6))
        k += 1
        if k > 10_000:
            break
    steps.append(float(max_m))
    return steps

def _select_candidates_dynamic_margin(
    pl: PolskaIndex,
    base_mask: pd.Series,
    area_target: float,
    max_margin_m2: float,
    step_m2: float,
    prefer_mask: pd.Series | None,
    min_hits: int,
) -> tuple[pd.DataFrame, float, bool]:
    """Wybiera kandydatow z dynamicznym marginesem m2."""
    best_df = pl.df.iloc[0:0].copy()
    best_margin = 0.0
    best_used_pref = False

    steps = _iter_m2_steps(max_margin_m2, step_m2)
    last_tried = float(steps[-1]) if steps else 0.0

    for m in steps:
        if prefer_mask is not None:
            df1 = pl.df[base_mask & prefer_mask].copy()
            df1 = df1[df1["_price_num"].notna()].copy()
            df1 = df1[df1["_area_num"].notna()].copy()
            df1 = df1[(df1["_area_num"] - area_target).abs() <= m].copy()
            if len(df1.index) > len(best_df.index):
                best_df, best_margin, best_used_pref = df1, m, True
            if len(df1.index) >= min_hits:
                return df1, m, True

        df2 = pl.df[base_mask].copy()
        df2 = df2[df2["_price_num"].notna()].copy()
        df2 = df2[df2["_area_num"].notna()].copy()
        df2 = df2[(df2["_area_num"] - area_target).abs() <= m].copy()
        if len(df2.index) > len(best_df.index):
            best_df, best_margin, best_used_pref = df2, m, False
        if len(df2.index) >= min_hits:
            return df2, m, False

    if best_df is None or len(best_df.index) == 0:
        best_margin = last_tried
    return best_df, best_margin, best_used_pref

def _build_stage_masks(
    pl: PolskaIndex,
    woj_c: str,
    pow_c: str,
    gmi_c: str,
    mia_c: str,
    loc_class: str,
    *,
    bucket_low: float | None = None,
    bucket_high: float | None = None,
    pop_resolver: PopulationResolver | None = None,
    woj_raw: str = "",
    pow_raw: str = "",
    gmi_raw: str = "",
    pop_cache: Dict[Tuple[str, str], float | None] | None = None,
) -> list[tuple[str, pd.Series, int, str]]:
    """Buduje maski etapow filtrowania."""
    df_pl = pl.df
    masks: list[tuple[str, pd.Series, int, str]] = []

    if pop_cache is None:
        pop_cache = {}

    def _bucket_tag() -> str:
        if bucket_low is None and bucket_high is None:
            return ""
        if bucket_low is not None and bucket_high is None:
            return f"|bucket>={int(bucket_low)}"
        if bucket_low is None and bucket_high is not None:
            return f"|bucket<{int(bucket_high)}"
        return f"|bucket={int(bucket_low)}-{int(bucket_high)}"

    def _min_hits(stage_name: str) -> int:
        if stage_name == "woj":
            return 30
        if stage_name in ("pow", "gmi"):
            return 12
        if stage_name == "aglo":
            return 12
        return 6

    base = pd.Series(True, index=df_pl.index)

    _price_col_abs = df_pl["_price_num"] if "_price_num" in df_pl.columns else None
    if _price_col_abs is not None:
        base &= _price_col_abs.between(PRICE_FLOOR_PLN_M2, PRICE_CEIL_PLN_M2, inclusive="both").fillna(False)

    if pl.c_woj and woj_c:
        base &= _mask_eq_canon(df_pl, pl.c_woj, woj_c)

    if loc_class in ("voiv_capital", "warsaw_city"):
        if pl.c_mia and mia_c:
            masks.append(("miasto", base & _mask_eq_canon(df_pl, pl.c_mia, mia_c), _min_hits("miasto"), ""))
        else:
            masks.append(("woj", base, _min_hits("woj"), ""))
        return masks

    if loc_class == "warsaw_aglo":
        aglo = getattr(classify_location, "_aglo_cache", None)
        if aglo is None:
            try:
                aglo = load_warsaw_agglomeration()
            except (OSError, ValueError) as e:
                logger.debug("Nie udalo sie wczytac aglomeracji: %s", e)
                aglo = set(AGLO_WARSZAWA_DEFAULT)
            setattr(classify_location, "_aglo_cache", aglo)

        if pl.c_mia:
            aglo_canon = {_canon_local(x) for x in aglo}
            col = df_pl[pl.c_mia].astype(str).map(lambda x: _canon_local(x))
            masks.append(("aglo", base & col.isin(aglo_canon), _min_hits("aglo"), ""))
        else:
            masks.append(("woj", base, _min_hits("woj"), ""))
        return masks

    if pl.c_pow and pow_c and pl.c_gmi and gmi_c and pl.c_mia and mia_c:
        masks.append((
            "pow+gmi+miasto",
            base
            & _mask_eq_canon(df_pl, pl.c_pow, pow_c)
            & _mask_eq_canon(df_pl, pl.c_gmi, gmi_c)
            & _mask_eq_canon(df_pl, pl.c_mia, mia_c),
            _min_hits("miasto"),
            "",
        ))

    if pl.c_gmi and gmi_c and pl.c_mia and mia_c:
        masks.append((
            "gmi+miasto",
            base
            & _mask_eq_canon(df_pl, pl.c_gmi, gmi_c)
            & _mask_eq_canon(df_pl, pl.c_mia, mia_c),
            _min_hits("miasto"),
            "",
        ))

    if pl.c_pow and pow_c and pl.c_mia and mia_c:
        masks.append((
            "pow+miasto",
            base
            & _mask_eq_canon(df_pl, pl.c_pow, pow_c)
            & _mask_eq_canon(df_pl, pl.c_mia, mia_c),
            _min_hits("miasto"),
            "",
        ))

    if pl.c_mia and mia_c:
        masks.append(("miasto", base & _mask_eq_canon(df_pl, pl.c_mia, mia_c), _min_hits("miasto"), ""))

    if pl.c_gmi and gmi_c:
        mm = base & _mask_eq_canon(df_pl, pl.c_gmi, gmi_c)
        meta = ""
        if pl.c_mia:
            candidates = pl.by_gmina.get((str(woj_c), str(pow_c), str(gmi_c)), {})
            if candidates and (bucket_low is not None or bucket_high is not None):
                allowed = _filter_miejscowosci_by_bucket(
                    candidates=candidates,
                    bucket_low=bucket_low,
                    bucket_high=bucket_high,
                    pop_resolver=pop_resolver,
                    woj_raw=woj_raw,
                    pow_raw=pow_raw,
                    gmi_raw=gmi_raw,
                    scope=f"gmi:{woj_c}:{pow_c}:{gmi_c}",
                    pop_cache=pop_cache,
                )
                if not allowed:
                    allowed = list(candidates.keys())
                    meta = _bucket_tag() + "|bucket_fallback"
                else:
                    meta = _bucket_tag()
                mm &= df_pl[pl.c_mia].astype(str).isin(allowed)
        masks.append(("gmi", mm, _min_hits("gmi"), meta))

    if pl.c_pow and pow_c:
        mm = base & _mask_eq_canon(df_pl, pl.c_pow, pow_c)
        meta = ""
        if pl.c_mia:
            candidates = pl.by_powiat.get((str(woj_c), str(pow_c)), {})
            if candidates and (bucket_low is not None or bucket_high is not None):
                allowed = _filter_miejscowosci_by_bucket(
                    candidates=candidates,
                    bucket_low=bucket_low,
                    bucket_high=bucket_high,
                    pop_resolver=pop_resolver,
                    woj_raw=woj_raw,
                    pow_raw=pow_raw,
                    gmi_raw="",
                    scope=f"pow:{woj_c}:{pow_c}",
                    pop_cache=pop_cache,
                )
                if not allowed:
                    allowed = list(candidates.keys())
                    meta = _bucket_tag() + "|bucket_fallback"
                else:
                    meta = _bucket_tag()
                mm &= df_pl[pl.c_mia].astype(str).isin(allowed)
        masks.append(("pow", mm, _min_hits("pow"), meta))

    mm = base
    meta = ""
    if pl.c_mia:
        candidates = pl.by_woj.get(str(woj_c), {})
        if candidates and (bucket_low is not None or bucket_high is not None):
            allowed = _filter_miejscowosci_by_bucket(
                candidates=candidates,
                bucket_low=bucket_low,
                bucket_high=bucket_high,
                pop_resolver=pop_resolver,
                woj_raw=woj_raw,
                pow_raw="",
                gmi_raw="",
                scope=f"woj:{woj_c}",
                pop_cache=pop_cache,
            )
            if not allowed:
                allowed = list(candidates.keys())
                meta = _bucket_tag() + "|bucket_fallback"
            else:
                meta = _bucket_tag()
            mm &= df_pl[pl.c_mia].astype(str).isin(allowed)
    masks.append(("woj", mm, _min_hits("woj"), meta))

    uniq: list[tuple[str, pd.Series, int, str]] = []
    seen = set()
    for name, mm, mh, meta in masks:
        key = (name, int(mm.sum()), mh, meta)
        if key in seen:
            continue
        seen.add(key)
        uniq.append((name, mm, mh, meta))
    return uniq

def _process_row(
    df_raport: pd.DataFrame,
    idx: int,
    pl: PolskaIndex,
    margin_m2_default: float = 15.0,
    margin_pct_default: float = 15.0,
    pop_resolver: PopulationResolver | None = None,
    *,
    min_hits: int = 5,
    step_m2: float = 3.0,
) -> None:
    """Przetwarza pojedynczy wiersz raportu."""
    if df_raport is None or idx < 0 or idx >= len(df_raport.index):
        return

    ensure_report_columns(df_raport)
    row = df_raport.iloc[idx]
    row_key = df_raport.index[idx]

    def _set_status(avg_text: str, hits: int, stage: str) -> None:
        df_raport.at[row_key, HITS_COL] = int(hits) if hits is not None else 0
        df_raport.at[row_key, STAGE_COL] = stage
        df_raport.at[row_key, VALUE_COLS[0]] = avg_text
        df_raport.at[row_key, VALUE_COLS[1]] = np.nan
        df_raport.at[row_key, VALUE_COLS[2]] = np.nan

    def _set_values(avg: float, corrected: float, value: float, hits: int, stage: str) -> None:
        df_raport.at[row_key, HITS_COL] = int(hits) if hits is not None else 0
        df_raport.at[row_key, STAGE_COL] = stage
        df_raport.at[row_key, VALUE_COLS[0]] = avg
        df_raport.at[row_key, VALUE_COLS[1]] = corrected
        df_raport.at[row_key, VALUE_COLS[2]] = value

    col_woj = _find_col(df_raport.columns, ["Województwo", "Wojewodztwo", "woj"])
    col_pow = _find_col(df_raport.columns, ["Powiat"])
    col_gmi = _find_col(df_raport.columns, ["Gmina"])
    col_mia = _find_col(df_raport.columns, ["Miejscowość", "Miejscowosc", "Miasto", "miejsc"])
    col_dzl = _find_col(df_raport.columns, ["Dzielnica", "Osiedle"])
    col_area = _find_col(df_raport.columns, ["Obszar", "metry", "powierzchnia"])

    if not (col_woj and col_pow and col_gmi and col_mia):
        _set_status(MISSING_ADDR_TEXT, 0, "brak_kolumn_adresu")
        return

    woj_raw = _trim_after_semicolon(row[col_woj]) if col_woj else ""
    pow_raw = _trim_after_semicolon(row[col_pow]) if col_pow else ""
    gmi_raw = _trim_after_semicolon(row[col_gmi]) if col_gmi else ""
    mia_raw = _trim_after_semicolon(row[col_mia]) if col_mia else ""
    dzl_raw = _trim_after_semicolon(row[col_dzl]) if col_dzl else ""
    area_val = _to_float_maybe(row[col_area]) if col_area else None

    if not woj_raw or not pow_raw or not gmi_raw or not mia_raw:
        _set_status(MISSING_ADDR_TEXT, 0, "brak_adresu")
        return

    if area_val is None:
        _set_status(MISSING_AREA_TEXT, 0, "brak_metrazu")
        return

    woj_c = _canon_admin(woj_raw, "woj")
    pow_c = _canon_admin(pow_raw, "pow")
    gmi_c = _canon_admin(gmi_raw, "gmi")
    mia_c = _canon_admin(mia_raw, "mia")
    dzl_c = _canon_admin(dzl_raw, "dzl")

    pop = None
    if pop_resolver is not None:
        pop = pop_resolver.get_population(woj_raw, pow_raw, gmi_raw, mia_raw, dzl_raw)

    margin_m2, margin_pct = rules_for_population(pop)

    if not (isinstance(margin_m2, (int, float)) and float(margin_m2) > 0):
        margin_m2 = float(margin_m2_default)
    if not isinstance(margin_pct, (int, float)):
        margin_pct = float(margin_pct_default)

    loc_class = classify_location(mia_c, pow_c, woj_c)

    df_pl = pl.df
    prefer_mask = None
    if dzl_c and pl.c_dzl:
        prefer_mask = _mask_eq_canon(df_pl, pl.c_dzl, dzl_c)

    pop_for_bucket = pop
    if pop_resolver is not None and dzl_raw:
        try:
            pop_city = pop_resolver.get_population(woj_raw, pow_raw, gmi_raw, mia_raw, "")
            if pop_city is not None:
                pop_for_bucket = pop_city
        except (ValueError, TypeError):
            pass

    bucket_low, bucket_high = _bucket_for_population(pop_for_bucket)
    pop_cache_local: Dict[Tuple[str, str], float | None] = {}

    _price_valid = pl.df["_price_num"].between(PRICE_FLOOR_PLN_M2, PRICE_CEIL_PLN_M2, inclusive="both")
    if not _price_valid.all():
        warnings.warn(
            f"[automat1] Pominieto {(~_price_valid).sum()} wierszy bazy z cena poza "
            f"[{PRICE_FLOOR_PLN_M2:,.0f}-{PRICE_CEIL_PLN_M2:,.0f} zl/m2].",
            stacklevel=2,
        )

    stage_masks = _build_stage_masks(
        pl, woj_c, pow_c, gmi_c, mia_c, loc_class,
        bucket_low=bucket_low,
        bucket_high=bucket_high,
        pop_resolver=pop_resolver,
        woj_raw=woj_raw,
        pow_raw=pow_raw,
        gmi_raw=gmi_raw,
        pop_cache=pop_cache_local,
    )

    best_df = pl.df.iloc[0:0].copy()
    best_hits = 0
    best_used_m = float(margin_m2)
    best_used_dzl = False
    best_stage_name = stage_masks[-1][0] if stage_masks else "woj"
    best_stage_req = stage_masks[-1][2] if stage_masks else int(min_hits)
    best_stage_meta = stage_masks[-1][3] if stage_masks else ""

    for stage_name, base_mask, stage_min_hits, stage_meta in stage_masks:
        stage_min_hits = int(stage_min_hits) if stage_min_hits is not None else int(min_hits)

        cand_df, used_m, used_dzl = _select_candidates_dynamic_margin(
            pl=pl,
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
    stage_base = f"{loc_class}:{best_stage_name}{meta}|m={float(best_used_m):g}|{'dzielnica' if best_used_dzl else 'bez_dzielnicy'}"

    if cand_df is None or cand_n < req_min:
        _set_status(NO_OFFERS_TEXT, cand_n, f"{stage_base}|hits<{req_min}")
        return

    cand_df2, prices = _filter_outliers_df(cand_df, "_price_num")
    avg = float(np.mean(prices)) if prices is not None and len(prices) else None
    if avg is None:
        _set_status(NO_OFFERS_TEXT, cand_n, f"{stage_base}|no_price")
        return

    avg = round(avg, 2)
    corrected = round(float(avg) * (1.0 - (float(margin_pct) / 100.0)), 2)
    value = round(corrected * float(area_val), 2)
    _set_values(avg, corrected, value, cand_n, stage_base)
