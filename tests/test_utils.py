#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Testy jednostkowe dla utils.py."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# dodaj katalog projektu do sciezki
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import (
    norm,
    plain,
    strip_parentheses,
    canon_admin,
    find_col,
    trim_after_semicolon,
    to_float_maybe,
    filter_outliers_df,
    rules_for_population,
    bucket_for_population,
    load_config,
    reset_config_cache,
)


# ---------------------------------------------------------------------------
# norm()
# ---------------------------------------------------------------------------

class TestNorm:
    def test_basic(self):
        assert norm("  Hello World  ") == "helloworld"

    def test_tabs_nbsp(self):
        assert norm("a\tb\xa0c") == "abc"

    def test_empty(self):
        assert norm("") == ""
        assert norm(None) == ""


# ---------------------------------------------------------------------------
# plain()
# ---------------------------------------------------------------------------

class TestPlain:
    def test_removes_diacritics(self):
        assert plain("Łódź") == "lodz"

    def test_nan(self):
        assert plain(float("nan")) == ""
        assert plain(None) == ""

    def test_normalizes_spaces(self):
        assert plain("  foo   bar  ") == "foo bar"


# ---------------------------------------------------------------------------
# strip_parentheses()
# ---------------------------------------------------------------------------

class TestStripParentheses:
    def test_removes_parens(self):
        assert strip_parentheses("Gmina (miejska)") == "Gmina"

    def test_no_parens(self):
        assert strip_parentheses("Gmina") == "Gmina"


# ---------------------------------------------------------------------------
# canon_admin()
# ---------------------------------------------------------------------------

class TestCanonAdmin:
    def test_removes_prefix(self):
        result = canon_admin("powiat krakowski", "pow")
        assert "krakowski" in result
        assert "powiat" not in result

    def test_removes_gmina(self):
        result = canon_admin("Gmina Miejska Kraków", "gmi")
        assert "krakow" in result

    def test_empty(self):
        assert canon_admin("", "woj") == ""
        assert canon_admin(None, "woj") == ""

    def test_removes_parentheses(self):
        result = canon_admin("Kraków (miasto na prawach powiatu)", "mia")
        assert "krakow" in result


# ---------------------------------------------------------------------------
# find_col()
# ---------------------------------------------------------------------------

class TestFindCol:
    def test_exact_match(self):
        cols = ["Województwo", "Powiat", "Gmina"]
        assert find_col(cols, ["Województwo"]) == "Województwo"

    def test_case_insensitive(self):
        cols = ["WOJEWÓDZTWO", "Powiat"]
        assert find_col(cols, ["województwo"]) == "WOJEWÓDZTWO"

    def test_contains(self):
        cols = ["cena_za_metr2"]
        assert find_col(cols, ["cena_za_metr"]) == "cena_za_metr2"

    def test_not_found(self):
        cols = ["A", "B"]
        assert find_col(cols, ["X", "Y"]) is None


# ---------------------------------------------------------------------------
# trim_after_semicolon()
# ---------------------------------------------------------------------------

class TestTrimAfterSemicolon:
    def test_with_semicolon(self):
        assert trim_after_semicolon("abc; def") == "abc"

    def test_without_semicolon(self):
        assert trim_after_semicolon("abc def") == "abc def"

    def test_none(self):
        assert trim_after_semicolon(None) == ""

    def test_nan(self):
        assert trim_after_semicolon(float("nan")) == ""


# ---------------------------------------------------------------------------
# to_float_maybe()
# ---------------------------------------------------------------------------

class TestToFloatMaybe:
    def test_simple(self):
        assert to_float_maybe("123.45") == 123.45

    def test_with_unit(self):
        assert to_float_maybe("52 m²") == 52.0

    def test_polish_format(self):
        assert to_float_maybe("11 999 zł/m²") == 11999.0

    def test_comma_decimal(self):
        assert to_float_maybe("101,62 m²") == 101.62

    def test_none(self):
        assert to_float_maybe(None) is None

    def test_nan(self):
        assert to_float_maybe(float("nan")) is None

    def test_empty_string(self):
        assert to_float_maybe("") is None


# ---------------------------------------------------------------------------
# filter_outliers_df()
# ---------------------------------------------------------------------------

class TestFilterOutliersDf:
    def test_empty_df(self):
        df = pd.DataFrame({"price": []})
        result_df, prices = filter_outliers_df(df, "price")
        assert len(result_df) == 0
        assert len(prices) == 0

    def test_removes_extreme_values(self):
        data = {"price": [100.0, 5000.0, 6000.0, 7000.0, 8000.0, 50000.0]}
        df = pd.DataFrame(data)
        cfg = {"PRICE_FLOOR_PLN_M2": 2000.0, "PRICE_CEIL_PLN_M2": 40000.0}
        result_df, prices = filter_outliers_df(df, "price", config=cfg)
        assert 100.0 not in prices
        assert 50000.0 not in prices

    def test_few_values_returned(self):
        df = pd.DataFrame({"price": [5000.0, 6000.0]})
        result_df, prices = filter_outliers_df(df, "price")
        assert len(prices) == 2


# ---------------------------------------------------------------------------
# rules_for_population()
# ---------------------------------------------------------------------------

class TestRulesForPopulation:
    def test_small_city(self):
        m2, pct = rules_for_population(3000)
        assert m2 == 20.0
        assert pct == 10.0

    def test_large_city(self):
        m2, pct = rules_for_population(300000)
        assert m2 == 8.0
        assert pct == 5.0

    def test_none(self):
        m2, pct = rules_for_population(None)
        assert isinstance(m2, float)
        assert isinstance(pct, float)


# ---------------------------------------------------------------------------
# bucket_for_population()
# ---------------------------------------------------------------------------

class TestBucketForPopulation:
    def test_none(self):
        assert bucket_for_population(None) == (None, None)

    def test_small(self):
        low, high = bucket_for_population(3000)
        assert low == 0.0
        assert high == 6000.0

    def test_large(self):
        low, high = bucket_for_population(300000)
        assert low == 200000.0
        assert high is None


# ---------------------------------------------------------------------------
# load_config()
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def setup_method(self):
        reset_config_cache()

    def teardown_method(self):
        reset_config_cache()

    def test_returns_dict(self):
        cfg = load_config()
        assert isinstance(cfg, dict)

    def test_has_required_keys(self):
        cfg = load_config()
        assert "PRICE_FLOOR_PLN_M2" in cfg
        assert "PRICE_CEIL_PLN_M2" in cfg
        assert "POP_MARGIN_RULES" in cfg

    def test_nonexistent_file_returns_defaults(self, tmp_path):
        cfg = load_config(tmp_path / "nonexistent.json")
        assert cfg["PRICE_FLOOR_PLN_M2"] == 2000.0
