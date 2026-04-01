#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cofnij.py
Cofa działanie filtrów:
  - jeden_właściciel.py
  - LOKAL_MIESZKALNY.py
  - jeden_właściciel_i_LOKAL_MIESZKALNY.py

Wszystkie te filtry przenoszą wiersze z arkusza 'raport'
do arkusza 'raport_odfiltrowane'. cofnij.py odwraca tę operację:
przenosi WSZYSTKIE wiersze z 'raport_odfiltrowane' z powrotem do 'raport'
i czyści 'raport_odfiltrowane' (zostawia tylko nagłówek).

Użycie:
  python cofnij.py --in <plik.xlsx>
  python cofnij.py          (okno wyboru pliku)
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd


SHEET_RAPORT = "raport"
SHEET_ODF    = "raport_odfiltrowane"


def _get_arg_value(flag: str) -> str | None:
    if flag not in sys.argv:
        return None
    i = sys.argv.index(flag)
    if i + 1 >= len(sys.argv):
        print(f"[ERR] Brak wartości po {flag}")
        sys.exit(1)
    return sys.argv[i + 1]


def _pick_file_gui() -> Path | None:
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askopenfilename(
            title="Wybierz plik raportu do cofnięcia filtrów",
            filetypes=[("Pliki Excel", "*.xlsx *.xlsm")],
        )
        root.destroy()
        return Path(path) if path else None
    except Exception as e:
        print(f"[ERR] Nie można otworzyć okna wyboru pliku: {e}")
        return None


def cofnij(xlsx: Path) -> None:
    if not xlsx.exists():
        print(f"[ERR] Plik nie istnieje: {xlsx}")
        sys.exit(1)

    xl = pd.ExcelFile(xlsx, engine="openpyxl")
    sheets = xl.sheet_names

    if SHEET_RAPORT not in sheets:
        raport_sheet = sheets[0]
        print(f"[WARN] Brak arkusza '{SHEET_RAPORT}' — używam '{raport_sheet}'")
    else:
        raport_sheet = SHEET_RAPORT

    df_raport = pd.read_excel(xlsx, sheet_name=raport_sheet, engine="openpyxl")

    if SHEET_ODF not in sheets:
        print(f"[INFO] Brak arkusza '{SHEET_ODF}' — nic do cofnięcia.")
        return

    df_odf = pd.read_excel(xlsx, sheet_name=SHEET_ODF, engine="openpyxl")

    if df_odf.empty:
        print(f"[INFO] Arkusz '{SHEET_ODF}' jest pusty — nic do cofnięcia.")
        return

    rows_to_restore = len(df_odf)
    print(f"[INFO] Wierszy do przywrócenia: {rows_to_restore}")
    print(f"[INFO] Wierszy w '{raport_sheet}' przed cofnięciem: {len(df_raport)}")

    # ujednolić kolumny (na wypadek drobnych różnic)
    all_cols = list(dict.fromkeys(list(df_raport.columns) + list(df_odf.columns)))
    df_raport = df_raport.reindex(columns=all_cols, fill_value="")
    df_odf    = df_odf.reindex(columns=all_cols, fill_value="")

    df_restored = pd.concat([df_raport, df_odf], ignore_index=True)
    df_empty_odf = pd.DataFrame(columns=all_cols)

    with pd.ExcelWriter(xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as wr:
        df_restored.to_excel(wr, sheet_name=raport_sheet, index=False)
        df_empty_odf.to_excel(wr, sheet_name=SHEET_ODF, index=False)

    print(f"[OK] Przywrócono {rows_to_restore} wierszy do '{raport_sheet}'")
    print(f"[OK] Arkusz '{SHEET_ODF}' wyczyszczony (zostały tylko nagłówki)")
    print(f"[OK] Łącznie wierszy w '{raport_sheet}': {len(df_restored)}")


def main() -> None:
    raw = _get_arg_value("--in")

    if raw:
        xlsx = Path(raw).expanduser().resolve()
    else:
        xlsx = _pick_file_gui()
        if not xlsx:
            print("[ERR] Nie wybrano pliku.")
            sys.exit(1)
        xlsx = xlsx.resolve()

    cofnij(xlsx)


if __name__ == "__main__":
    main()
