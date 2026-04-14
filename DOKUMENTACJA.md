# Price-Bot — Dokumentacja techniczna

## 1. Opis projektu

Price-Bot to narzedzie do analizy cen nieruchomosci na polskim rynku. System:
- zbiera oferty z portalu Otodom.pl (scraping)
- czyści i standaryzuje adresy wg bazy TERYT
- wycenia nieruchomosci na podstawie porównywalnych ofert z uwzglednieniem wielkosci miejscowosci
- generuje raporty cenowe w formacie Excel

Projekt jest napisany w Python 3.11+ i korzysta z GUI opartego na Tkinter.

---

## 2. Wymagania

### Zależności (requirements.txt)
```
requests, beautifulsoup4, lxml       # scraping
pandas, numpy, openpyxl              # przetwarzanie danych
selenium                             # scraping (opcjonalnie)
python-dateutil, urllib3              # narzedzia sieciowe
charset-normalizer, idna             # kodowanie
pytest                               # testy (dev)
```

### Pliki danych (wymagane do działania)
| Plik | Opis |
|------|------|
| `Polska.xlsx` | Baza ogłoszeń nieruchomości (generowana przez scraper + scalanie) |
| `ludnosc.csv` | Dane o ludności miejscowości (format: woj;powiat;gmina;miejscowosc;ludnosc) |
| `teryt.csv` | Baza jednostek administracyjnych (TERYT) |
| `obszar_sadow.xlsx` | Obszary sądów (do czyszczenia adresów) |
| `aglomeracja_warszawska.xlsx` | Lista miejscowości aglomeracji warszawskiej |
| `config.json` | Konfiguracja progów i parametrów |

---

## 3. Konfiguracja (config.json)

```json
{
    "PRICE_FLOOR_PLN_M2": 2000.0,      // min cena zł/m² (odrzuca błędy danych)
    "PRICE_CEIL_PLN_M2": 40000.0,       // max cena zł/m²
    "POP_MARGIN_RULES": [               // reguły marginesów wg ludności
        [0, 6000, 20.0, 10.0],          // [min_pop, max_pop, margin_m², margin_%]
        [6000, 20000, 15.0, 10.0],
        [20000, 50000, 12.0, 10.0],
        [50000, 200000, 10.0, 8.0],
        [200000, null, 8.0, 5.0]
    ],
    "DELAY_MIN": 4.0,                   // opóźnienie scrapera (sekundy)
    "DELAY_MAX": 6.0,
    "RETRIES": 3,                       // powtórzenia przy błędach sieciowych
    "SOFT_STOP_MORE": 10,               // ile ogłoszeń dokończyć po kliknięciu Stop
    "BDL_API_KEY_DEFAULT": "..."        // klucz API GUS BDL
}
```

### Tabela progów ludności

| Ludność | Margines m² | Obniżka % | Znaczenie |
|---------|------------|-----------|-----------|
| 0 – 6 000 | ±20 m² | 10% | Małe wsie/miasteczka — szeroki zakres metrażu |
| 6 000 – 20 000 | ±15 m² | 10% | Małe miasta |
| 20 000 – 50 000 | ±12 m² | 10% | Średnie miasta |
| 50 000 – 200 000 | ±10 m² | 8% | Duże miasta |
| 200 000+ | ±8 m² | 5% | Metropolie — najwęższy zakres |

---

## 4. Architektura

```
┌──────────────────────────────────────────────────────────────┐
│                      GUI (Tkinter)                           │
│  ┌─────────────────┐  ┌──────────────────┐                  │
│  │ selektor_csv.py  │  │ bazadanych.py    │                  │
│  │ (Główne okno)    │  │ (Baza danych)    │                  │
│  └────────┬─────────┘  └────────┬─────────┘                  │
└───────────┼──────────────────────┼───────────────────────────┘
            │                      │
            ▼                      ▼
┌───────────────────────┐  ┌───────────────────────────────────┐
│    Obliczenia          │  │    Scraping / zbieranie danych    │
│  ┌──────────────────┐  │  │  ┌────────────────────────────┐  │
│  │ automat.py        │  │  │  │ linki_mieszkania.py        │  │
│  │ (batch CLI)       │  │  │  │ (zbieranie linków)         │  │
│  │       │           │  │  │  └────────────┬───────────────┘  │
│  │       ▼           │  │  │               ▼                  │
│  │ automat1.py       │  │  │  ┌────────────────────────────┐  │
│  │ (algorytm wyceny) │  │  │  │ scraper_otodom_mieszk.py   │  │
│  │       │           │  │  │  │ (scraping ofert)           │  │
│  │       ▼           │  │  │  │       │                    │  │
│  │ manual.py         │  │  │  │       ▼                    │  │
│  │ (ręczna wycena)   │  │  │  │ adres_otodom.py            │  │
│  └──────────────────┘  │  │  │ (ekstrakcja adresów)        │  │
└───────────────────────┘  │  └────────────────────────────┘  │
                           └───────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────────────────┐
│                Czyszczenie adresów                            │
│  CzyszczenieAdresu.py → czyszczenieadresu1.py                │
│                        → czyszczenieadresu2.py                │
└───────────────────────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────────────────┐
│                Narzędzia wspólne                              │
│  utils.py    — normalizacja, config, filtrowanie outlierów    │
│  scalanie.py — łączenie CSV województw w Polska.xlsx          │
│  kolumny.py  — zarządzanie kolumnami Excel                    │
└───────────────────────────────────────────────────────────────┘
```

---

## 5. Pliki źródłowe

### Warstwa główna

| Plik | Linie | Opis |
|------|-------|------|
| `selektor_csv.py` | 904 | Główne okno GUI — podgląd raportu, nawigacja, uruchamianie obliczeń i filtrów |
| `automat.py` | 238 | CLI/GUI do batchowego uruchamiania wycen na całym raporcie |
| `automat1.py` | 1228 | Algorytm wyceny — dynamiczny margines, etapy doboru, PopulationResolver |
| `manual.py` | 650 | Ręczna wycena wiersza — ten sam algorytm co automat1, dodatkowo zapis Excel |
| `bazadanych.py` | 909 | Okno zarządzania bazą danych — scraping po województwach, scalanie |
| `utils.py` | 321 | Wspólne funkcje: normalizacja tekstu, konfiguracja, filtrowanie outlierów |

### Scraping

| Plik | Linie | Opis |
|------|-------|------|
| `linki_mieszkania.py` | 385 | Zbiera linki ofert z Otodom.pl wg województwa |
| `scraper_otodom.py` | 129 | Prosty scraper linków z Otodom |
| `scraper_otodom_mieszkania.py` | 544 | Pełny scraper ofert — wyciąga dane z JSON/DOM |
| `adres_otodom.py` | 243 | Ekstrakcja adresu z oferty Otodom (JSON → DOM fallback) |

### Czyszczenie adresów

| Plik | Linie | Opis |
|------|-------|------|
| `CzyszczenieAdresu.py` | 120 | Orkiestrator pipeline'u (etap 1 → etap 2) |
| `czyszczenieadresu1.py` | 267 | Etap 1: normalizacja, mapowanie starych województw (49→16) |
| `czyszczenieadresu2.py` | 524 | Etap 2: wzbogacanie z bazy TERYT, detekcja stolic |

### Filtry i narzędzia

| Plik | Linie | Opis |
|------|-------|------|
| `jeden_właściciel.py` | 72 | Filtr: zostaw tylko wiersze z jednym właścicielem |
| `LOKAL_MIESZKALNY.py` | 80 | Filtr: zostaw tylko lokale mieszkalne |
| `jeden_właściciel_i_LOKAL_MIESZKALNY.py` | 86 | Filtr: oba powyższe jednocześnie |
| `cofnij.py` | 120 | Cofnij ostatni filtr (przywróć z raport_odfiltrowane) |
| `scalanie.py` | 145 | Łączy regionalne CSV w jeden plik Polska.xlsx |
| `kolumny.py` | 170 | Zarządzanie kolumnami i arkuszami Excel |
| `bootstrap_files.py` | 83 | Tworzenie struktury katalogów projektu |
| `rcn_bydgoski.py` | 483 | Parser danych RCN dla rejonu bydgoskiego |

---

## 6. Algorytm wyceny

Algorytm jest identyczny w `automat1.py` (_process_row) i `manual.py` (compute_and_save_row).

### 6.1. Przepływ ogólny

```
Wiersz raportu
    │
    ▼
1. Walidacja adresu (woj + pow + gmi + mia wymagane)
    │
    ▼
2. Kanonizacja nazw (usunięcie ogonków, prefiksów, nawiasów)
    │
    ▼
3. Pobranie ludności miejscowości (ludnosc.csv → cache → GUS API)
    │
    ▼
4. Ustalenie progów (margin_m², margin_%, bucket populacyjny)
    │
    ▼
5. Klasyfikacja lokalizacji (warsaw_city / voiv_capital / warsaw_aglo / normal)
    │
    ▼
6. Budowa masek etapów (_build_stage_masks)
    │
    ▼
7. Iteracja etapów z dynamicznym marginesem m² (_select_candidates_dynamic_margin)
    │
    ▼
8. Filtrowanie outlierów (percentyle 10–90)
    │
    ▼
9. Obliczenie: średnia → skorygowana (- margin_%) → wartość (× metraż)
    │
    ▼
10. Zapis wyników (zaokrąglone do 2 miejsc po przecinku)
```

### 6.2. Klasyfikacja lokalizacji

Funkcja `classify_location()` przypisuje nieruchomość do jednej z 4 klas:

| Klasa | Warunek | Zachowanie |
|-------|---------|-----------|
| `warsaw_city` | miejscowość = "warszawa" | Jeden etap: "miasto" (min 6 trafień) |
| `voiv_capital` | miejscowość to stolica województwa | Jeden etap: "miasto" (min 6 trafień) |
| `warsaw_aglo` | miejscowość w aglomeracji warszawskiej + woj. mazowieckie | Jeden etap: "aglo" (min 12 trafień) — szuka we wszystkich miejscowościach aglomeracji |
| `normal` | wszystko inne | Pełna sekwencja 7 etapów |

**Stolice województw:**
Białystok, Bydgoszcz, Toruń, Gdańsk, Gorzów Wielkopolski, Katowice, Kielce, Kraków, Lublin, Łódź, Olsztyn, Opole, Poznań, Rzeszów, Szczecin, Warszawa, Wrocław, Zielona Góra

**Aglomeracja warszawska (47 miejscowości):**
Piaseczno, Konstancin-Jeziorna, Góra Kalwaria, Lesznowola, Prażmów, Józefów, Otwock, Celestynów, Karczew, Kółbiel, Wiązowna, Pruszków, Piastów, Brwinów, Michałowice, Nadarzyn, Raszyn, Błonie, Izabelin, Kampinos, Leszno, Stare Babice, Łomianki, Ożarów Mazowiecki, Marki, Ząbki, Zielonka, Wołomin, Kobyłka, Radzymin, Tłuszcz, Jadów, Dąbrówka, Poświętne, Legionowo, Jabłonna, Nieporęt, Serock, Wieliszew, Nowy Dwór Mazowiecki, Czosnów, Leoncin, Pomiechówek, Zakroczym, Grodzisk Mazowiecki, Milanówek, Podkowa Leśna

Lista może być nadpisana plikiem `aglomeracja_warszawska.xlsx`.

### 6.3. Etapy doboru (lokalizacja "normal")

Algorytm przechodzi przez etapy od najwęższego do najszerszego. Zatrzymuje się na pierwszym, który spełnia próg minimalnych trafień.

| Nr | Etap | Zakres geograficzny | Min trafień |
|----|------|-------------------|-------------|
| 1 | pow+gmi+miasto | Powiat + gmina + miejscowość | 6 |
| 2 | gmi+miasto | Gmina + miejscowość | 6 |
| 3 | pow+miasto | Powiat + miejscowość | 6 |
| 4 | miasto | Sama miejscowość | 6 |
| 5 | gmi | Gmina — tylko miejscowości w tym samym progu ludności | 12 |
| 6 | pow | Powiat — tylko miejscowości w tym samym progu ludności | 12 |
| 7 | woj | Województwo — tylko miejscowości w tym samym progu ludności | 30 |

Na etapach 5–7 stosowane jest **filtrowanie populacyjne (bucket)**: oferty brane są tylko z miejscowości o zbliżonej liczbie ludności.

### 6.4. Dynamiczne kroki marginesu metrażu

Na każdym etapie algorytm NIE stosuje od razu pełnego marginesu m². Zamiast tego poszerza zakres krokami co 3 m²:

```
Przykład: nieruchomość 80 m², max margines 15 m²

Krok 1: szukam w [77–83 m²] → za mało? →
Krok 2: szukam w [74–86 m²] → za mało? →
Krok 3: szukam w [71–89 m²] → za mało? →
Krok 4: szukam w [68–92 m²] → za mało? →
Krok 5: szukam w [65–95 m²] → wystarczająco? → STOP
```

Jeśli istnieje dzielnica, algorytm najpierw próbuje z preferencją dzielnicy (prefer_mask), a dopiero potem bez niej — na **każdym** kroku i **każdym** etapie.

### 6.5. Filtrowanie outlierów

Dwuetapowe filtrowanie cen (`_filter_outliers_df`):

1. **Limity absolutne**: odrzuć oferty z ceną < 2 000 zł/m² lub > 40 000 zł/m²
2. **Percentyle 10–90**: z pozostałych weź tylko te między 10. a 90. percentylem

Jeśli po filtrowaniu zostaje < 2 oferty, algorytm cofa się do szerszego zbioru.

### 6.6. Obliczenie ceny

```
średnia = round(mean(ceny_po_filtrowaniu), 2)
skorygowana = round(średnia × (1 - margin_pct / 100), 2)
wartość = round(skorygowana × metraż, 2)
```

Wszystkie wartości zaokrąglane do 2 miejsc po przecinku.

---

## 7. Pipeline scrapingu

### 7.1. Przepływ

```
1. linki_mieszkania.py
   │  Zbiera linki ofert z Otodom dla wybranego województwa
   │  Zapisuje do: linki/{Województwo}.csv
   │  Tworzy marker .done po zakończeniu
   ▼
2. scraper_otodom_mieszkania.py
   │  Wchodzi na każdy link i wyciąga dane oferty
   │  Używa: adres_otodom.py (ekstrakcja adresu)
   │  Zapisuje do: województwa/{Województwo}.csv
   ▼
3. scalanie.py
   │  Łączy wszystkie CSV z województwa/ w jeden plik
   │  Zapisuje: Polska.xlsx (arkusz zbiorczy + per województwo)
   ▼
4. Polska.xlsx gotowa do użycia przez algorytm wyceny
```

### 7.2. Pola wyciągane z oferty

| Pole | Źródło |
|------|--------|
| cena | JSON / DOM |
| cena_za_metr | JSON / obliczona |
| metry (powierzchnia) | JSON / DOM |
| liczba_pokoi | JSON / DOM |
| pietro | JSON / DOM |
| rynek (pierwotny/wtórny) | JSON / DOM |
| rok_budowy | JSON / DOM |
| material | JSON / DOM |
| wojewodztwo, powiat, gmina, miejscowosc, dzielnica, ulica | adres_otodom.py |
| link | URL oferty |

### 7.3. Zabezpieczenia scrapera

- Losowe opóźnienia między requestami (config: DELAY_MIN – DELAY_MAX)
- Rotacja User-Agent
- Powtórzenia przy błędach (config: RETRIES)
- Miękkie zatrzymanie (SOFT_STOP_MORE — dokończ N ogłoszeń po kliknięciu Stop)
- Atomiczny zapis plików (tmp → rename)
- File locking (threading.Lock) przy równoczesnym zapisie

---

## 8. Pipeline czyszczenia adresów

Uruchamiany z GUI (przycisk "Czyszczenie Pliku") lub CLI.

### Etap 1 (czyszczenieadresu1.py)
- Normalizacja tekstu (lowercase, usunięcie diakrytyków)
- Mapowanie historycznych województw (49 → 16):
  - Płockie → MAZOWIECKIE
  - Wrocławskie → DOLNOŚLĄSKIE
  - itd. (pełna mapa 49 → 16)
- Rozpoznawanie aktualnych województw (ochrona przed "opolskie" ⊂ "wielkopolskie")

### Etap 2 (czyszczenieadresu2.py)
- Wzbogacanie adresów z bazy TERYT
- Uzupełnianie brakujących pól (powiat, gmina)
- Detekcja stolic województw
- Wpis "brak adresu" do kolumn cenowych gdy adres niekompletny

---

## 9. GUI — opis interfejsu

### 9.1. Selektor CSV (selektor_csv.py) — główne okno

Sekcje interfejsu:
1. **Plik raportu** — wybór pliku XLSX/CSV, przycisk "Czyszczenie Pliku"
2. **Folder bazowy** — ścieżka do folderu z Polska.xlsx, przycisk "Przygotowanie Aplikacji"
3. **Baza danych** — otwiera okno zarządzania scrapingiem
4. **Filtry** — wybór filtra (jeden właściciel / lokal mieszkalny / oba / cofnij)
5. **Folder wyników** — gdzie zapisać pliki (Nr KW).xlsx
6. **Sterowanie** — nawigacja po wierszach, skok do Nr KW
7. **Obliczenia** — margines m², obniżka %, przycisk "Oblicz i zapisz", "Automat"
8. **Podgląd** — wyświetla bieżący wiersz raportu

### 9.2. Baza danych (bazadanych.py)

Okno zarządzania pobieraniem danych:
- Lista 16 województw ze statusem (faza, postęp, %)
- Przycisk Start/Wznów — uruchamia scraping dla wybranego regionu
- Przycisk Zatrzymaj — miękkie zatrzymanie (dokańcza bieżące)
- Przycisk Scal do Polska.xlsx — łączy wszystkie CSV
- Auto-refresh co 2 sekundy
- Pipeline: linki → ogłoszenia → automatycznie

---

## 10. PopulationResolver — rozwiązywanie ludności

Hierarchia źródeł:

```
1. Lokalny ludnosc.csv (klucz: woj|pow|gmi|mia|dzl)
   │  Kolejność prób:
   │  - dokładny klucz (woj+pow+gmi+mia+dzl)
   │  - bez dzielnicy (woj+pow+gmi+mia)
   │  - gmina (woj+pow+gmi)
   │  - powiat (woj+pow)
   │  - województwo (woj)
   │  + fallback po samym woj+miejscowość
   ▼
2. Cache API (population_cache.csv)
   ▼
3. GUS BDL API (https://bdl.stat.gov.pl/api/v1)
   │  - wyszukuje jednostkę administracyjną po nazwie
   │  - pobiera zmienną "ludność ogółem"
   │  - cachuje wynik na dysku
```

---

## 11. Struktura katalogów

```
Price-Bot/
├── config.json              # konfiguracja
├── requirements.txt         # zależności Python
├── Polska.xlsx              # baza ogłoszeń (generowana)
├── ludnosc.csv              # dane o ludności
├── teryt.csv                # baza TERYT
├── aglomeracja_warszawska.xlsx
├── obszar_sadow.xlsx
├── dzielnice.json
│
├── linki/                   # CSV z linkami do ofert (per województwo)
│   ├── Dolnośląskie.csv
│   ├── Dolnośląskie.done    # marker zakończenia
│   └── ...
│
├── województwa/             # CSV ze scraped danymi (per województwo)
│   ├── Dolnośląskie.csv
│   └── ...
│
├── logs/                    # logi
│   └── pipeline.log
│
├── tests/                   # testy jednostkowe
│   ├── __init__.py
│   └── test_utils.py        # 39 testów
│
└── .github/workflows/
    └── ci.yml               # CI pipeline (pytest + flake8)
```

---

## 12. Uruchamianie

### GUI (główne okno)
```bash
python selektor_csv.py
```

### Automat (batch z CLI)
```bash
python automat.py RAPORT.xlsx FOLDER_BAZY
```

### Czyszczenie adresów
```bash
python CzyszczenieAdresu.py RAPORT.xlsx --teryt teryt.csv --obszar obszar_sadow.xlsx
```

### Baza danych (standalone)
```bash
python bazadanych.py --base FOLDER_BAZY
```

### Testy
```bash
python -m pytest tests/ -v
```

---

## 13. Logowanie

System korzysta z modułu `logging` Python:
- **Konsola**: INFO i wyżej
- **Plik**: `logs/pricebot.log` (jeśli skonfigurowany przez `setup_logging`)
- **Baza danych**: `logs/pipeline.log` (dedykowany logger)
- **Per region**: `logs/{region}_{stage}.log` (subprocess)

Format: `2025-01-15 14:30:00 [automat1] INFO: Start — liczba wierszy w raporcie: 150`

---

## 14. Testy

39 testów jednostkowych w `tests/test_utils.py` pokrywających:
- `norm()`, `plain()` — normalizacja tekstu
- `strip_parentheses()` — usuwanie nawiasów
- `canon_admin()` — kanonizacja nazw administracyjnych
- `find_col()` — wyszukiwanie kolumn
- `trim_after_semicolon()` — obcinanie tekstu
- `to_float_maybe()` — parsowanie liczb z polskim formatem
- `filter_outliers_df()` — filtrowanie outlierów cenowych
- `rules_for_population()` — progi marginesów
- `bucket_for_population()` — buckety populacyjne
- `load_config()` — ładowanie konfiguracji

CI: GitHub Actions (pytest + flake8) na Python 3.11 i 3.12.
