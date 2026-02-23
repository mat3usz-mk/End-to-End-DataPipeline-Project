# 🚌 Warsaw Bus GTFS Pipeline

Projekt ETL oparty na **architekturze medalionowej (Bronze → Silver → Gold)**,
który pobiera dane GPS autobusów ZTM Warszawa w czasie rzeczywistym,
transformuje je przy użyciu **PySpark** i generuje raport kosztów paliwa
oraz analizę prędkości dla każdej linii autobusowej.

---

## 📊 Przykładowe wyniki

### Top 10 najdroższych linii autobusowych (szacowany koszt paliwa)
![Top 10 linii](docs/images/Figure_1.jpg)

### Prędkość autobusu z największym kosztem paliwa (linia 189, pojazd #8349)
![Prędkość w czasie](docs/images/Figure_2.jpg)

---

## 🏗️ Architektura

```
API ZTM Warszawa
      │
      ▼
┌─────────────┐
│   BRONZE    │  Surowe dane JSON z API, partycjonowane wg daty
│  (lokalnie) │  year=YYYY/month=MM/day=DD/*.json
└──────┬──────┘
       │  GTFSTransformer (gtfstransformerSilver.py)
       │  -  Walidacja schematu
       │  -  Filtrowanie wg współrzędnych GPS (Warszawa)
       │  -  Usuwanie duplikatów i nullów
       │  -  Rzutowanie typów
       ▼
┌─────────────┐
│   SILVER    │  Oczyszczone dane Parquet, partycjonowane wg daty
│  (Parquet)  │
└──────┬──────┘
       │  GTFSGold (gtfsGold.py)
       │  -  Obliczanie dystansu (formuła Haversine)
       │  -  Szacowanie zużycia paliwa i kosztu
       │  -  Obliczanie prędkości chwilowej (Window Functions)
       │  -  Agregacja dzienna per linia
       ▼
┌─────────────┐
│    GOLD     │  Raport dzienny per linia + analiza najdroższej linii
│  (Parquet)  │  + mapa trasy (Folium HTML)
└─────────────┘
```

---

## 🛠️ Technologie

| Technologia | Zastosowanie |
|---|---|
| **PySpark** | Transformacje Silver i Gold, Window Functions |
| **Requests + Retry** | Pobieranie danych z API ZTM z obsługą błędów |
| **Folium** | Interaktywna mapa trasy autobusu |
| **Seaborn / Matplotlib** | Wizualizacje kosztów i prędkości |
| **python-dotenv** | Zarządzanie konfiguracją przez zmienne środowiskowe |
| **pytest** | Testy jednostkowe transformacji |

---

## 🚀 Uruchomienie

### 1. Klonowanie repozytorium

```bash
git clone https://github.com/TWOJ_USERNAME/NAZWA_REPO.git
cd NAZWA_REPO
```

### 2. Instalacja zależności

```bash
pip install -r requirements.txt
```

### 3. Konfiguracja zmiennych środowiskowych

Skopiuj plik przykładowy i uzupełnij wartości:

```bash
cp .env.example .env
```

Wymagany klucz API do ZTM Warszawa: [api.um.warszawa.pl](https://api.um.warszawa.pl)

### 4. Uruchomienie pipeline'u

**Tryb ingestii** — pobiera dane GPS co 15 sekund (50 razy ≈ ~12 minut):
```bash
python main.py --mode ingest
```

**Tryb transformacji** — przetwarza Bronze → Silver → Gold i generuje wykresy:
```bash
python main.py --mode transform
```

---

## 🧪 Testy

```bash
pytest tests/ -v
```

Testy obejmują:
- Usuwanie duplikatów GPS (`VehicleNumber` + `Time`)
- Filtrowanie współrzędnych spoza Warszawy
- Filtrowanie rekordów z nieprawidłową datą
- Poprawność formuły Haversine
- Schemat wyjściowy warstwy Silver i Gold

---

## 📁 Struktura projektu

```
gtfs_project/
├── main.py                      # Punkt wejścia, argparse (--mode ingest/transform)
├── gtfsdataingestor.py          # Bronze: pobieranie i zapis surowych danych z API
├── gtfstransformerSilver.py     # Silver: walidacja, deduplication, filtrowanie
├── gtfsGold.py                  # Gold: Haversine, koszty paliwa, agregacja
├── mapping.py                   # Generowanie interaktywnej mapy Folium
├── tests/
│   ├── test_silver.py           # Testy jednostkowe transformacji Silver
│   └── test_gold.py             # Testy jednostkowe logiki Gold
├── docs/
│   └── images/                  # Screenshoty outputów
├── .env.example                 # Szablon zmiennych środowiskowych
├── .gitignore
└── requirements.txt
```

---

## 📌 Uwagi

- Dane GPS pobierane są z publicznego API [UM Warszawa](https://api.um.warszawa.pl)
- Koszty paliwa są **szacunkowe** — oparte na parametrach `FUEL_CONSUMPTION` i `FUEL_PRICE` z `.env`
- Filtr prędkości: rekordy > 70 km/h są odrzucane jako anomalie pomiarowe GPS
- Współrzędne filtrowane do obszaru Warszawy: `Lat ∈ [52.0, 52.4]`, `Lon ∈ [20.5, 21.5]`
