import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, ArrayType
)
from gtfstransformerSilver import GTFSTransformer


# --- FIXTURE ---
# Fixture to specjalny obiekt w pytest który jest tworzony RAZ
# i współdzielony przez wszystkie testy w tym pliku.
# "scope=session" oznacza: stwórz SparkSession tylko raz na cały czas
# trwania testów — bo uruchamianie Sparka to kilka sekund.
@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local")        # tryb lokalny, bez klastra
            .appName("test_silver")
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate())


# Fixture tworząca obiekt transformera — też raz na sesję
@pytest.fixture(scope="session")
def transformer(spark):
    return GTFSTransformer(spark_session=spark)


# Pomocnicza funkcja do tworzenia testowego DataFrame
# zamiast czytać prawdziwe JSONy, tworzymy dane ręcznie
def make_raw_df(spark, records: list[dict]):
    """
    Tworzy DataFrame w takiej samej strukturze jak prawdziwe dane Bronze,
    czyli: {"result": [ {Lines, VehicleNumber, Lat, Lon, Time}, ... ]}
    """
    bus_schema = StructType([
        StructField("Lines", StringType(), True),
        StructField("VehicleNumber", StringType(), True),
        StructField("Lat", DoubleType(), True),
        StructField("Lon", DoubleType(), True),
        StructField("Time", StringType(), True),
    ])
    root_schema = StructType([
        StructField("result", ArrayType(bus_schema), True)
    ])
    return spark.createDataFrame([{"result": records}], schema=root_schema)


# =============================================================================
# TEST 1
# Sprawdzamy czy transform usuwa zduplikowane rekordy
# (ten sam autobus, ten sam czas = duplikat GPS)
# =============================================================================
def test_transform_removes_duplicates(spark, transformer):
    records = [
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.2, "Lon": 21.0, "Time": "2026-02-23 10:00:00"},
        # identyczny rekord — duplikat
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.2, "Lon": 21.0, "Time": "2026-02-23 10:00:00"},
    ]
    df = make_raw_df(spark, records)
    result = transformer.transform(df, current_date="2026-02-23")

    # assert to serce testu:
    # jeśli warunek jest False → test FAIL (coś jest zepsute)
    # jeśli warunek jest True  → test PASS
    assert result.count() == 1, "Duplikat powinien zostać usunięty"


# =============================================================================
# TEST 2
# Sprawdzamy czy rekordy spoza Warszawy są odfiltrowane
# Twój Silver filtruje: Lat między 52.0–52.4, Lon między 20.5–21.5
# =============================================================================
def test_transform_filters_coordinates_outside_warsaw(spark, transformer):
    records = [
        # Kraków — poza zakresem, powinien zniknąć
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 50.06, "Lon": 19.94, "Time": "2026-02-23 10:00:00"},
        # Warszawa — w zakresie, powinien zostać
        {"Lines": "180", "VehicleNumber": "1002",
         "Lat": 52.2,  "Lon": 21.0,  "Time": "2026-02-23 10:01:00"},
    ]
    df = make_raw_df(spark, records)
    result = transformer.transform(df, current_date="2026-02-23")

    assert result.count() == 1, "Rekord spoza Warszawy powinien być odfiltrowany"


# =============================================================================
# TEST 3
# Sprawdzamy czy rekordy z innej daty niż current_date są usuwane
# (np. stare dane z poprzedniego dnia w folderze)
# =============================================================================
def test_transform_filters_wrong_date(spark, transformer):
    records = [
        # właściwa data
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.2, "Lon": 21.0, "Time": "2026-02-23 10:00:00"},
        # zła data — powinien zniknąć
        {"Lines": "180", "VehicleNumber": "1002",
         "Lat": 52.2, "Lon": 21.0, "Time": "2026-02-22 10:00:00"},
    ]
    df = make_raw_df(spark, records)
    result = transformer.transform(df, current_date="2026-02-23")

    assert result.count() == 1, "Rekord z inną datą powinien być odfiltrowany"


# =============================================================================
# TEST 4
# Sprawdzamy czy puste Lines są usuwane
# =============================================================================
def test_transform_filters_empty_lines(spark, transformer):
    records = [
        {"Lines": "",    "VehicleNumber": "1001",
         "Lat": 52.2, "Lon": 21.0, "Time": "2026-02-23 10:00:00"},
        {"Lines": "180", "VehicleNumber": "1002",
         "Lat": 52.2, "Lon": 21.0, "Time": "2026-02-23 10:01:00"},
    ]
    df = make_raw_df(spark, records)
    result = transformer.transform(df, current_date="2026-02-23")

    assert result.count() == 1, "Rekord z pustą linią powinien być odfiltrowany"


# =============================================================================
# TEST 5
# Sprawdzamy kolumny wyjściowe — czy transform zwraca dokładnie te
# kolumny których oczekujemy (kontrakt schematu)
# =============================================================================
def test_transform_output_columns(spark, transformer):
    records = [
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.2, "Lon": 21.0, "Time": "2026-02-23 10:00:00"},
    ]
    df = make_raw_df(spark, records)
    result = transformer.transform(df, current_date="2026-02-23")

    expected_columns = {"Lines", "VehicleNumber", "Lat", "Lon", "Time", "date"}
    assert set(result.columns) == expected_columns, \
        f"Nieoczekiwane kolumny: {set(result.columns)}"
