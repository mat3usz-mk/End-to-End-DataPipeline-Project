import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from gtfsGold import GTFSGold


@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
            .master("local")
            .appName("test_gold")
            .getOrCreate())


# patch to "podmiana" zmiennych środowiskowych na czas testu
# dzięki temu nie potrzebujesz pliku .env żeby odpalić testy
@pytest.fixture(scope="session")
def gold(spark):
    with patch.dict("os.environ", {
        "FUEL_CONSUMPTION": "30.0",
        "FUEL_PRICE": "6.50"
    }):
        return GTFSGold(spark_session=spark)


def make_silver_df(spark, records: list[dict]):
    """Tworzy testowy DataFrame Silver (już po transformacji)"""
    from pyspark.sql.types import (
        StructType, StructField, StringType,
        DoubleType, TimestampType, DateType
    )
    from datetime import datetime, date

    schema = StructType([
        StructField("Lines", StringType(), True),
        StructField("VehicleNumber", StringType(), True),
        StructField("Lat", DoubleType(), True),
        StructField("Lon", DoubleType(), True),
        StructField("Time", TimestampType(), True),
        StructField("date", DateType(), True),
    ])
    return spark.createDataFrame(records, schema=schema)


# =============================================================================
# TEST 1
# Haversine — sprawdzamy czy odległość między dwoma punktami GPS
# jest w sensownym zakresie (nie ujemna, nie kosmicznie duża)
# Używamy dwóch punktów w Warszawie (~1 km od siebie)
# =============================================================================
def test_haversine_returns_positive_distance(spark, gold):
    from pyspark.sql import functions as F

    # Dwa punkty w Warszawie: Centrum i Praga
    df = spark.createDataFrame([
        (52.2297, 21.0122, 52.2488, 21.0440)
    ], ["lat1", "lon1", "lat2", "lon2"])

    result = df.withColumn(
        "dist",
        gold._haversine_distance(
            F.col("lat1"), F.col("lon1"),
            F.col("lat2"), F.col("lon2")
        )
    ).collect()[0]["dist"]

    # Oczekujemy dystansu między 1 a 10 km (realne wartości dla Warszawy)
    assert 1.0 < result < 10.0, \
        f"Haversine zwrócił nieoczekiwaną wartość: {result} km"


# =============================================================================
# TEST 2
# create_daily_report — sprawdzamy czy wynikowy raport zawiera
# wszystkie oczekiwane kolumny
# =============================================================================
def test_daily_report_has_expected_columns(spark, gold):
    from datetime import datetime, date

    records = [
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.23, "Lon": 21.01,
         "Time": datetime(2026, 2, 23, 10, 0, 0),
         "date": date(2026, 2, 23)},
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.24, "Lon": 21.02,
         "Time": datetime(2026, 2, 23, 10, 15, 0),
         "date": date(2026, 2, 23)},
    ]
    df = make_silver_df(spark, records)
    report = gold.create_daily_report(df_silver=df)

    expected = {
        "Lines", "total_distance_km", "total_cost_pln",
        "avg_speed", "max_recorded_speed", "unique_vehicles_count",
        "cost_of_1km"
    }
    actual = set(report.columns)
    assert expected.issubset(actual), \
        f"Brakujące kolumny w raporcie: {expected - actual}"


# =============================================================================
# TEST 3
# Sprawdzamy czy speed_kmh jest zawsze >= 0
# (nie może być ujemna — to błąd logiki)
# =============================================================================
def test_enrich_speed_non_negative(spark, gold):
    from datetime import datetime, date

    records = [
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.23, "Lon": 21.01,
         "Time": datetime(2026, 2, 23, 10, 0, 0),
         "date": date(2026, 2, 23)},
        {"Lines": "180", "VehicleNumber": "1001",
         "Lat": 52.24, "Lon": 21.02,
         "Time": datetime(2026, 2, 23, 10, 15, 0),
         "date": date(2026, 2, 23)},
    ]
    df = make_silver_df(spark, records)
    enriched = gold._enrich_with_metrics(df_silver=df)

    from pyspark.sql import functions as F
    negative_speeds = enriched.filter(F.col("speed_kmh") < 0).count()

    assert negative_speeds == 0, \
        f"Znaleziono {negative_speeds} rekordów z ujemną prędkością!"
