from pyspark.sql import Window
from pyspark.sql import functions as F
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, Column
import os

load_dotenv()


class GTFSGold:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        # Parametry biznesowe 💸
        self.FUEL_CONSUMPTION_L_PER_100KM = float(os.environ['FUEL_CONSUMPTION'])
        self.FUEL_PRICE_PLN_PER_L = float(os.environ['FUEL_PRICE'])

    def check_data_quality(self, df: DataFrame) -> DataFrame:
        """Sprawdza, czy w danych nie ma wartości null w kluczowych kolumnach"""
        null_count = df.filter(F.col("Lines").isNull() | F.col("VehicleNumber").isNull()).count()
        if null_count > 0:
            print(f"Ostrzeżenie: Znaleziono {null_count} niekompletnych rekordów!")

    def _haversine_distance(self, lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
        """Oblicza dystans w km między dwoma punktami GPS"""
        R = 6371.0  # Promień Ziemi w km
        
        d_lat = F.radians(lat2 - lat1)
        d_lon = F.radians(lon2 - lon1)
        
        a = (F.sin(d_lat / 2) ** 2 + 
             F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(d_lon / 2) ** 2)
        
        c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
        return R * c

    def _enrich_with_metrics(self,df_silver: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("VehicleNumber").orderBy("Time")



        # 2. Pobieramy poprzednią pozycję
        df_with_prev = df_silver.withColumn("prev_lat", F.lag("Lat").over(window_spec)) \
                                .withColumn("prev_lon", F.lag("Lon").over(window_spec)) \
                                .withColumn("prev_time", F.lag('Time').over(window_spec))

        

        # 3. Liczymy dystans i koszty

        enriched_df = df_with_prev.withColumn(
            "dist_km", 
            F.coalesce(self._haversine_distance(F.col("prev_lat"), F.col("prev_lon"), 
                                                F.col("Lat"), F.col("Lon")), F.lit(0.0))
        )

        

        # Obliczamy zużycie paliwa i koszt dla każdego odcinka
        enriched_df = (
        enriched_df.withColumn(
            "fuel_l", (F.col("dist_km") / 100) * self.FUEL_CONSUMPTION_L_PER_100KM)
        .withColumn(
            "cost_pln", F.col("fuel_l") * self.FUEL_PRICE_PLN_PER_L
        )
        )

        #średni czas 
        enriched_df = (
            enriched_df
            .withColumn('diff_time_sec',(F.unix_timestamp('Time')-F.unix_timestamp('prev_time')))
        )

        #średnia prędkość km/h

        enriched_df = (
            enriched_df
            .withColumn('speed_kmh',
                        F.when(F.col('diff_time_sec')>0,
                            (F.col('dist_km')/F.col('diff_time_sec'))*3600
                            ).otherwise(0.0)
                        )
        )
        enriched_df = enriched_df.select('*').filter(F.col("speed_kmh") <=70)

        return enriched_df



    def create_daily_report(self, df_silver: DataFrame) -> DataFrame:
        self.check_data_quality(df_silver)
        enriched_df = self._enrich_with_metrics(df_silver=df_silver)

        # 4. Agregacja końcowa po liniach
        final_report = enriched_df.groupBy("Lines").agg(
            F.sum("dist_km").alias("total_distance_km"),
            F.sum("cost_pln").alias("total_cost_pln"),
            F.max("dist_km").alias("max_segment_km"), 
            F.count("VehicleNumber").alias("data_points_count"),
            F.avg("speed_kmh").alias("avg_speed"),
            F.max("speed_kmh").alias("max_recorded_speed"),
            F.countDistinct("VehicleNumber").alias("unique_vehicles_count"),
            (F.sum("dist_km") / F.countDistinct("VehicleNumber")).alias("avg_dist_per_vehicle")
        ).orderBy(F.desc("total_cost_pln")).withColumn(
            "cost_of_1km",  F.col("total_cost_pln") / F.nullif(F.col("total_distance_km"), F.lit(0.0))
        )
        

        return final_report
    
    def most_exp_line(self, final_report: DataFrame, df_silver: DataFrame) -> tuple[DataFrame, DataFrame] :
        # 1. Pobieramy numer najdroższej linii (pierwszy wiersz po sortowaniu)
        most_expensive_line = final_report.orderBy(F.desc("total_cost_pln")).first()["Lines"]

        # 2. Automatycznie filtrujemy dane Silver dla tej konkretnej linii
        detailed_silver_df = df_silver.filter(F.col("Lines") == most_expensive_line)

        # 3. Tworzymy tabelę analityczną (np. prędkość i dystans w czasie)
        # Tutaj możemy użyć Window Functions, o których rozmawialiśmy
        analysis_table = self._enrich_with_metrics(df_silver=detailed_silver_df)
        top_vehicle_number = (
            analysis_table
            .groupBy("VehicleNumber")
            .agg(F.sum("dist_km").alias("total_v_dist"))
            .orderBy(F.desc("total_v_dist"))
            .first()["VehicleNumber"]
        )


        top_vehicle_t = (
            analysis_table
            .filter(F.col("VehicleNumber")==top_vehicle_number)

        )
        return analysis_table, top_vehicle_t
    
    def save_gold(self, df: DataFrame, output_path: str) -> None:
        df.write.mode("overwrite").partitionBy("date").parquet(output_path)
