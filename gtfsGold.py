from pyspark.sql import Window
from pyspark.sql import functions as F

class GTFSGold:
    def __init__(self, spark_session):
        self.spark = spark_session
        # Parametry biznesowe 💸
        self.FUEL_CONSUMPTION_L_PER_100KM = 30.0
        self.FUEL_PRICE_PLN_PER_L = 6.50

    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Oblicza dystans w km między dwoma punktami GPS"""
        R = 6371.0  # Promień Ziemi w km
        
        d_lat = F.radians(lat2 - lat1)
        d_lon = F.radians(lon2 - lon1)
        
        a = (F.sin(d_lat / 2) ** 2 + 
             F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(d_lon / 2) ** 2)
        
        c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
        return R * c

    def create_daily_report(self, df_silver):
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


        # 4. Agregacja końcowa po liniach
        final_report = enriched_df.groupBy("Lines").agg(
            F.sum("dist_km").alias("total_distance_km"),
            F.sum("cost_pln").alias("total_cost_pln"),
            F.max("dist_km").alias("max_segment_km"), 
            F.count("VehicleNumber").alias("data_points_count"),
            F.avg("speed_kmh").alias("avg_speed"),
            F.max("speed_kmh").alias("max_recorded_speed")
        ).orderBy(F.desc("total_cost_pln")).withColumn(
            "cost_of_1km", F.col("total_cost_pln")/ F.col("total_distance_km")
        )
        
        return final_report