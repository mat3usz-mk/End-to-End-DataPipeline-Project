from pyspark.sql import Window
import pyspark.sql.functions as F

class GTFSGold:
    def process_to_gold(self, df_silver):

        
        # 1. posegregowanie chronologicznie
        window_spec = Window.partitionBy("VehicleNumber").orderBy("Time")
        
        # 2. używanie poprzednich współrzędnych 
        df_with_prev = df_silver.withColumn("prev_lat", F.lag("Lat").over(window_spec)) \
                                .withColumn("prev_lon", F.lag("Lon").over(window_spec))
        
        # 3. Obliczanie długości każdego odcinka dla poszczególnego autobusu 
        df_dist = df_with_prev.withColumn("dist_km", 
            haversine_distance(F.col("prev_lat"), F.col("prev_lon"), F.col("Lat"), F.col("Lon"))
        ).fillna(0, subset=["dist_km"]) # Pierwszy wiersz trasy będzie miał null, zmieniamy na 0

        # 4. Obliczanie kosztów wypalonego paliwa

        gold_df = df_dist.withColumn("fuel_consumed_L", (F.col("dist_km") / 100) * 30) \
                         .withColumn("cost_pln", F.col("fuel_consumed_L") * 6.50)
        
        # 5. Podsumowanie najważniejszych danych
        summary_df = gold_df.groupBy("Lines").agg(
            F.sum("dist_km").alias("total_distance_km"),
            F.avg("dist_km").alias("avg_segment_km"),
            F.sum("cost_pln").alias("total_cost_pln"),
            F.count("VehicleNumber").alias("total_records")
        )
        
        return summary_df
    
def haversine_distance(lat1, lon1, lat2, lon2):

    R = 6371.0 


    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    delta_phi = F.radians(lat2 - lat1)
    delta_lambda = F.radians(lon2 - lon1)


    a = F.sin(delta_phi / 2)**2 + \
        F.cos(phi1) * F.cos(phi2) * \
        F.sin(delta_lambda / 2)**2
    
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return R * c