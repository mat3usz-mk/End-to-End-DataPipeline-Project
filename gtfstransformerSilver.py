from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
import glob

class GTFSTransformer:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def read_bronze(self, path):
        path = glob.glob(path)
        bus_schema =( StructType([
            StructField("Lines", StringType(), True),
            StructField("VehicleNumber", StringType(), True),
            StructField("Lat", DoubleType(), True),
            StructField("Lon", DoubleType(), True),
            StructField("Time", StringType(), True)
        ]))
        # Główny schemat pliku
        root_schema = StructType([
            StructField("result", ArrayType(bus_schema), True)
        ])

        # Użycie w read_bronze
        return self.spark.read.option("multiline", "true").schema(root_schema).json(path)

    def transform(self, df,current_date):
        df_exploded = df.select(F.explode("result").alias("v"))

        final_df = (
            df_exploded
            .select(
                F.trim(F.col("v.Lines")).alias('Lines'),
                F.trim(F.col("v.VehicleNumber")).alias('VehicleNumber'),
                F.col("v.Lat").cast("double").alias('Lat'),
                F.col("v.Lon").cast("double").alias('Lon'),
                F.col("v.Time").cast("timestamp").alias('Time'),
                F.to_date(F.col("v.Time")).alias('date') 
            )
            .dropna(how='any')
            .filter((F.col("Lat").between(52.0, 52.4)) & (F.col("Lon").between(20.5, 21.5)))
            .filter(F.col('date') == current_date) 
            .dropDuplicates(['VehicleNumber', 'Time'])
            .orderBy(F.col('Lines'), F.col('VehicleNumber'),F.col('Time'))
        )
        

        return final_df


    def save_silver(self, df, output_path):
        df.write.mode("overwrite").partitionBy("date").parquet(output_path)