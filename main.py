from gtfsdataingestor import GTFSDataIngestor
from gtfstransformerSilver import GTFSTransformer
from gtfsGold import GTFSGold
import time
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark
load_dotenv()


base_url = 'https://api.um.warszawa.pl/api/action/busestrams_get/'
save_path = os.environ['SAVE_PATH']
resource_id = "f2e5503e-927d-4ad3-9500-4ab9e55deb59"
type = '1'

""" gtfs_data_ingestor = GTFSDataIngestor(city_name='WAW',base_url=base_url,save_path=save_path)



for i in range(1,20,1):
    data = gtfs_data_ingestor.get_data(
        resource_id=resource_id,
        type_of_api=type

    )

    gtfs_data_ingestor.save_raw_data(data=data)
    time.sleep(15.0)
 """
year, month, day = 2026, 2, 11

path = f"C:/Users/mateu/python_KURS/GTFS_project/WAW/year={year}/month={month:02d}/day={day:02d}/*.json"

spark = SparkSession.builder.appName('ETL').getOrCreate()

gtfs_silver = GTFSTransformer(spark_session=spark)
gtfs_gold = GTFSGold(spark_session=spark)

df_bronze = gtfs_silver.read_bronze(path=path)

df_silver = gtfs_silver.transform(df_bronze)

df_gold = gtfs_gold.create_daily_report(df_silver=df_silver)

df_gold.show()


