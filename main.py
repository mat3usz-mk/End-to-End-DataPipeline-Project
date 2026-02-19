from gtfsdataingestor import GTFSDataIngestor
from gtfstransformerSilver import GTFSTransformer
from gtfsGold import GTFSGold
from mapping import Mapping
import time
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark

import matplotlib.pyplot as plt
import seaborn as sns
import folium

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
current_date = f'{year}-{month:02d}-{day:02d}'

path = f"C:/Users/mateu/python_KURS/GTFS_project/WAW/year={year}/month={month:02d}/day={day:02d}/*.json"

spark = SparkSession.builder.appName('ETL').getOrCreate()

gtfs_silver = GTFSTransformer(spark_session=spark)
gtfs_gold = GTFSGold(spark_session=spark)

df_bronze = gtfs_silver.read_bronze(path=path)

df_silver = gtfs_silver.transform(df_bronze,current_date=current_date)
df_silver.cache()

#df_silver.show()

df_gold = gtfs_gold.create_daily_report(df_silver=df_silver)

#df_gold.show()

df_analysis, top_Vehicle = gtfs_gold.most_exp_line(final_report=df_gold,df_silver=df_silver)

top_10_costs_pd = df_gold.limit(10).toPandas().sort_values(by=['total_cost_pln'],ascending=True)




#df_analysis.show()




# Ustawiamy styl wykresu dla lepszej czytelności
sns.set_theme(style="whitegrid")

"""
# Tworzymy wykres
plt.figure(figsize=(10, 6))
plt.bar(top_10_costs_pd['Lines'], top_10_costs_pd['total_cost_pln'], color='skyblue')

# Dodajemy opisy osi i tytuł
plt.xlabel('Numer Linii 🚌')
plt.ylabel('Całkowity koszt paliwa (PLN) 💰')
plt.title('Top 10 najdroższych linii autobusowych 📊')

plt.show() """


top_line = df_analysis.toPandas()


top_V = top_Vehicle.toPandas()

    
sns.set_theme(style="darkgrid")
sns.relplot(data=top_V,x="Time",y="speed_kmh",kind="line",hue='VehicleNumber')
plt.show()

map = Mapping()

map.path_map(data_points=top_V)