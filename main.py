from gtfsdataingestor import GTFSDataIngestor
from gtfstransformerSilver import GTFSTransformer
from gtfsGold import GTFSGold
from mapping import Mapping
from dotenv import load_dotenv
from pyspark.sql import SparkSession

import time
import os
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import argparse

load_dotenv()

logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

base_url = os.environ['BASE_URL']
save_path = os.environ['SAVE_PATH']
resource_id = os.environ['RESOURCE_ID']
api_type = os.environ['API_TYPE']
base_path = os.environ["GTFS_BASE_PATH"]


parser = argparse.ArgumentParser()
parser.add_argument('--mode', choices=['ingest', 'transform'], required=True)
args = parser.parse_args()





if args.mode == 'ingest':
    gtfs_data_ingestor = GTFSDataIngestor(city_name='WAW',base_url=base_url,save_path=save_path)
    for i in range(1,50,1):
        data = gtfs_data_ingestor.get_data(
            resource_id=resource_id,
            type_of_api=api_type

        )
        gtfs_data_ingestor.save_raw_data(data=data)
        time.sleep(15.0)


elif args.mode == 'transform':
    year, month, day = 2026, 2, 23
    current_date = f'{year}-{month:02d}-{day:02d}'

    path = f"{base_path}/year={year}/month={month:02d}/day={day:02d}/*.json"

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





    sns.set_theme(style="whitegrid")

    
    # Tworzymy wykres top 10 linii
    plt.figure(figsize=(10, 6))
    plt.bar(top_10_costs_pd['Lines'], top_10_costs_pd['total_cost_pln'], color='skyblue')

    # Dodajemy opisy osi i tytuł
    plt.xlabel('Numer Linii ')
    plt.ylabel('Całkowity koszt paliwa (PLN) ')
    plt.title('Top 10 najdroższych linii autobusowych ')

    plt.show() 


    top_line = df_analysis.toPandas()


    top_V = top_Vehicle.toPandas()

        
    sns.set_theme(style="darkgrid")
    sns.relplot(data=top_V,x="Time",y="speed_kmh",kind="line",hue='VehicleNumber')
    plt.xlabel('Czas')
    plt.ylabel('Predkosc km/h')
    plt.title('Prędkość autobusu z największym kosztem paliwa ')
    plt.show()

    map = Mapping()

    map.path_map(data_points=top_V)

