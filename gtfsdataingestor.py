import requests
from datetime import datetime
import os
import json
import logging
from dotenv import load_dotenv


load_dotenv()

class GTFSDataIngestor:
    def __init__(self,city_name, base_url, save_path):

        self.city_name = city_name
        self.base_url = base_url
        self.save_path = save_path
        self.api_key = os.environ['APIKEY_WAW']
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f"Ingestor-{city_name}")

    def get_data(self, resource_id, type_of_api):
        
        query={
            'resource_id' : resource_id,
            'apikey' : self.api_key,
            'type' : type_of_api
        }

        try:
            self.logger.info(f"Pobieranie danych dla miasta: {self.city_name}...") 
            response = requests.get(
                url= self.base_url,
                params=query,
                timeout=10
                )
            response.raise_for_status()
            data = response.json()
           
            self.logger.info("Dane pobrane pomyślnie.")
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Błąd podczas pobierania danych: {e}")
            return None


    def save_raw_data(self, data,file_format='json'):

        t_now = datetime.utcnow()
        
        dir_path = f'{self.save_path}/{self.city_name}/year={t_now.strftime("%Y")}/month={t_now.strftime("%m")}/day={t_now.strftime("%d")}/'

        os.makedirs(dir_path,exist_ok=True)
        file_path= f'{dir_path}/{self.city_name}_{t_now.strftime("%Y%m%d_%H%M%S")}.{file_format}'
        with open(file_path, 'w') as f:
            json.dump(data, f)
        self.logger.info("Dane zapisane pomyślnie")
        

        

