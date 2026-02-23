from datetime import datetime, timezone
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import requests
import os
import json
import logging


load_dotenv()

class GTFSDataIngestor:
    def __init__(self,city_name: str, base_url: str, save_path: str):

        self.city_name = city_name
        self.base_url = base_url
        self.save_path = save_path
        self.api_key = os.environ['APIKEY_WAW']
        retry_strategy = Retry(
            total=3, 
            backoff_factor=1, 
            status_forcelist=[429, 500, 502, 503, 504] 
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.logger = logging.getLogger(f"Ingestor-{city_name}")

    def get_data(self, resource_id: str, type_of_api: str) -> dict | None:
        
        query={
            'resource_id' : resource_id,
            'apikey' : self.api_key,
            'type' : type_of_api
        }

        try:
            self.logger.info(f"Pobieranie danych dla miasta: {self.city_name}...") 
            response = self.session.get(
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


    def save_raw_data(self, data: dict, file_format: str = 'json') -> None:

        t_now = datetime.now(timezone.utc)
        
        dir_path = f'{self.save_path}/{self.city_name}/year={t_now.strftime("%Y")}/month={t_now.strftime("%m")}/day={t_now.strftime("%d")}/'

        os.makedirs(dir_path,exist_ok=True)
        file_path= f'{dir_path}/{self.city_name}_{t_now.strftime("%Y%m%d_%H%M%S")}.{file_format}'
        with open(file_path, 'w') as f:
            json.dump(data, f)
        self.logger.info("Dane zapisane pomyślnie")
        

        

