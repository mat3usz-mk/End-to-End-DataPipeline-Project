import requests
from datetime import datetime
import os
import json


class GTFSDataIngestor:
    def __init__(self,city_name, base_url, save_path):
        self.city_name = city_name
        self.base_url = base_url
        self.save_path = save_path

    def get_data(self):
        try: 
            response = requests.get(f'{self.base_url}/{self.city_name}', timeout=10)
            response.raise_for_status()
            return response.json() 
        except requests.exceptions.RequestException as e:
            print(f"Error with {self.city_name}: {e}")
            return None 


    def save_raw_data(self, data,file_format='json'):

        t_now = datetime.utcnow()
        
        dir_path = f'{self.save_path}/{self.city_name}/year={t_now.strftime('%Y')}/month={t_now.strftime('m')}/day={t_now.strftime('%d')}/'

        os.makedirs(dir_path,exist_ok=True)
        file_path= f'{dir_path}/{self.city_name}_{t_now.strftime('%Y%m%d_%H%m')}.{file_format}'
        with open(file_path, 'w') as f:
            json.dump(data, f)
        

        

