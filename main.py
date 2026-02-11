from gtfsdataingestor import GTFSDataIngestor
import time
import os
from dotenv import load_dotenv

load_dotenv()


base_url = 'https://api.um.warszawa.pl/api/action/busestrams_get/'
save_path = os.environ['SAVE_PATH']
resource_id = "f2e5503e-927d-4ad3-9500-4ab9e55deb59"
type = '1'

gtfs_data_ingestor = GTFSDataIngestor(city_name='WAW',base_url=base_url,save_path=save_path)



for i in range(1,20,1):
    data = gtfs_data_ingestor.get_data(
        resource_id=resource_id,
        type_of_api=type

    )

    gtfs_data_ingestor.save_raw_data(data=data)
    time.sleep(15.0)
