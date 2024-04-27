import pandas as pd
import requests

def extract_zone_geometry(url,path_to_save):
    r = requests.get(url)
    taxi_zone_map = r.json()
    taxi_zone = pd.DataFrame(taxi_zone_map['data'])
    taxi_zone.columns = [i['name'] for i in taxi_zone_map['meta']['view']['columns']]
    taxi_zone['LocationID'] = taxi_zone['LocationID'].astype('int')
    taxi_zone.to_csv(path_to_save,index= False)
