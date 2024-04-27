import pandas as pd
import numpy as np

def transform_location_dim(taxi_zone_lookup_path,zone_geometry_path,location_dim_path):
    taxi_zone_lookup = pd.read_csv(taxi_zone_lookup_path)
    zone_geometry = pd.read_csv(zone_geometry_path)
    taxi_zone_lookup.rename(columns = {'Borough':'borough','Zone':'zone'},inplace = True)
    location_dim = taxi_zone_lookup.merge(zone_geometry, on = ['LocationID','borough','zone'], how = 'left')
    location_dim.rename(columns={'LocationID':'location_id','Shape_Leng':'shape_length',\
        'Shape_Area':'shape_area','the_geom':'geometry'},inplace = True)
    location_dim = location_dim[['location_id','shape_length','shape_area','geometry',\
        'zone','borough','service_zone']]
    location_dim[['shape_length','shape_area']] = location_dim[['shape_length','shape_area']].astype('float')
    location_dim.drop_duplicates(subset=['location_id'], inplace=True)
    location_dim.to_csv(location_dim_path,index=False)
    
def transform_datetime_dim(trip_data_path,datetime_dim_path):
    trip_data = pd.read_csv(trip_data_path)
    trip_data = trip_data.drop_duplicates().reset_index(drop=True)
    
    datetime_dim = trip_data.loc[:,['pickup_datetime','dropoff_datetime',
                                    'request_datetime','on_scene_datetime']].reset_index(drop=True)
    
    datetime_dim['pickup_datetime'] = pd.to_datetime(datetime_dim['pickup_datetime'])
    datetime_dim['dropoff_datetime'] = pd.to_datetime(datetime_dim['dropoff_datetime'])
    datetime_dim['request_datetime'] = pd.to_datetime(datetime_dim['request_datetime'])
    datetime_dim['on_scene_datetime'] = pd.to_datetime(datetime_dim['on_scene_datetime'])

    cur_year = datetime_dim['pickup_datetime'].dt.year[0]
    datetime_dim['on_scene_datetime'] = datetime_dim['on_scene_datetime'].apply(lambda x: np.nan if abs(x.year - cur_year) > 1 else x) 
    
    datetime_dim['pickup_datetime'] = datetime_dim['pickup_datetime']
    datetime_dim['pickup_year'] = datetime_dim['pickup_datetime'].dt.year
    datetime_dim['pickup_month'] = datetime_dim['pickup_datetime'].dt.month
    datetime_dim['pickup_day'] = datetime_dim['pickup_datetime'].dt.day
    datetime_dim['pickup_hour'] = datetime_dim['pickup_datetime'].dt.hour
    datetime_dim['pickup_minute'] = datetime_dim['pickup_datetime'].dt.minute
    datetime_dim['pickup_second'] = datetime_dim['pickup_datetime'].dt.second
    datetime_dim['pickup_weekday'] = datetime_dim['pickup_datetime'].dt.weekday

    datetime_dim['dropoff_datetime'] = datetime_dim['dropoff_datetime']
    datetime_dim['dropoff_year'] = datetime_dim['dropoff_datetime'].dt.year
    datetime_dim['dropoff_month'] = datetime_dim['dropoff_datetime'].dt.month
    datetime_dim['dropoff_day'] = datetime_dim['dropoff_datetime'].dt.day
    datetime_dim['dropoff_hour'] = datetime_dim['dropoff_datetime'].dt.hour
    datetime_dim['dropoff_minute'] = datetime_dim['dropoff_datetime'].dt.minute
    datetime_dim['dropoff_second'] = datetime_dim['dropoff_datetime'].dt.second
    datetime_dim['dropoff_weekday'] = datetime_dim['dropoff_datetime'].dt.weekday

    datetime_dim['request_datetime'] = datetime_dim['request_datetime']
    datetime_dim['request_year'] = datetime_dim['request_datetime'].dt.year
    datetime_dim['request_month'] = datetime_dim['request_datetime'].dt.month
    datetime_dim['request_day'] = datetime_dim['request_datetime'].dt.day
    datetime_dim['request_hour'] = datetime_dim['request_datetime'].dt.hour
    datetime_dim['request_minute'] = datetime_dim['request_datetime'].dt.minute
    datetime_dim['request_second'] = datetime_dim['request_datetime'].dt.second

    datetime_dim['on_scene_datetime'] = datetime_dim['on_scene_datetime']
    datetime_dim['on_scene_year'] = datetime_dim['on_scene_datetime'].dt.year
    datetime_dim['on_scene_month'] = datetime_dim['on_scene_datetime'].dt.month
    datetime_dim['on_scene_day'] = datetime_dim['on_scene_datetime'].dt.day
    datetime_dim['on_scene_hour'] = datetime_dim['on_scene_datetime'].dt.hour
    datetime_dim['on_scene_minute'] = datetime_dim['on_scene_datetime'].dt.minute
    datetime_dim['on_scene_second'] = datetime_dim['on_scene_datetime'].dt.second
    
    datetime_dim['on_scene_year'] = datetime_dim['on_scene_year'].astype('Int32')
    datetime_dim['on_scene_month'] = datetime_dim['on_scene_month'].astype('Int32')
    datetime_dim['on_scene_day'] = datetime_dim['on_scene_day'].astype('Int32')
    datetime_dim['on_scene_hour'] = datetime_dim['on_scene_hour'].astype('Int32')
    datetime_dim['on_scene_minute'] = datetime_dim['on_scene_minute'].astype('Int32')
    datetime_dim['on_scene_second'] = datetime_dim['on_scene_second'].astype('Int32')
    
    datetime_dim['datetime_id'] = datetime_dim.index
    
    datetime_dim = datetime_dim.loc[:,['datetime_id',
                                       'pickup_datetime','pickup_year','pickup_month','pickup_day','pickup_hour',
                                       'pickup_minute','pickup_second','pickup_weekday','dropoff_datetime','dropoff_year',
                                       'dropoff_month','dropoff_day','dropoff_hour','dropoff_minute','dropoff_second',
                                       'dropoff_weekday','request_datetime','request_year','request_month','request_day',
                                       'request_hour','request_minute','request_second','on_scene_datetime','on_scene_year',
                                       'on_scene_month','on_scene_day','on_scene_hour','on_scene_minute','on_scene_second']]
    datetime_dim.to_csv(datetime_dim_path,index=False)
    
def transform_base_dim(base_dim_raw_path, base_dim_cleaned_path):
    base_dim = pd.read_csv(base_dim_raw_path)
    base_dim['base_id'] = base_dim.index
    base_dim = base_dim.loc[:,['base_id','hvfhs_license_num',
                            'base_num','base_name','licensees']]
    base_dim.to_csv(base_dim_cleaned_path,index=False)

def transform_trip_fact(trip_data_path,trip_fact_path,base_dim_cleaned_path,datetime_dim_path):
    base_dim = pd.read_csv(base_dim_cleaned_path)
    datetime_dim = pd.read_csv(datetime_dim_path)
    trip_data = pd.read_csv(trip_data_path)
    
    trip_data = trip_data.drop_duplicates().reset_index(drop=True)
    
    trip_data.rename(columns = {'PULocationID':'PU_location_id', 
                                'DOLocationID':'DO_location_id'}, inplace = True)
    
    trip_data['trip_id'] = trip_data.index
    
    trip_data['airport_fee'] = trip_data['airport_fee'].apply(lambda x: False if pd.isnull(x) else True)
    convert_to_boolean = {'N': False, 'Y': True}
    trip_data['shared_request_flag'] = trip_data['shared_request_flag'].map(convert_to_boolean)
    trip_data['shared_match_flag'] = trip_data['shared_match_flag'].map(convert_to_boolean)
    trip_data['access_a_ride_flag'] = trip_data['access_a_ride_flag'].map(convert_to_boolean)
    trip_data['wav_request_flag'] = trip_data['wav_request_flag'].map(convert_to_boolean)
    trip_data['wav_match_flag'] = trip_data['wav_match_flag'].map(convert_to_boolean)
    
    
    dispatching_base_id = pd.merge(trip_data[['hvfhs_license_num','dispatching_base_num']],
                                  base_dim[['base_id','hvfhs_license_num','base_num']], 
                                  left_on=['hvfhs_license_num','dispatching_base_num'], 
                                  right_on=['hvfhs_license_num','base_num'], how='left')
    dispatching_base_id = dispatching_base_id.drop(columns=['hvfhs_license_num','dispatching_base_num','base_num'])
    dispatching_base_id.rename(columns={'base_id':'dispatching_base_id'}, inplace=True)
    dispatching_base_id = dispatching_base_id.astype('Int32')
    
    originate_base_id = pd.merge(trip_data[['hvfhs_license_num','originating_base_num']],
                            base_dim[['base_id','hvfhs_license_num','base_num']], 
                            left_on=['hvfhs_license_num','originating_base_num'], 
                            right_on=['hvfhs_license_num','base_num'], how='left')
    originate_base_id = originate_base_id.drop(columns=['hvfhs_license_num','originating_base_num','base_num'])
    originate_base_id.rename(columns={'base_id':'originating_base_id'}, inplace=True)
    originate_base_id = originate_base_id.astype('Int32')
    
    trip_data = pd.concat([trip_data, dispatching_base_id, originate_base_id], axis=1)
    trip_data = trip_data.drop(columns=['hvfhs_license_num','dispatching_base_num','originating_base_num'])
    
    trip_fact = trip_data.merge(datetime_dim, left_on='trip_id',right_on='datetime_id', how='left')\
                            [['trip_id','dispatching_base_id','originating_base_id','datetime_id', \
                              'PU_location_id','DO_location_id','trip_miles','trip_time','base_passenger_fare', \
                              'tolls','bcf','sales_tax','congestion_surcharge','airport_fee', \
                              'tips','driver_pay','shared_request_flag','shared_match_flag', \
                              'access_a_ride_flag','wav_request_flag','wav_match_flag']]
    
    trip_fact.to_csv(trip_fact_path,index=False)
    
    
    
    