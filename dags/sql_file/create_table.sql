CREATE TABLE IF NOT EXISTS base_dim(
    base_id int PRIMARY KEY,
    hvfhs_license_num VARCHAR(10),
    base_num VARCHAR(10),
    base_name VARCHAR(25),
    licensees VARCHAR(25)
);
CREATE TABLE IF NOT EXISTS datetime_dim(
    datetime_id int PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    pickup_year int,
    pickup_month smallint,
    pickup_day smallint,
    pickup_hour smallint,
    pickup_minute smallint,
    pickup_second smallint,
    pickup_weekday smallint,
    dropoff_datetime TIMESTAMP,
    dropoff_year int,
    dropoff_month smallint,
    dropoff_day smallint,
    dropoff_hour smallint,
    dropoff_minute smallint,
    dropoff_second smallint,
    dropoff_weekday smallint,
    request_datetime TIMESTAMP,
    request_year int,
    request_month smallint,
    request_day smallint,
    request_hour smallint,
    request_minute smallint,
    request_second smallint,
    on_scene_datetime TIMESTAMP,
    on_scene_year int,
    on_scene_month smallint,
    on_scene_day smallint,
    on_scene_hour smallint,
    on_scene_minute smallint,
    on_scene_second smallint
);
CREATE TABLE IF NOT EXISTS location_dim(
    location_id int PRIMARY KEY,
    shape_length float,
    shape_area float,
    "geometry"  TEXT,
    "zone" VARCHAR(50),
    borough VARCHAR(50),
    service_zone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS trip_fact(
    trip_id int PRIMARY KEY,
    dispatching_base_id int,
    originating_base_id int,
    datetime_id int,
    PU_location_id int,
    DO_location_id int,
    trip_miles float,
    trip_time int,
    base_passenger_fare float,
    tolls float,
    bcf float,
    sales_tax float,
    congestion_surcharge float,
    airport_fee BOOLEAN,
    tips float,
    driver_pay float,
    shared_request_flag BOOLEAN,
    shared_match_flag BOOLEAN,
    access_a_ride_flag BOOLEAN,
    wav_request_flag BOOLEAN,
    wav_match_flag BOOLEAN,
    FOREIGN KEY (dispatching_base_id) REFERENCES base_dim(base_id),
    FOREIGN KEY (originating_base_id) REFERENCES base_dim(base_id),
    FOREIGN KEY (PU_location_id) REFERENCES location_dim(location_id),
    FOREIGN KEY (DO_location_id) REFERENCES location_dim(location_id),
    FOREIGN KEY (datetime_id) REFERENCES datetime_dim(datetime_id)
);

TRUNCATE TABLE trip_fact;
TRUNCATE TABLE base_dim CASCADE;
TRUNCATE TABLE location_dim CASCADE;
TRUNCATE TABLE datetime_dim CASCADE;



