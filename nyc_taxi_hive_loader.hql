create database if not exists nyctaxidb;

create external table if not exists nyctaxidb.trip
(
    medallion string,
    hack_license string,
    vendor_id string,
    rate_code string,
    store_and_fwd_flag string,
    pickup_datetime string,
    dropoff_datetime string,
    passenger_count int,
    trip_time_in_secs double,
    trip_distance double,
    pickup_longitude double,
    pickup_latitude double,
    dropoff_longitude double,
    dropoff_latitude double)  
PARTITIONED BY (month int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' lines terminated by '\n'
STORED AS TEXTFILE LOCATION 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_db/trip' TBLPROPERTIES('skip.header.line.count'='1');

create external table if not exists nyctaxidb.fare
(
    medallion string,
    hack_license string,
    vendor_id string,
    pickup_datetime string,
    payment_type string,
    fare_amount double,
    surcharge double,
    mta_tax double,
    tip_amount double,
    tolls_amount double,
    total_amount double)
PARTITIONED BY (month int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' lines terminated by '\n'
STORED AS TEXTFILE LOCATION 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_db/fare' TBLPROPERTIES('skip.header.line.count'='1');

for /L %i IN (1,1,12) DO (hive -hiveconf MONTH=%i -f "C:\temp\sample_hive_load_data_by_partitions.hql")

LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_1.csv' INTO TABLE nyctaxidb.trip PARTITION (month=1);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_2.csv' INTO TABLE nyctaxidb.trip PARTITION (month=2);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_3.csv' INTO TABLE nyctaxidb.trip PARTITION (month=3);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_4.csv' INTO TABLE nyctaxidb.trip PARTITION (month=4);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_5.csv' INTO TABLE nyctaxidb.trip PARTITION (month=5);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_6.csv' INTO TABLE nyctaxidb.trip PARTITION (month=6);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_7.csv' INTO TABLE nyctaxidb.trip PARTITION (month=7);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_8.csv' INTO TABLE nyctaxidb.trip PARTITION (month=8);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_9.csv' INTO TABLE nyctaxidb.trip PARTITION (month=9);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_10.csv' INTO TABLE nyctaxidb.trip PARTITION (month=10);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_11.csv' INTO TABLE nyctaxidb.trip PARTITION (month=11);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_data_12.csv' INTO TABLE nyctaxidb.trip PARTITION (month=12);

LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_1.csv' INTO TABLE nyctaxidb.fare PARTITION (month=1);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_2.csv' INTO TABLE nyctaxidb.fare PARTITION (month=2);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_3.csv' INTO TABLE nyctaxidb.fare PARTITION (month=3);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_4.csv' INTO TABLE nyctaxidb.fare PARTITION (month=4);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_5.csv' INTO TABLE nyctaxidb.fare PARTITION (month=5);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_6.csv' INTO TABLE nyctaxidb.fare PARTITION (month=6);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_7.csv' INTO TABLE nyctaxidb.fare PARTITION (month=7);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_8.csv' INTO TABLE nyctaxidb.fare PARTITION (month=8);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_9.csv' INTO TABLE nyctaxidb.fare PARTITION (month=9);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_10.csv' INTO TABLE nyctaxidb.fare PARTITION (month=10);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_11.csv' INTO TABLE nyctaxidb.fare PARTITION (month=11);
LOAD DATA INPATH 'wasb://nyctaxihive@maxkazsouthcentralus.blob.core.windows.net/nyc_taxi_data/trip_fare_12.csv' INTO TABLE nyctaxidb.fare PARTITION (month=12);

