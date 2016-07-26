#!/bin/bash

# This script downloads NYC Taxi Trips database. To execute it in Linux:
#	$ chmod +x download_database_nyc_taxi_trips.sh
#	$ ./download_database_nyc_taxi_trips.sh
#
# If it returns this error: /bin/bash^M: bad interpreter: No such file or directory
# there is a problem with the formating. To solve it you have to type:
#	$ sudo apt-get install dos2unix
#	$ dos2unix download_database_nyc_taxi_trips.sh
#	$ ./download_database_nyc_taxi_trips.sh
#

DBDIR=nyc_taxi_trips
echo "Downloading NYC Taxi Trips database in ${DBDIR}"
mkdir ${DBDIR}
cd ${DBDIR}
echo "Downloading trip_data files"
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_1.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_2.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_3.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_4.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_5.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_6.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_7.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_8.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_9.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_10.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_11.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_data_12.csv.zip
echo "Downloading trip_fare files"
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_1.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_2.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_3.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_4.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_5.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_6.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_7.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_8.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_9.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_10.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_11.csv.zip
wget https://nyctaxitrips.blob.core.windows.net/data/trip_fare_12.csv.zip
echo "Downloading finished"
echo "Data stored in ${DBDIR}"