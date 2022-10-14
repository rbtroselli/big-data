#!/bin/bash

# set up and start hdfs
echo "########## Starting hdfs ##########"
hdfs namenode -format
start-dfs.sh
start-yarn.sh
echo "Wait a few seconds before trying to access the HDFS..." && sleep 3

# download file in a temp folder, unzip it, remove zip
echo -e "\n\n########## Downloading and unzipping file ##########"
mkdir ./temp
wget https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip -P ./temp/
unzip ./temp/*.zip -d ./temp/
rm ./temp/*.zip

# load file on hdfs, remove csv from local
echo -e "\n\n########## Pushing CSV file to hdfs ##########"
hdfs dfs -put ./temp/*.csv hdfs:///sensor_data.txt
rm ./temp/*.csv
rmdir ./temp

# run python script, file is loaded on hdfs
echo -e "\n\n########## Starting PySpark script ##########"
spark-submit ./sensor_query/sensor_query.py

# read csvs inside query_1 and query_2 folders, format for readability giving csv sep and display sep, show only first 10 rows with head
echo -e "\n\n########## Printing query 1 results head ##########"
hdfs dfs -cat hdfs:///results/query_1/*.csv | column -s ";" -t | head -10
sleep 3
echo -e "\n\n########## Printing query 2 results head ##########"
hdfs dfs -cat hdfs:///results/query_2/*.csv | column -s ";" -t | head -10
