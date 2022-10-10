#!/bin/bash

# set up and start hdfs
echo "--- Starting hdfs ---"
hdfs namenode -format
start-dfs.sh
start-yarn.sh
echo "Wait a few seconds before trying to access the HDFS..." && sleep 3

# download file, unzip it, remove zip
echo "--- Downloading and unzipping file ---"
wget https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip
unzip *.zip
rm *.zip

# load file on hdfs, remove csv from local
echo "--- Pushing CSV file to hdfs ---"
hdfs dfs -put ./*.csv hdfs:///sensor_data.txt
rm *.csv

# run python script, file is loaded on hdfs
echo "--- Starting PySpark script ---"
spark-submit sensor.py

# read csvs inside query_1 and query_2 folders, format for readability giving csv sep and display sep, show only first 10 rows with head
echo "Printing query 1 results head"
hdfs dfs -cat hdfs:///results/query_1/*.csv | column -s ";" -t | head -10
sleep 3
echo "Printing query 2 results head"
hdfs dfs -cat hdfs:///results/query_2/*.csv | column -s ";" -t | head -10
