#!/bin/bash

# set up and start hdfs
echo "Starting hdfs"
hdfs namenode -format
start-dfs.sh
start-yarn.sh
echo "Wait a few seconds before trying to access the HDFS..." && sleep 2

# download file, unzip it, remove zip
echo "Downloading file"
wget https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip
echo "File downloaded" && sleep 2
unzip *.zip
echo "File unzipped" && sleep 2
rm *.zip
echo "Archive deleted" && sleep 2

# load file on hdfs, remove csv from local
echo "Pushing file to hdfs"
hdfs dfs -put ./*.csv hdfs:///sensor_data.txt
echo "CSV pushed to hdfs" && sleep 2
rm *.csv
echo "CSV removed from local" && sleep 2

# run python script, file is loaded on hdfs
echo "Starting PySpark script"
spark-submit sensor.py
echo "PySpark script finished run" && sleep 2

# read csvs inside query_1 and query_2 folders, format for readability giving csv sep and display sep, show only first 10 rows with head
echo "Printing query 1 results head"
hdfs dfs -cat hdfs:///query_1/*.csv | column -s ";" -t | head -10
sleep 2
echo "Printing query 2 results head"
hdfs dfs -cat hdfs:///query_2/*.csv | column -s ";" -t | head -10
