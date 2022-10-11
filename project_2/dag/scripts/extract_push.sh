#!/bin/bash

# load file on hdfs, remove csv from local
echo -e "\n\n########## Pushing CSV file to hdfs ##########"
source_file_path=$1
hdfs dfs -put ./temp/*.csv $source_file_path
rm ./temp/*.csv
rmdir ./temp

# run python script, file is loaded on hdfs
echo -e "\n\n########## Starting PySpark script ##########"
spark-submit sensor_query/sensor_query.py