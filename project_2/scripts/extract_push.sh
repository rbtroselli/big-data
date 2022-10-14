#!/bin/bash

# load file on hdfs, remove csv from local
echo "########## Pushing CSV file to hdfs ##########"
hdfs dfs -put ./temp/*.csv hdfs:///sensor_data.txt
rm ./temp/*.csv
rmdir ./temp
