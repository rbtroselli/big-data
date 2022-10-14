#!/bin/bash

# read csvs inside query_1 and query_2 folders, format for readability giving csv sep and display sep, show only first 10 rows with head
echo "########## Printing query 1 results head ##########"
hdfs dfs -cat hdfs:///results/query_1/*.csv | column -s ";" -t | head -10
sleep 3
echo "########## Printing query 2 results head ##########"
hdfs dfs -cat hdfs:///results/query_2/*.csv | column -s ";" -t | head -10