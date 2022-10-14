#!/bin/bash

# set up and start hdfs
echo "########## Starting hdfs ##########"
hdfs namenode -format
start-dfs.sh
start-yarn.sh
echo "Wait a few seconds before trying to access the HDFS..." && sleep 3