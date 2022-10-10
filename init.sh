#!/bin/bash

hdfs namenode -format
start-dfs.sh
start-yarn.sh

echo "Wait a few seconds before trying to access the HDFS..."
