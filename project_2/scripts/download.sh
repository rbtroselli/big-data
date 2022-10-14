#!/bin/bash

# download file in a temp folder, unzip it, remove zip
# get year and month passed as parameters
echo "########## Downloading and unzipping file ##########"
mkdir ./temp
year=$1; month=$2
wget https://archive.sensor.community/csv_per_month/$year-$month/$year-$month\_bmp180.zip -P ./temp/
unzip ./temp/*.zip -d ./temp/
rm ./temp/*.zip