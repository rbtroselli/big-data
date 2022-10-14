# big-data

## Project 1
The first project goal is to download and extract data from a given URL in CSV format, load it into a distributed storage, and process it with a distributed processing framework. Tools used are: HDFS as distributed storage, Spark (pySpark) for distributed calculations, bash and python for scripting and coding.

Project_1 script contains all the bash commands needed to download, extract, push to hdfs, run pyspark code, print results in terminal. Commands are commented inside the script (script was tested by running it after cding inside folder).
Going into more details for the Python code, a SparkInterface class is defined to interact with Spark saving state. A few functions are defined to read CSV into Spark, to query data by passing a SQL formatted query, to save dataframe to files. Dataframe is used for convenience and due to the columnar nature of the data, SQL queries are used for readability and standardization.
A main module (sensor_query) instantiates an object from the class, then read CSV and queries it, first for cleaning data from null and out of reasonable range values, and then for obtaining wanted results, that are then saved to files. Paths and queries are imported from respective modules, stored inside resources folder.
Pressure and temperature ranges are obtained by taking max/min values ever registered, and adding/subracting a certain percentage. Null values are substituted with 'N/A' string for readability (at the cost of having a string in the column).
SparkInterface standardizes download and query of the data. Any CSV and any query can be passed to it to obtain different results from the two needed in the project.
Eveything was tested in a Linux VM, results were renamed and save inside results folder. A requirements.txt file is provided for project_1.

## Project 2
The second project goal is to standardize the process done in project_1, repeating the above task monthly, to obtain the two query results every month.

Project_2 expands on the first one, still using batch processing, with monthly frequency. Chosen solution relies on Apache Airflow, using a DAG scheduled to run once every month, after a few days from its start. The dag instantiates a task for each one of the previously mentioned instructions, that are now divided into single bash scripts inside script folder. Scripts are called with BashOperators, including cding to respective folder. Download script has year and month parameterized in the download URL, to get the right new file each month.
The "ETL" is basic and could be largely evolved, for example by: parameterizing hardcoded paths, inserting a sensor for the file with possible retries, inserting try catch logics when downloading files, moving raw data files to a datalake for retention or further interrogation purposes, moving results to another datalake for consulting, managing hdfs and spark nodes, and so on.

https://github.com/rbtroselli/big-data