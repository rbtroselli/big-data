from var import input_file, output_folder
from queries import query_clean, query_1, query_2

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def get_spark_session():
	"""
	Instantiate a SparkSession and return it
	"""
	spark = SparkSession\
		.builder\
		.appName('Python Spark SQL')\
		.getOrCreate()
	return spark

def read_sensor_data(spark, file):
	"""
	Create dataframe from csv file.
	Takes spark sessions and source csv as input, prints and returns dataframe.
	"""
	df = spark.read.csv(file, sep=';', header='True')
	df.show(10)
	return df

def query_dataframe(spark, df, query):
	"""
	Function to query dataframes. Creates or replace a temp view for the query 
	that is then executed. Results are then shown and the new df is returned.
	Takes spark session, df and the query as input, returns the new dataframe.
	"""
	df.createOrReplaceTempView('sensor_read')
	df = spark.sql(query)
	df.show(10)
	return df

def save_to_file(df, subfolder):
	"""
	Save dataframe to a file. Output is partitioned to one file only, 
	given the small sized nature of the aggregated results data (.coalesce(1)).
	Output format is given as csv, header is output as first row in the file, 
	separatotor is given as ';', and the output directory is given on hdfs.
	Takes df and output subfolder as input
	"""
	df.coalesce(1).write.mode("overwrite")\
		.format('csv')\
		.option('header','true')\
		.option('sep',';')\
		.save(output_folder+subfolder)
	return

def main():
	"""
	A Spark sessions is instantiated, and the data is read and returned as a dataframe.
	The usage ot the df structure is based upon the nature of the data, organized in named columns.
	Given the realtively simple nature of queries, sql syntax is used directly on a view 
	created from the spark df (just missing syntax checks while writing queries).
	A clean dataframe is returned keeping the needed and transformed columns.
	Two more queries are executed on the new dataframe, and the results written on two files in hdfs.
	"""
	spark = get_spark_session()
	df = read_sensor_data(spark, input_file)
	df = query_dataframe(spark, df, query_clean)
	save_to_file(query_dataframe(spark, df, query_1), 'query_1')
	save_to_file(query_dataframe(spark, df, query_2), 'query_2')
	return
	
if __name__ == "__main__":
	main()
