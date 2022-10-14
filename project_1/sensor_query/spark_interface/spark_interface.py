from pyspark.sql import SparkSession

class SparkInterface:
	def __init__(self, spark_session=None):
		self.spark_session = self.get_spark_session()
		pass

	def get_spark_session(self):
		"""
		Instantiate a SparkSession and return it.
		Also disable INFO logging.
		"""
		spark = SparkSession\
			.builder\
			.appName('Python Spark SQL')\
			.getOrCreate()
		spark.sparkContext.setLogLevel('WARN')
		return spark

	def read_csv_file(self, file):
		"""
		Create dataframe from csv file.
		Takes spark sessions and source csv as input, prints and returns dataframe.
		"""
		df = self.spark_session.read.csv(file, sep=';', header='True')
		df.show(10)
		return df

	def query_dataframe(self, df, query):
		"""
		Function to query dataframes. Creates or replace a temp view for the query 
		that is then executed. Results are then shown and the new df is returned.
		Takes spark session, df and the query as input, returns the new dataframe.
		"""
		df.createOrReplaceTempView('sensor_read')
		df = self.spark_session.sql(query)
		df.show(10)
		return df

def save_to_file(df, output_folder, subfolder, partitions=1):
	"""
	Save dataframe to a file. Output is partitioned to one file only, 
	given the small sized nature of the aggregated results data (.coalesce(1)).
	Output format is given as csv, header is output as first row in the file, 
	separatotor is given as ';', and the output directory is given on hdfs.
	Takes df and output subfolder as input.
	"""
	df.coalesce(partitions).write.mode("overwrite")\
		.format('csv')\
		.option('header','true')\
		.option('sep',';')\
		.save(output_folder+subfolder)
	return
