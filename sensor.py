from var import input_file, output_folder
from queries import query_clean, query_1, query_2
from spark_interface import SparkInterface, save_to_file

def main():
	"""
	A Spark sessions is instantiated, and the data is read and returned as a dataframe.
	The usage ot the df structure is based upon the nature of the data, organized in named columns.
	Given the realtively simple nature of queries, sql syntax is used directly on a view 
	created from the spark df (just missing syntax checks while writing queries).
	A clean dataframe is returned keeping the needed and transformed columns.
	Two more queries are executed on the new dataframe, and the results written on two files in hdfs.
	"""
	si = SparkInterface()
	df = si.read_csv_file(input_file)
	df = si.query_dataframe(df, query_clean)

	df_result_1 = si.query_dataframe(df, query_1)
	save_to_file(df_result_1, output_folder, 'query_1')

	df_result_2 = si.query_dataframe(df, query_2)
	save_to_file(df_result_2, output_folder, 'query_2')
	
	return
	
if __name__ == "__main__":
	main()
