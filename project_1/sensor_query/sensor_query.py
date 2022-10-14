from resources import input_file, output_folder
from resources import query_clean, query_1, query_2
from spark_interface import SparkInterface, save_to_file


def main():
	"""
	A SparkInterface object is instantiated, and it is used to read data from file and load it into a df.
	The usage ot the df structure is based upon the nature of the data, organized in named columns.
	A new df is returned passing the query written to clean data.
	Two more dfs are then produced by querying the cleaned df, and results are saved to CSV files.
	"""
	si = SparkInterface()
	df = si.read_csv_file(input_file)
	df = si.query_dataframe(df, query_clean)
	# query 1:
	df_result_1 = si.query_dataframe(df, query_1)
	save_to_file(df_result_1, output_folder, 'query_1')
	# query 2:
	df_result_2 = si.query_dataframe(df, query_2)
	save_to_file(df_result_2, output_folder, 'query_2')
	return
	
if __name__ == "__main__":
	main()
