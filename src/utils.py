import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

load_data_call_count, export_data_call_count = 0, 0

def load_data(spark, file_path):
    """
    Load a CSV file into a Spark DataFrame

    :param spark: the Spark session
    :type spark: pyspark.sql.SparkSession
    :param file_path: the path to the CSV file
    :type file_path: str
    :return a DataFrame containing the data from the CSV file, or None if an error occurs
    :rtype pyspark.sql.DataFrame or None
    :raises Exception: if an error occurs during the loading of the CSV file

    """
    global load_data_call_count
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        load_data_call_count += 1
        if load_data_call_count == 3:
            logging.info("Loaded all 3 datasets")
        return df
    except Exception as e:
        logging.error(f"An error occurred while loading data: {e}")
        return None

def export_data(df, output_path, variable_name):
    """
    Export the Spark DataFrame to a CSV file using Spark and Pandas

    :param df: The Spark DataFrame to export
    :type df: pyspark.sql.DataFrame
    :param output_path: The directory path where the CSV file should be saved
    :type output_path: str
    :param variable_name: A string used to determine the output file name
    :type variable_name: str
    :return - or None
    :rtype - or None
    :raises Exception: if an error occurs while exporting the Dataframe to CSV file
    """
    global export_data_call_count

    try:
        if 'output1' in variable_name:
            output_file_name = "it_data.csv"
        elif 'output2' in variable_name:
            output_file_name = "marketing_address_info.csv"
        elif 'output3' in variable_name:
            output_file_name = "department_breakdown.csv"
        else:
            output_file_name = "default_output.csv"

        output_dir_name = os.path.splitext(output_file_name)[0] # return a tuple with 2 elements, and get the first one, ex. it_data
        output_dir_path = os.path.join(output_path, output_dir_name) # create the path of the new directory, ex. data/output_data/it_data

        if not os.path.exists(output_dir_path): # create the directory if it doesn't exist
            os.makedirs(output_dir_path) # ex. it_data directory is created for data/output_data/it_data

        df.write.csv(output_dir_path, header=True, mode='overwrite') # write the DataFrame to CSV inside the created directory, this generates part files

        df_list = [] # create an empty list to store Pandas DataFrames read from part files

        for filename in os.listdir(output_dir_path):
            if filename.startswith("part") and filename.endswith('.csv'):
                file_path = os.path.join(output_dir_path, filename) # we add the found filename to our current path, ex. data/output_data/it_data/part0.csv
                df_list.append(pd.read_csv(file_path)) # we read the csv file using the file path (ex.data/output_data/it_data/part0.csv) into a Pandas dataframe and add it to the list
                os.remove(file_path) # we remove the part file(s) (and have them as Pandas dataframes in a list)

        combined_df = pd.concat(df_list, ignore_index=True) # we put the Pandas dataframes together, ignoring the individual index
        combined_df.to_csv(os.path.join(output_dir_path, output_file_name), index=False)  # we create the path for the final CSV file => we save the combined DataFrame without the index as the final CSV file

        all_files = os.listdir(output_dir_path)
        files_to_remove = [f for f in all_files if f.endswith('.crc') or f == '_SUCCESS']
        for file in files_to_remove:
            file_path = os.path.join(output_dir_path, file)  # we get the file to delete
            os.remove(file_path) # we remove the file

        export_data_call_count += 1
        if export_data_call_count == 3:
            logging.info("Exported all 3 datasets")

    except Exception as e:
        logging.error(f"An error occurred while exporting data: {e}")
