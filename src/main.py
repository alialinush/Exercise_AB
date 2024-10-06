from pyspark.sql import SparkSession
from src.data_transformation import transform_data
from src.utils import load_data, export_data
import argparse

def main(input1, input2, input3):

    spark = SparkSession.builder \
        .appName("multi-csv-spark") \
        .getOrCreate()

    try:
        df1 = load_data(spark, input1)
        df2 = load_data(spark, input2)
        df3 = load_data(spark, input3)

        if df1 is None or df2 is None or df3 is None:
            return

        transformed_df_output1, transformed_df_output2, transformed_df_output3 = transform_data(df1, df2, df3)

        if transformed_df_output1 is None or transformed_df_output2 is None or transformed_df_output3 is None:
            return

        export_data(transformed_df_output1, "data/output_data", "transformed_df_output1")
        export_data(transformed_df_output2, "data/output_data", "transformed_df_output2")
        export_data(transformed_df_output3, "data/output_data", "transformed_df_output3")

    finally:
        spark.stop()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("input1")
    parser.add_argument("input2")
    parser.add_argument("input3")
    args = parser.parse_args()

    main(args.input1, args.input2, args.input3)

