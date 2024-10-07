from pyspark.sql import functions as f
from src.data_cleaning import clean_data
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data(df1, df2, df3):
    """
    Perform transformations on the DataFrames

    :param df1: the first DataFrame
    :type df1: pyspark.sql.DataFrame
    :param df2: the second DataFrame
    :type df2: pyspark.sql.DataFrame
    :param df3: the third DataFrame
    :type df3: pyspark.sql.DataFrame
    :return: the transformed DataFrames after specific operations
    :rtype pyspark.sql.DataFrame or None
    :raises Exception: if an error occurs during the transformation of the data

    """

    try:

        df1 = clean_data(df1)
        df2 = clean_data(df2)
        df3 = clean_data(df3)

        if df1 is None or df2 is None or df3 is None:
            return None

        joined_df_task1 = df1.join(df2, on="id", how="inner")
        filtered_df = joined_df_task1.filter(joined_df_task1.area == 'IT')
        sorted_df = filtered_df.sort(filtered_df.sales_amount.desc())
        df_task1 = sorted_df.limit(100)
        logging.info("Handled task1 successfully")

        joined_df_task2 = joined_df_task1.filter(joined_df_task1.area == 'Marketing')
        joined_df_task2 = joined_df_task2.withColumn(
            'zip_code',
            f.regexp_extract('address', r'(\d{4} [A-Z]{2})', 0)  # 0 => full match (2 groups combined)
        )
        df_task2 = joined_df_task2.select("id", "address", "zip_code")
        logging.info("Handled task2 successfully")

        joined_df_task3 = joined_df_task1.join(df3, joined_df_task1["id"] == df3["caller_id"], how="inner")
        df_task3 = (
            joined_df_task3
            .groupBy("area")
            .agg(
                f.sum("sales_amount").alias("total_sales"),
                f.sum("calls_successful").alias("total_successful"),
                f.sum("calls_made").alias("total_calls")
            )
        )
        df_task3 = df_task3.withColumn(
            "success_rate",
            (f.col("total_successful") / f.col("total_calls")) * 100
        )
        df_task3 = df_task3.withColumn(
            "total_sales",
            f.format_string("%,.0f", f.col("total_sales"))
        )
        df_task3 = df_task3.withColumn(
            "success_rate",
            f.format_string("%.2f%%", f.col("success_rate"))
        )
        df_task3 = df_task3.select("area", "total_sales", "success_rate")
        logging.info("Handled task3 successfully")

        logging.info("Data transformation process completed successfully")

        return df_task1, df_task2, df_task3

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return None
