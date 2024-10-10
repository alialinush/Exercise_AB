from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql import functions as f
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

data_clean_count = 0

def clean_data(df):
    """
   Perform data cleaning on a Dataframe

   :param df: the first DataFrame
   :type df: pyspark.sql.DataFrame
   :return: the cleaned DataFrame after specific operations
   :rtype pyspark.sql.DataFrame or None
   :raises Exception: if an error occurs during the cleaning of the data

   """

    global data_clean_count
    try:
        duplicate_count = df.count() - df.dropDuplicates().count()
        if duplicate_count > 0:
            df = df.dropDuplicates()  # 1. Uniqueness constraint

        total_rows = df.count()
        columns_with_nulls = [column for column in df.columns if df.filter(f.col(column).isNull()).count() > 0]  # 2. Completeness constraint

        for column in columns_with_nulls:
            null_count = df.filter(f.col(column).isNull()).count()
            null_percentage = (null_count / total_rows) * 100

            if null_percentage < 70:
                if df.schema[column].dataType == DoubleType():
                    mean_value = df.agg(f.mean(column)).collect()[0][0]
                    df = df.withColumn(column, f.when(f.col(column).isNull(), mean_value).otherwise(f.col(column)))
                elif df.schema[column].dataType == IntegerType():
                    mean_value = int(df.agg(f.mean(column)).collect()[0][0])
                    df = df.withColumn(column, f.when(f.col(column).isNull(), mean_value).otherwise(f.col(column)))
                elif df.schema[column].dataType == StringType():
                    mode_value = df.groupBy(column).count().orderBy(f.desc("count")).first()[0]
                    df = df.withColumn(column, f.when(f.col(column).isNull(), mode_value).otherwise(f.col(column)))
            else:
                df = df.drop(column)

        categorical_cols = [column for column in df.columns if df.schema[column].dataType == StringType()]
        for col_name in categorical_cols:
            df = df.withColumn(col_name, f.trim(f.col(col_name))) # Membership constraint - categorical data type

        data_clean_count += 1
        if data_clean_count == 3:
            logging.info("Data cleaning process completed successfully for all 3 datasets")

        return df

    except Exception as e:
        logging.error(f"Error during data cleaning: {e}")
        return None
