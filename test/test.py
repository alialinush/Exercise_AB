import unittest
from src.utils import load_data, export_data
from src.data_transformation import transform_data
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

class UnitTest(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("unit_tests") \
            .getOrCreate()

        self.schema_one = StructType([
            StructField("id", IntegerType(), True),
            StructField("area", StringType(), True),
            StructField("calls_made", IntegerType(), True),
            StructField("calls_successful", IntegerType(), True)
        ])

        self.schema_two = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("sales_amount", DoubleType(), True)
        ])

        self.schema_three = StructType([
            StructField("id", IntegerType(), True),
            StructField("caller_id", IntegerType(), True),
            StructField("company", StringType(), True),
            StructField("recipient", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("product_sold", StringType(), True),
            StructField("quantity", IntegerType(), True)
        ])

        self.schema_task1 = StructType([
            StructField("id", IntegerType(), True),
            StructField("area", StringType(), True),
            StructField("calls_made", IntegerType(), True),
            StructField("calls_successful", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("sales_amount", DoubleType(), True)
        ])

        self.schema_task2 = StructType([
            StructField("id", IntegerType(), True),
            StructField("address", StringType(), True),
            StructField("zip_code", StringType(), True)
        ])

        self.schema_task3 = StructType([
            StructField("area", StringType(), True),
            StructField("total_sales", StringType(), False),
            StructField("success_rate", StringType(), False)
        ])

    def tearDown(self):
        self.spark.stop()

    def test_load_data_method(self):

        valid_df = load_data(self.spark, "data/input_data/dataset_one.csv")

        self.assertIsNotNone(valid_df, "The DataFrame should not be None for a valid file")
        self.assertGreater(valid_df.count(), 0, "The DataFrame should contain at least one row for a valid file")

    def test_transform_data_method(self):

        sample_data_one_not_null = [
            Row(id=1, area="IT", calls_made=30, calls_successful=5),
            Row(id=2, area="IT", calls_made=25, calls_successful=10),
            Row(id=3, area="Marketing", calls_made=20, calls_successful=3)
        ]

        df1_example_not_null = self.spark.createDataFrame(sample_data_one_not_null, self.schema_one)

        sample_data_two_not_null = [
            Row(id=1, name="Alina", address="2588 VD, Kropswolde", sales_amount=26145.23),
            Row(id=2, name="Andrei", address="Thijmenweg 38, 7801 OC, Grijpskerk", sales_amount=46145.23),
            Row(id=3, name="Dan", address="Tessapad 82, 8487 PZ, Sambeek", sales_amount=96145.23)
        ]

        df2_example_not_null = self.spark.createDataFrame(sample_data_two_not_null, self.schema_two)

        sample_data_three_not_null = [
            Row(id=1, caller_id=1, company="CompanyA", recipient="X", age=30, country="RO", product_sold="A", quantity = 2),
            Row(id=2, caller_id=2, company="CompanyB",recipient="Y", age=20, country="RO", product_sold="B", quantity = 4),
            Row(id=3, caller_id=3, company="CompanyC",recipient="Z", age=10, country="RO", product_sold="C", quantity = 5)
        ]

        df3_example_not_null = self.spark.createDataFrame(sample_data_three_not_null, self.schema_three)

        transformed_df_output1_not_null, transformed_df_output2_not_null , transformed_df_output3_not_null = transform_data(df1_example_not_null,df2_example_not_null,df3_example_not_null)

        for df in [transformed_df_output1_not_null, transformed_df_output2_not_null, transformed_df_output3_not_null]:
            self.assertIsNotNone(df, "The DataFrame should not be None for a valid transformed file")
            self.assertGreater(df.count(), 0, "The DataFrame should contain at least one row  for a valid transformed file")

        task1_expected_not_null = [
            Row(id=2, area="IT", calls_made=25, calls_successful=10,  name="Andrei", address="Thijmenweg 38, 7801 OC, Grijpskerk", sales_amount=46145.23),
            Row(id=1, area="IT", calls_made=30, calls_successful=5, name="Alina", address="2588 VD, Kropswolde", sales_amount=26145.23)
        ]

        dataframe_task1_expected_not_null = self.spark.createDataFrame(task1_expected_not_null, self.schema_task1)
        assert_df_equality(transformed_df_output1_not_null, dataframe_task1_expected_not_null)

        task2_expected_not_null = [
            Row(id=3, address="Tessapad 82, 8487 PZ, Sambeek", zip_code = "8487 PZ")
        ]

        dataframe_task2_expected_not_null = self.spark.createDataFrame(task2_expected_not_null, self.schema_task2)
        assert_df_equality(transformed_df_output2_not_null, dataframe_task2_expected_not_null)

        task3_expected_not_null = [
            Row(area="Marketing",total_sales = "96,145", success_rate = "15.00%"),
            Row(area="IT",total_sales = "72,290", success_rate = "27.27%")
        ]

        dataframe_task3_expected_not_null = self.spark.createDataFrame(task3_expected_not_null, self.schema_task3)
        assert_df_equality(transformed_df_output3_not_null, dataframe_task3_expected_not_null)

        sample_data_one_null = [
            Row(id=1, area="IT", calls_made=30, calls_successful=5),
            Row(id=2, area="IT", calls_made=None, calls_successful=10),
            Row(id=3, area="Marketing", calls_made=20, calls_successful=3)
        ]

        df1_example_null = self.spark.createDataFrame(sample_data_one_null, self.schema_one)

        sample_data_two_null = [
            Row(id=1, name="Alina", address="2588 VD, Kropswolde", sales_amount=None),
            Row(id=2, name="Andrei", address="Thijmenweg 38, 7801 OC, Grijpskerk", sales_amount=46145.23),
            Row(id=3, name="Dan", address="Tessapad 82, 8487 PZ, Sambeek", sales_amount=96145.23)
        ]

        df2_example_null = self.spark.createDataFrame(sample_data_two_null, self.schema_two)

        sample_data_three_null = [
            Row(id=1, caller_id=1, company="CompanyA", recipient="X", age=30, country="RO", product_sold="A", quantity = None),
            Row(id=2, caller_id=2, company="CompanyB",recipient="Y", age=20, country="RO", product_sold="B", quantity = 4),
            Row(id=3, caller_id=3, company="CompanyC",recipient="Z", age=10, country="RO", product_sold="C", quantity = 5)
        ]

        df3_example_null = self.spark.createDataFrame(sample_data_three_null, self.schema_three)

        transformed_df_output1_null, transformed_df_output2_null , transformed_df_output3_null = transform_data(df1_example_null,df2_example_null,df3_example_null)

        for df in [transformed_df_output1_null, transformed_df_output2_null, transformed_df_output3_null]:
            self.assertIsNotNone(df, "The DataFrame should not be None for a valid transformed file")
            self.assertGreater(df.count(), 0, "The DataFrame should contain at least one row  for a valid transformed file")

        task1_expected_null = [
            Row(id=1, area="IT", calls_made=30, calls_successful=5, name="Alina", address="2588 VD, Kropswolde", sales_amount=71145.23),
            Row(id=2, area="IT", calls_made=25, calls_successful=10,  name="Andrei", address="Thijmenweg 38, 7801 OC, Grijpskerk", sales_amount=46145.23)
        ]

        dataframe_task1_expected_null = self.spark.createDataFrame(task1_expected_null, self.schema_task1)
        assert_df_equality(transformed_df_output1_null, dataframe_task1_expected_null)

    def test_export_data_method(self):

        sample = [
            Row(id=1, area="Marketing", calls_made=43, calls_successful=2),
            Row(id=2, area="IT", calls_made=54, calls_successful=4)
        ]

        sample_example = self.spark.createDataFrame(sample, self.schema_one)
        export_data(sample_example, "data/test_data", "transformed_df_output1")

if __name__ == '__main__':
    unittest.main()
