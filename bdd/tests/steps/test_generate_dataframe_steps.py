import os
import unittest

import pytest
from pyspark import Row
from pyspark.sql import SparkSession
from pytest_bdd import scenario, given, when, then, parsers

from bdd.generate_dataframe import add_country_code, add_values, count_total_clients, values_condition

dir_path = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.master("local").appName("TestGenerateDataFrame").getOrCreate()


@scenario("../features/totalclients.feature", "Count total number of clients")
def test_count_total_number_of_clients():
    pass


@pytest.fixture
@given("client_details.csv")
def source_data():
    return spark.read.option("header", True).option("inferSchema", True).csv(
        dir_path + "/../fixtures/client_details.csv"
    )

@pytest.fixture
@when("the source DataFrame is passed to the count total clients process")
def add_country_code_result(source_data):
    return count_total_clients(dataframe=source_data)


@then("the DataFrame should have a count of the total number of clients")
def dataframe_has_column_with_pt_country_code(add_country_code_result):
    add_country_code_result.show()

if __name__ == '__main__':
    unittest.main()
