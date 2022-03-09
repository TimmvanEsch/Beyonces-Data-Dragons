import os
import unittest

import pytest
from pyspark import Row
from pyspark.sql import SparkSession
from pytest_bdd import scenario, given, when, then, parsers

from bdd.generate_dataframe import count_total_clients, filter_dragons

dir_path = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.master("local").appName("TestGenerateDataFrame").getOrCreate()



@pytest.fixture
@given("client_details.csv")
def source_data():
    return spark.read.option("header", True).option("inferSchema", True).csv(
        dir_path + "/../fixtures/client_details.csv"
    )

@pytest.fixture
@when("the source DataFrame is passed to the filter clients in the healthcare sector")
def filter_clients_sector(source_data):
    filtered_df =  filter_dragons(dataframe=source_data,
                          column="sector",
                          value="Health Care")
    return filtered_df


@then("the DataFrame should contain the clients in the healthcare sector")
def filtered_clients_sector_results():
    filter_clients_sector.show()

@then("the DataFrame hould contain the clients in the healthcare sector that use JCB creditcards")
def filtered_clients_sector_results(filter_clients_sector):
    filtered_df =  filter_dragons(dataframe=filter_clients_sector,
                          column="credit_card",
                          value="jcb")
    return count_total_clients(filtered_df)


if __name__ == '__main__':
    unittest.main()
