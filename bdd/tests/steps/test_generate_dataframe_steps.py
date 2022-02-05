import os
import unittest

import pytest
from pyspark import Row
from pyspark.sql import SparkSession
from pytest_bdd import scenario, given, when, then, parsers

from bdd.generate_dataframe import add_country_code, add_values, values_condition

dir_path = os.path.dirname(os.path.abspath(__file__))

spark = SparkSession.builder.master("local").appName("TestGenerateDataFrame").getOrCreate()


@scenario("../features/bdd.feature", "Add country code")
def test_add_country_code():
    pass


@scenario("../features/bdd.feature", "Add dynamic country code")
def test_add_nl_country_code():
    pass


@pytest.fixture
@given("the source data")
def source_data():
    return spark.read.option("header", True).option("inferSchema", True).csv(
        dir_path + "/../fixtures/data.csv"
    )


@pytest.fixture
@when("the source DataFrame is passed to the add country code process")
def add_country_code_result(source_data):
    return add_country_code(dataframe=source_data)


@pytest.fixture
@when(
    parsers.parse(
        'the source DataFrame is passed to the add country code process with "{country_column_name}" as the column name and "{country}" as the country'
    )
)
def add_nl_country_code_result(source_data, country_column_name, country):
    return add_country_code(dataframe=source_data, country_code_column_name=country_column_name, country_code=country)


@then("the DataFrame should have an extra column with the country code")
def dataframe_has_column_with_pt_country_code(add_country_code_result):
    add_country_code_result.show()
    actual = sorted(add_country_code_result.select("country_code").collect())
    expected = sorted([Row(country_code='PT'), Row(country_code='PT')])
    assert expected == actual


@then("the DataFrame should have an extra column with the correct name and country code")
def dataframe_has_column_with_nl_country_code(add_nl_country_code_result):
    add_nl_country_code_result.show()
    actual = sorted(add_nl_country_code_result.select("country").collect())
    expected = sorted([Row(country_code='NL'), Row(country_code='NL')])
    assert expected == actual


@scenario("../features/bdd.feature", "Add values")
def test_add_values():
    pass


@pytest.fixture
@when(parsers.parse(
    'the source DataFrame is passed to the add values process with value columns to add "{columns_to_add}" and the new column named "{column_name}"'))
def add_values_result(source_data, columns_to_add, column_name):
    return add_values(
        dataframe=source_data,
        columns_to_add=columns_to_add.split(","),
        added_values_column_name=column_name
    )


@then("the DataFrame should have an extra column with the values added")
def dataframe_has_column_with_added_values(add_values_result):
    add_values_result.show()
    actual = sorted(add_values_result.select("values_added").collect())
    expected = sorted([Row(added_values=3), Row(added_values=6)])
    assert expected == actual


@scenario(
    "../features/bdd.feature", "Conditions",
    example_converters=dict(column_name=str, equal_to=int, should_be=str, otherwise=str, resulting_column_name=str)
)
def test_values_condition():
    pass


@pytest.fixture
@when(
    "the source DataFrame is passed to the values_condition method with a <column_name> equal to <equal_to> the result should be <should_be> otherwise it must be <otherwise> and the resulting column name is <resulting_column_name>")
def values_condition_result(source_data, column_name, equal_to, should_be, otherwise, resulting_column_name):
    return values_condition(
        dataframe=source_data,
        column_name=column_name,
        equal_to=equal_to,
        should_be=should_be,
        otherwise=otherwise,
        resulting_column_name=resulting_column_name
    )


@then("the DataFrame should have an extra column with the condition column added")
def dataframe_has_column_with_condition_column(values_condition_result):
    values_condition_result.show()


if __name__ == '__main__':
    unittest.main()
