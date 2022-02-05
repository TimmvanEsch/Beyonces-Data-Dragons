from functools import reduce
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def add_values(
        dataframe: DataFrame,
        columns_to_add: List[str] = ["value1", "value2"],
        added_values_column_name: str = "added_values"):
    columns_to_add_expression = reduce(lambda x, y: col(x) + col(y), columns_to_add)
    return dataframe.withColumn(added_values_column_name, columns_to_add_expression)


def values_condition(
        dataframe: DataFrame,
        column_name: str,
        resulting_column_name: str,
        equal_to: int,
        should_be: str,
        otherwise: str):
    condition = (resulting_column_name, when(col(column_name) == equal_to, lit(should_be)).otherwise(lit(otherwise)))
    return dataframe.withColumn(condition[0], condition[1])


def add_country_code(
        dataframe: DataFrame,
        country_code_column_name: str = "country_code",
        country_code: str = "PT"):
    return dataframe.withColumn(country_code_column_name, lit(country_code))
