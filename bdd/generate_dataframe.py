from functools import reduce
from typing import List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, countDistinct

def count_total_clients(
    dataframe: DataFrame) -> int:    
    return dataframe.countDistinct()

def filter_dragons(
    dataframe: DataFrame,
    column: str, 
    value: str) -> DataFrame:
    return dataframe.filter(col(column)==value)