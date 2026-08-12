from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_zip_codes.config.ConfigStore import *
from silver_zip_codes.udfs.UDFs import *

def SumIncomeBracketsByZip(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("zipcode"))

    return df1.agg(
        sum(expr("if((income_bracket = 6), num_returns, 0)")).alias("high_income_returns"), 
        sum(expr("if((income_bracket < 6), num_returns, 0)")).alias("low_income_returns"), 
        sum(col("num_returns")).alias("all_returns"), 
        first(col("state")).alias("state")
    )
