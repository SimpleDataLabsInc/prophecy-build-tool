from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SumIncomeBracketsByZip(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("zipcode"))

    return df1.agg(
        sum(expr("if((income_bracket = 6), num_returns, 0)")).alias("high_income_returns"), 
        sum(expr("if((income_bracket < 6), num_returns, 0)")).alias("low_income_returns"), 
        sum(col("num_returns")).alias("all_returns")
    )
