from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CalcHasFm(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("zip"), 
        col("num_farmers_markets"), 
        col("high_income_percent"), 
        col("zipcode"), 
        col("high_income_returns"), 
        col("low_income_returns"), 
        col("all_returns"), 
        col("is_high_income"), 
        (coalesce(col("num_farmers_markets"), lit(0)) > lit(0)).alias("has_fm")
    )
