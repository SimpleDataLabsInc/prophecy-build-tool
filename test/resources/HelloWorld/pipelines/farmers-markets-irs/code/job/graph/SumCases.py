from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SumCases(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        sum(expr("if(has_fm_high_income, 1, 0)")).alias("has_fm_high_income"), 
        sum(expr("if(has_fm_low_income, 1, 0)")).alias("has_fm_low_income"), 
        sum(expr("if(no_fm_high_income, 1, 0)")).alias("no_fm_high_income"), 
        sum(expr("if(no_fm_low_income, 1, 0)")).alias("no_fm_low_income"), 
        count(lit(1)).alias("num_zipcodes"), 
        sum(expr("if(has_fm, 1, 0)")).alias("has_fm"), 
        sum(expr("if((NOT has_fm), 1, 0)")).alias("no_fm")
    )
