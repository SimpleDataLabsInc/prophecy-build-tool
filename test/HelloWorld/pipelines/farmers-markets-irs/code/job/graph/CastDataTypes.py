from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CastDataTypes(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("zipcode"), 
        col("agi_stub").cast(IntegerType()).alias("income_bracket"), 
        col("N1").cast(IntegerType()).alias("num_returns")
    )
