from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_zip_codes.config.ConfigStore import *
from silver_zip_codes.udfs.UDFs import *

def CastDataTypes(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("zipcode"), 
        col("agi_stub").cast(IntegerType()).alias("income_bracket"), 
        col("N1").cast(IntegerType()).alias("num_returns"), 
        col("STATE").alias("state")
    )
