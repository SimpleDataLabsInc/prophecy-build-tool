from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"), 
        ceil(col("amounts")).alias("amount"), 
        col("customer_id")
    )
