from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def SortBiggestOrders(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("amount").desc())
