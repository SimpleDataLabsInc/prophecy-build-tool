from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from raw_bronze.config.ConfigStore import *
from raw_bronze.udfs.UDFs import *

def bronze_customers(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`scottdemo`.`bronze_customers`")
