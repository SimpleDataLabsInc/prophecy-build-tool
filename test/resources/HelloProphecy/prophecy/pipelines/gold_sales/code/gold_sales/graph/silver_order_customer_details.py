from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_sales.config.ConfigStore import *
from gold_sales.udfs.UDFs import *

def silver_order_customer_details(spark: SparkSession) -> DataFrame:
    return spark.read.table("`scottdemo`.`silver_order_customer_details`")
