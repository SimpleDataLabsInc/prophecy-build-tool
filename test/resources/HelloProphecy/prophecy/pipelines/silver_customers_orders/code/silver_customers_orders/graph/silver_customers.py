from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_customers_orders.config.ConfigStore import *
from silver_customers_orders.udfs.UDFs import *

def silver_customers(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`scottdemo`.`silver_customers`")
