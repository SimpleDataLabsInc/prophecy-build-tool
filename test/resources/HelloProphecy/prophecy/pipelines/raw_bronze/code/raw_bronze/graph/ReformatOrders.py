from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from raw_bronze.config.ConfigStore import *
from raw_bronze.udfs.UDFs import *

def ReformatOrders(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("order_id").cast(IntegerType()).alias("order_id"), 
        col("customer_id").cast(IntegerType()).alias("customer_id"), 
        col("order_status"), 
        col("order_category"), 
        col("order_date").cast(DateType()).alias("order_date"), 
        col("amount").cast(FloatType()).alias("amount")
    )
