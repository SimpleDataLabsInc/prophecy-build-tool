from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_sales.config.ConfigStore import *
from gold_sales.udfs.UDFs import *

def Cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        datediff(col("account_open_date"), col("order_date")).alias("account_age_at_order_date"), 
        col("order_id"), 
        col("customer_id"), 
        col("amount"), 
        col("order_date"), 
        col("zipcode"), 
        col("account_flags")
    )
