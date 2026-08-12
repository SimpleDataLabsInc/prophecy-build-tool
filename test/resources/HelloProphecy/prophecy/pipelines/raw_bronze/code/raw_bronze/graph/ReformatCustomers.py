from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from raw_bronze.config.ConfigStore import *
from raw_bronze.udfs.UDFs import *

def ReformatCustomers(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id").cast(IntegerType()).alias("customer_id"), 
        col("first_name"), 
        col("last_name"), 
        col("phone"), 
        col("email"), 
        col("country_code"), 
        col("account_open_date").cast(DateType()).alias("account_open_date"), 
        col("account_flags"), 
        concat(col("first_name"), col("last_name")).alias("full_name")
    )
