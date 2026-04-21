from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_customers_orders.config.ConfigStore import *
from silver_customers_orders.udfs.UDFs import *

def CustomerZipCodes(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (rand_zip_index(col("customer_id")) == col("in1.row_number")), "inner")\
        .select(col("in0.customer_id").alias("customer_id"), col("in0.first_name").alias("first_name"), col("in0.last_name").alias("last_name"), col("in0.phone").alias("phone"), col("in0.email").alias("email"), col("in1.zipcode").alias("zipcode"), col("in0.account_open_date").alias("account_open_date"), col("in0.account_flags").alias("account_flags"))
