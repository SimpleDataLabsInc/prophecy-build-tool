from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Customer_Orders(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "dev":
        return spark.read\
            .schema(
              StructType([
                StructField("customer_id", IntegerType(), True), StructField("orders", LongType(), False), StructField("amounts", DoubleType(), True), StructField("account_length_days", IntegerType(), True)
            ])
            )\
            .option("header", True)\
            .option("sep", ",")\
            .option("ignoreLeadingWhiteSpace", True)\
            .option("ignoreTrailingWhiteSpace", True)\
            .csv("dbfs:/Prophecy/ashish@prophecy.io/CustomersOrders.csv")
    else:
        raise Exception("No valid dataset present to read fabric")
