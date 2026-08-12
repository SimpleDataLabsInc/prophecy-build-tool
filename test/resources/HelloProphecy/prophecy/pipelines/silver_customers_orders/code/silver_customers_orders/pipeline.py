from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from silver_customers_orders.config.ConfigStore import *
from silver_customers_orders.udfs.UDFs import *
from prophecy.utils import *
from silver_customers_orders.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_orders = bronze_orders(spark)
    silver_orders(spark, df_bronze_orders)
    df_ZipCodes = ZipCodes(spark, Config.ZipCodes)
    df_bronze_customers = bronze_customers(spark)
    df_CustomerZipCodes = CustomerZipCodes(spark, df_bronze_customers, df_ZipCodes)
    silver_customers(spark, df_CustomerZipCodes)
    df_ByCustomerId = ByCustomerId(spark, df_bronze_orders, df_CustomerZipCodes)
    silver_order_customer_details(spark, df_ByCustomerId)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/silver_customers_orders")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/silver_customers_orders", config = Config)(pipeline)

if __name__ == "__main__":
    main()
