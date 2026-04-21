from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from raw_bronze.config.ConfigStore import *
from raw_bronze.udfs.UDFs import *
from prophecy.utils import *
from raw_bronze.graph import *

def pipeline(spark: SparkSession) -> None:
    df_raw_orders = raw_orders(spark)
    df_raw_irs_zipcode = raw_irs_zipcode(spark)
    df_ReformatIRS = ReformatIRS(spark, df_raw_irs_zipcode)
    df_ReformatOrders = ReformatOrders(spark, df_raw_orders)
    df_raw_customers = raw_customers(spark)
    df_ReformatCustomers = ReformatCustomers(spark, df_raw_customers)
    bronze_orders(spark, df_ReformatOrders)
    bronze_customers(spark, df_ReformatCustomers)
    bronze_irs_zipcode(spark, df_ReformatIRS)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/raw_bronze")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/raw_bronze", config = Config)(pipeline)

if __name__ == "__main__":
    main()
