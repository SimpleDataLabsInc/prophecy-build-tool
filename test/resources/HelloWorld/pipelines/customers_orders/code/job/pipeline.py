from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Orders = Orders(spark)
    df_Customers = Customers(spark)
    df_By_CustomerId = By_CustomerId(spark, df_Orders, df_Customers)
    df_Cleanup = Cleanup(spark, df_By_CustomerId)
    df_Sum_Amounts = Sum_Amounts(spark, df_Cleanup)
    Customer_Orders(spark, df_Sum_Amounts)

def main():
    spark = SparkSession.builder \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.legacy.allowUntypedScalaUDF", "true") \
        .enableHiveSupport() \
        .appName("Prophecy Pipeline") \
        .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    pipeline(spark)

if __name__ == "__main__":
    main()
