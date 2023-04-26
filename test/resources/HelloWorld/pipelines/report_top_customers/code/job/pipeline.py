from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Customer_Orders = Customer_Orders(spark)
    df_By_Total_Amount = By_Total_Amount(spark, df_Customer_Orders)
    df_Top_10 = Top_10(spark, df_By_Total_Amount)
    Report(spark, df_Top_10)

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
