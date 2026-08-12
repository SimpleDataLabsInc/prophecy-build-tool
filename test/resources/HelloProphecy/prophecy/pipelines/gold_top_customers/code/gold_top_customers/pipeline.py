from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from gold_top_customers.config.ConfigStore import *
from gold_top_customers.udfs.UDFs import *
from prophecy.utils import *
from gold_top_customers.graph import *

def pipeline(spark: SparkSession) -> None:
    df_gold_total_sales_by_customer = gold_total_sales_by_customer(spark)
    df_OrderByTotalSpend = OrderByTotalSpend(spark, df_gold_total_sales_by_customer)
    df_Top50 = Top50(spark, df_OrderByTotalSpend)
    gold_top50_customers_by_spend(spark, df_Top50)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/gold_top_customers")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/gold_top_customers", config = Config)(pipeline)

if __name__ == "__main__":
    main()
