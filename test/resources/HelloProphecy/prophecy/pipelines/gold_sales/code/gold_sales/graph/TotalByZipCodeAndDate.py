from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_sales.config.ConfigStore import *
from gold_sales.udfs.UDFs import *

def TotalByZipCodeAndDate(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("zipcode"), col("order_date"))

    return df1.agg(
        sum(col("amount")).alias("total_amount"), 
        avg(col("amount")).alias("avg_amount"), 
        count(col("order_id")).alias("order_count")
    )
