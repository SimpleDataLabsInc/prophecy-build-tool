from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_top_customers.config.ConfigStore import *
from gold_top_customers.udfs.UDFs import *

def gold_top50_customers_by_spend(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`scottdemo`.`gold_top50_customers_by_spend`")
