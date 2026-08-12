from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from gold_top_customers.config.ConfigStore import *
from gold_top_customers.udfs.UDFs import *

def Top50(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.limit(50)
