from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from silver_customers_orders.udfs.UDFs import *

def silver_irs_zipcode(spark: SparkSession) -> DataFrame:
    return spark.read.table("`scottdemo`.`silver_irs_zipcode`")
