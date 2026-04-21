from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from silver_zip_codes.config.ConfigStore import *
from silver_zip_codes.udfs.UDFs import *

def bronze_irs_zipcode(spark: SparkSession) -> DataFrame:
    return spark.read.table("`scottdemo`.`bronze_irs_zipcode`")
