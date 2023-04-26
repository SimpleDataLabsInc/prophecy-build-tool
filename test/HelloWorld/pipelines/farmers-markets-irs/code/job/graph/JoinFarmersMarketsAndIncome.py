from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def JoinFarmersMarketsAndIncome(spark: SparkSession, farmers_markets: DataFrame, tax_data: DataFrame, ) -> DataFrame:
    return farmers_markets\
        .alias("farmers_markets")\
        .join(tax_data.alias("tax_data"), (col("farmers_markets.zip") == col("tax_data.zipcode")), "right_outer")
