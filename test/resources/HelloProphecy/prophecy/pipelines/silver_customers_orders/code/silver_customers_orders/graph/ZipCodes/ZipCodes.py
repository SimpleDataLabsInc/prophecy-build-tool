from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from silver_customers_orders.udfs.UDFs import *
from . import *
from .config import *

def ZipCodes(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(subgraph_config)
    df_silver_irs_zipcode = silver_irs_zipcode(spark)
    df_UniqueZipCodes = UniqueZipCodes(spark, df_silver_irs_zipcode)
    subgraph_config.update(Config)

    return df_UniqueZipCodes
