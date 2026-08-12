from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from silver_customers_orders.udfs.UDFs import *

def UniqueZipCodes(spark: SparkSession, in0: DataFrame) -> (DataFrame):

    try:
        registerUDFs(spark)
    except NameError:
        print("registerUDFs not working")

    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "SELECT \n    row_number() OVER (PARTITION BY 1 ORDER BY zipcode ASC) AS row_number, \n    zipcode \nFROM (\n    SELECT distinct zipcode from in0 order by zipcode asc\n)"
    )

    return df1
