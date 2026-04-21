from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)

def registerUDFs(spark: SparkSession):
    spark.udf.register("rand_zip_index", rand_zip_index)
    

    try:
        from prophecy.utils import ScalaUtil
        ScalaUtil.initializeUDFs(spark)
    except :
        pass

@udf(returnType = IntegerType())
def rand_zip_index(value: int):
    from random import Random
    r = Random(value)

    return r.randrange(0, 27689)
