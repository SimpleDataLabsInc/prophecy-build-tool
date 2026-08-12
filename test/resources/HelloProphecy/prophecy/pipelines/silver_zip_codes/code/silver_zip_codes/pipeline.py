from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from silver_zip_codes.config.ConfigStore import *
from silver_zip_codes.udfs.UDFs import *
from prophecy.utils import *
from silver_zip_codes.graph import *

def pipeline(spark: SparkSession) -> None:
    df_bronze_irs_zipcode = bronze_irs_zipcode(spark)
    df_IgnoreBadZip = IgnoreBadZip(spark, df_bronze_irs_zipcode)
    df_CastDataTypes = CastDataTypes(spark, df_IgnoreBadZip)
    df_SumIncomeBracketsByZip = SumIncomeBracketsByZip(spark, df_CastDataTypes)
    df_CalcIsHighIncome = CalcIsHighIncome(spark, df_SumIncomeBracketsByZip)
    silver_irs_zipcode(spark, df_CalcIsHighIncome)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/silver_zip_codes")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/silver_zip_codes", config = Config)(pipeline)

if __name__ == "__main__":
    main()
