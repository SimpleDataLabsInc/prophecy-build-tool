from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from prophecy.utils import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_FarmersMarketsSource = FarmersMarketsSource(spark)
    df_FilterOutNullZips = FilterOutNullZips(spark, df_FarmersMarketsSource)
    df_CountFarmersMarketsByZip = CountFarmersMarketsByZip(spark, df_FilterOutNullZips)
    df_IrsZipcodesSource = IrsZipcodesSource(spark)
    df_FilterOutBadZips = FilterOutBadZips(spark, df_IrsZipcodesSource)
    df_CastDataTypes = CastDataTypes(spark, df_FilterOutBadZips)
    df_SumIncomeBracketsByZip = SumIncomeBracketsByZip(spark, df_CastDataTypes)
    df_CalcIsHighIncome = CalcIsHighIncome(spark, df_SumIncomeBracketsByZip)
    df_JoinFarmersMarketsAndIncome = JoinFarmersMarketsAndIncome(
        spark, 
        df_CountFarmersMarketsByZip, 
        df_CalcIsHighIncome
    )
    df_CalcHasFm = CalcHasFm(spark, df_JoinFarmersMarketsAndIncome)
    df_CalcCases = CalcCases(spark, df_CalcHasFm)
    df_SumCases = SumCases(spark, df_CalcCases)
    df_CalcPercents = CalcPercents(spark, df_SumCases)
    farmers_market_tax_report(spark, df_CalcPercents)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.start(spark)
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
