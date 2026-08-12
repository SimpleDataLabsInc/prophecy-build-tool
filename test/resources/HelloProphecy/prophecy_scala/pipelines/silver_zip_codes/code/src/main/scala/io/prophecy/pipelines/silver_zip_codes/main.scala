package io.prophecy.pipelines.silver_zip_codes

import io.prophecy.libs._
import io.prophecy.pipelines.silver_zip_codes.config._
import io.prophecy.pipelines.silver_zip_codes.udfs.UDFs._
import io.prophecy.pipelines.silver_zip_codes.udfs.PipelineInitCode._
import io.prophecy.pipelines.silver_zip_codes.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_bronze_irs_zipcode = bronze_irs_zipcode(context)
    val df_IgnoreBadZip       = IgnoreBadZip(context,  df_bronze_irs_zipcode)
    val df_CastDataTypes      = CastDataTypes(context, df_IgnoreBadZip)
    val df_SumIncomeBracketsByZip =
      SumIncomeBracketsByZip(context, df_CastDataTypes)
    val df_CalcIsHighIncome =
      CalcIsHighIncome(context, df_SumIncomeBracketsByZip)
    silver_irs_zipcode(context, df_CalcIsHighIncome)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf
      .set("prophecy.metadata.pipeline.uri", "pipelines/silver_zip_codes")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/silver_zip_codes") {
      apply(context)
    }
  }

}
