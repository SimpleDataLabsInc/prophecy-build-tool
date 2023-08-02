package io.prophecy.pipelines.onlyone

import io.prophecy.libs._
import io.prophecy.pipelines.onlyone.config.Context
import io.prophecy.pipelines.onlyone.config._
import io.prophecy.pipelines.onlyone.udfs.UDFs._
import io.prophecy.pipelines.onlyone.udfs._
import io.prophecy.pipelines.onlyone.udfs.PipelineInitCode._
import io.prophecy.pipelines.onlyone.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_newDS      = newDS(context)
    val df_Reformat_1 = Reformat_1(context, df_newDS)
    val df_Reformat_2 = Reformat_2(context, df_Reformat_1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/OnlyOne")
    registerUDFs(spark)
    try MetricsCollector.start(spark, "pipelines/OnlyOne", context.config)
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/OnlyOne")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
