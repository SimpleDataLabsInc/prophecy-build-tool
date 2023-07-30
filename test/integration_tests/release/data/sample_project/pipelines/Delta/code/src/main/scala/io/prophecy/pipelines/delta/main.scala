package io.prophecy.pipelines.delta

import io.prophecy.libs._
import io.prophecy.pipelines.delta.config.Context
import io.prophecy.pipelines.delta.config._
import io.prophecy.pipelines.delta.udfs.UDFs._
import io.prophecy.pipelines.delta.udfs._
import io.prophecy.pipelines.delta.udfs.PipelineInitCode._
import io.prophecy.pipelines.delta.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_newDS    = newDS(context)
    val df_Filter_1 = Filter_1(context, df_newDS)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Delta")
    registerUDFs(spark)
    try MetricsCollector.start(spark, "pipelines/Delta", context.config)
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/Delta")
    }
    apply(context)
    MetricsCollector.end(spark)
  }

}
