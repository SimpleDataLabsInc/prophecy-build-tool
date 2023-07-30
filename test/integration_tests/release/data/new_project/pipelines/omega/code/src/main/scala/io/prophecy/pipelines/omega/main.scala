package io.prophecy.pipelines.omega

import io.prophecy.libs._
import io.prophecy.pipelines.omega.config.Context
import io.prophecy.pipelines.omega.config._
import io.prophecy.pipelines.omega.udfs.UDFs._
import io.prophecy.pipelines.omega.udfs._
import io.prophecy.pipelines.omega.udfs.PipelineInitCode._
import io.prophecy.pipelines.omega.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {
  def apply(context: Context): Unit = {}

  def main(args:     Array[String]): Unit = {
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/omega")
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/omega")
    apply(context)
    MetricsCollector.end(spark, context.config)
  }

}
