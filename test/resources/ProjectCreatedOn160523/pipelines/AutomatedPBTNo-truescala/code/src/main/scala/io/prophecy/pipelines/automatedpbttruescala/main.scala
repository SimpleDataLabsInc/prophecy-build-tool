package io.prophecy.pipelines.automatedpbttruescala

import io.prophecy.libs._
import io.prophecy.pipelines.automatedpbttruescala.config.Context
import io.prophecy.pipelines.automatedpbttruescala.config._
import io.prophecy.pipelines.automatedpbttruescala.udfs.UDFs._
import io.prophecy.pipelines.automatedpbttruescala.udfs._
import io.prophecy.pipelines.automatedpbttruescala.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_s3_source_dataset = s3_source_dataset(context)
    val df_Reformat_1        = Reformat_1(context, df_s3_source_dataset)
    val df_Script_1          = Script_1(context,   df_Reformat_1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
      .newSession()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/AutomatedPBTNo-truescala"
    )
    registerUDFs(spark)
    MetricsCollector.start(spark, "pipelines/AutomatedPBTNo-truescala")
    apply(context)
    MetricsCollector.end(spark)
  }

}
