package io.prophecy.pipelines.omega

import io.prophecy.libs._
import io.prophecy.pipelines.omega.config.Context
import io.prophecy.pipelines.omega.config._
import io.prophecy.pipelines.omega.config.ConfigStore.interimOutput
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

  def graph(context: Context): Unit = {
    val df_newDS = newDS(context).interim(
      "graph",
      "E60y4MxLxitgsYEFz6q6n$$X5Sc1S3C3sjCTgUt2LxTz",
      "oB3ESarrWC2R4XkkOu6YW$$mUOObyoPe-An02XjyVn8d"
    )
    val df_Reformat_1 = Reformat_1(context, df_newDS).interim(
      "graph",
      "-mRkKTbhqlY6211fgL7X2$$SkXWqXzv87H2eCng09_iQ",
      "5fmItaDQn5_tpsCqEhR-d$$pWWj0MDPkd8m9HgQKrBFD"
    )
    val df_Reformat_2 = Reformat_2(context, df_Reformat_1).interim(
      "graph",
      "hTWI8fxiThKrOtUJ6rpbq$$QsX3BQLyA1LmO_KX-dMPy",
      "2EWCc5PylWkx8rV_ghqda$$tA7aBTxn9bM2Pb8dNacWi"
    )
    df_Reformat_2.cache().count()
    df_Reformat_2.unpersist()
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
    MetricsCollector.initializeMetrics(spark)
    implicit val interimOutputConsole: InterimOutput = InterimOutputHive2("")
    spark.conf.set("prophecy.collect.basic.stats",          "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules",
                   "org.apache.spark.sql.catalyst.optimizer.ColumnPruning"
    )
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Omega")
    try MetricsCollector.start(spark,                "pipelines/Omega", context.config)
    catch {
      case _: Throwable =>
        MetricsCollector.start(spark, "pipelines/Omega")
    }
    graph(context)
    MetricsCollector.end(spark)
  }

}
