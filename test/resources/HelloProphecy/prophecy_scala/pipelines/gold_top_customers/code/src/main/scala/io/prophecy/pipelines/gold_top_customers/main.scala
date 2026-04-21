package io.prophecy.pipelines.gold_top_customers

import io.prophecy.libs._
import io.prophecy.pipelines.gold_top_customers.config._
import io.prophecy.pipelines.gold_top_customers.udfs.UDFs._
import io.prophecy.pipelines.gold_top_customers.udfs.PipelineInitCode._
import io.prophecy.pipelines.gold_top_customers.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_gold_total_sales_by_customer = gold_total_sales_by_customer(context)
    val df_OrderByTotal                 = OrderByTotal(context, df_gold_total_sales_by_customer)
    val df_Top50                        = Top50(context,        df_OrderByTotal)
    gold_top50_customers_by_spend(context, df_Top50)
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
      .set("prophecy.metadata.pipeline.uri", "pipelines/gold_top_customers")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/gold_top_customers") {
      apply(context)
    }
  }

}
