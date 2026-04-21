package io.prophecy.pipelines.gold_sales

import io.prophecy.libs._
import io.prophecy.pipelines.gold_sales.config._
import io.prophecy.pipelines.gold_sales.udfs.UDFs._
import io.prophecy.pipelines.gold_sales.udfs.PipelineInitCode._
import io.prophecy.pipelines.gold_sales.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_silver_orders_customer_details = silver_orders_customer_details(
      context
    )
    val df_Cleanup    = Cleanup(context,    df_silver_orders_customer_details)
    val df_SumAmounts = SumAmounts(context, df_Cleanup)
    gold_total_sales_by_customer(context, df_SumAmounts)
    val df_TotalByZipCode   = TotalByZipCode(context,   df_Cleanup)
    val df_SortByDateAndZip = SortByDateAndZip(context, df_TotalByZipCode)
    gold_sales_by_zip_date(context, df_SortByDateAndZip)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/gold_sales")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/gold_sales") {
      apply(context)
    }
  }

}
