package io.prophecy.pipelines.silver_customers_orders

import io.prophecy.libs._
import io.prophecy.pipelines.silver_customers_orders.config._
import io.prophecy.pipelines.silver_customers_orders.udfs.UDFs._
import io.prophecy.pipelines.silver_customers_orders.udfs.PipelineInitCode._
import io.prophecy.pipelines.silver_customers_orders.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_bronze_orders      = bronze_orders(context)
    val df_silver_irs_zipcode = silver_irs_zipcode(context)
    val df_UniqueZipCodes     = UniqueZipCodes(context, df_silver_irs_zipcode)
    val df_bronze_customers   = bronze_customers(context)
    val df_CustomerZipCodes =
      CustomerZipCodes(context, df_bronze_customers, df_UniqueZipCodes)
    val df_ByCustomerId =
      ByCustomerId(context,                 df_bronze_orders, df_CustomerZipCodes)
    silver_orders_customer_details(context, df_ByCustomerId)
    silver_orders(context,                  df_bronze_orders)
    silver_customers(context,               df_CustomerZipCodes)
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
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/silver_customers_orders"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/silver_customers_orders") {
      apply(context)
    }
  }

}
