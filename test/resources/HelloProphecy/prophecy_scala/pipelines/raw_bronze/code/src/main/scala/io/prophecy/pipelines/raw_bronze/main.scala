package io.prophecy.pipelines.raw_bronze

import io.prophecy.libs._
import io.prophecy.pipelines.raw_bronze.config._
import io.prophecy.pipelines.raw_bronze.udfs.UDFs._
import io.prophecy.pipelines.raw_bronze.udfs.PipelineInitCode._
import io.prophecy.pipelines.raw_bronze.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_raw_customers     = raw_customers(context)
    val df_ReformatCustomers = ReformatCustomers(context, df_raw_customers)
    val df_raw_irs_zipcode   = raw_irs_zipcode(context)
    val df_ReformatIRS       = ReformatIRS(context,       df_raw_irs_zipcode)
    bronze_irs_zipcode(context, df_ReformatIRS)
    val df_raw_orders     = raw_orders(context)
    val df_ReformatOrders = ReformatOrders(context, df_raw_orders)
    bronze_orders(context,    df_ReformatOrders)
    bronze_customers(context, df_ReformatCustomers)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/raw_bronze")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/raw_bronze") {
      apply(context)
    }
  }

}
