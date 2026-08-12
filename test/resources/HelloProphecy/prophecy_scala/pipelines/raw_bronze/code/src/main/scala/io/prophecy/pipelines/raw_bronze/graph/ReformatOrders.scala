package io.prophecy.pipelines.raw_bronze.graph

import io.prophecy.libs._
import io.prophecy.pipelines.raw_bronze.udfs.PipelineInitCode._
import io.prophecy.pipelines.raw_bronze.udfs.UDFs._
import io.prophecy.pipelines.raw_bronze.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object ReformatOrders {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("order_id").cast(IntegerType).as("order_id"),
      col("customer_id").cast(IntegerType).as("customer_id"),
      col("order_status"),
      col("order_category"),
      col("order_date").cast(DateType).as("order_date"),
      col("amount").cast(FloatType).as("amount")
    )

}
