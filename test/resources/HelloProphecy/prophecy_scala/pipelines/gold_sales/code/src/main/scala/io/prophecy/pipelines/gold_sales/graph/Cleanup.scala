package io.prophecy.pipelines.gold_sales.graph

import io.prophecy.libs._
import io.prophecy.pipelines.gold_sales.udfs.PipelineInitCode._
import io.prophecy.pipelines.gold_sales.udfs.UDFs._
import io.prophecy.pipelines.gold_sales.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Cleanup {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      datediff(col("account_open_date"), col("order_date"))
        .as("account_age_at_order_date"),
      col("order_id"),
      col("customer_id"),
      col("amount"),
      col("order_date"),
      col("zipcode"),
      col("account_flags")
    )

}
