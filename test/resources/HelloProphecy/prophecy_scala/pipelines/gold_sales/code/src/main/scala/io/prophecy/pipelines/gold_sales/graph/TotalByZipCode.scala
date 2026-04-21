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

object TotalByZipCode {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("zipcode"), col("order_date"))
      .agg(sum(col("amount")).as("total_amount"),
           avg(col("amount")).as("avg_amount"),
           count(col("order_id")).as("order_count")
      )

}
