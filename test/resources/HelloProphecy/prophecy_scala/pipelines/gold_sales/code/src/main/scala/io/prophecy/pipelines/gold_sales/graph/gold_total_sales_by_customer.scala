package io.prophecy.pipelines.gold_sales.graph

import io.prophecy.libs._
import io.prophecy.pipelines.gold_sales.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object gold_total_sales_by_customer {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("`scottdemoscala`.`gold_total_sales_by_customer`")

}
