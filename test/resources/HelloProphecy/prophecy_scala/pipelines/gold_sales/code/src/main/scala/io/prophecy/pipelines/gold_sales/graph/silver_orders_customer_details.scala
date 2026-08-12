package io.prophecy.pipelines.gold_sales.graph

import io.prophecy.libs._
import io.prophecy.pipelines.gold_sales.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object silver_orders_customer_details {

  def apply(context: Context): DataFrame =
    context.spark.read.table("`scottdemoscala`.`silver_order_customer_details`")

}
