package io.prophecy.pipelines.silver_customers_orders.graph

import io.prophecy.libs._
import io.prophecy.pipelines.silver_customers_orders.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object bronze_customers {

  def apply(context: Context): DataFrame =
    context.spark.read.table("`scottdemoscala`.`bronze_customers`")

}
