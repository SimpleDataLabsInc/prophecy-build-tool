package io.prophecy.pipelines.silver_customers_orders.graph

import io.prophecy.libs._
import io.prophecy.pipelines.silver_customers_orders.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object silver_orders {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("`scottdemoscala`.`silver_orders`")

}
