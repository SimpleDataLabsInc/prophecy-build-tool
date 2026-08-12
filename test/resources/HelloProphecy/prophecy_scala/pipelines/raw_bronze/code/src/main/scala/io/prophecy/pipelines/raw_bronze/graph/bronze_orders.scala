package io.prophecy.pipelines.raw_bronze.graph

import io.prophecy.libs._
import io.prophecy.pipelines.raw_bronze.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object bronze_orders {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("`scottdemoscala`.`bronze_orders`")

}
