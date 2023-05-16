package io.prophecy.pipelines.automatedpbttruescala.graph

import io.prophecy.libs._
import io.prophecy.pipelines.automatedpbttruescala.udfs.UDFs._
import io.prophecy.pipelines.automatedpbttruescala.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      concat(col("first_name"),
             col("last_name"),
             lit(Config.c_string),
             lit(Config.c_int),
             lit(Config.c_boolean)
      ).as("col1"),
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_open_date"),
      col("account_flags")
    )
  }

}
