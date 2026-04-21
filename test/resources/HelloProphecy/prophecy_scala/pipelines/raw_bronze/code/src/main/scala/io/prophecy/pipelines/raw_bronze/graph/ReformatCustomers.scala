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

object ReformatCustomers {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("customer_id").cast(IntegerType).as("customer_id"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_open_date").cast(DateType).as("account_open_date"),
      col("account_flags"),
      concat(col("first_name"), col("last_name")).as("full_name")
    )

}
