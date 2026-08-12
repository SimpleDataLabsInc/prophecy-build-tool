package io.prophecy.pipelines.silver_zip_codes.graph

import io.prophecy.libs._
import io.prophecy.pipelines.silver_zip_codes.udfs.PipelineInitCode._
import io.prophecy.pipelines.silver_zip_codes.udfs.UDFs._
import io.prophecy.pipelines.silver_zip_codes.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object CalcIsHighIncome {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("zipcode"),
      col("state"),
      (lit(100) * col("high_income_returns") / col("all_returns"))
        .as("high_income_percent"),
      col("high_income_returns"),
      col("low_income_returns"),
      col("all_returns"),
      (col("high_income_returns") / col("all_returns") > lit(0.1d))
        .as("is_high_income")
    )

}
