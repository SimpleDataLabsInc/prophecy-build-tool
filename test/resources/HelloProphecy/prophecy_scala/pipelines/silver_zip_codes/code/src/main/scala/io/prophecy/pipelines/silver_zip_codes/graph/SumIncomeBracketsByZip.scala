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

object SumIncomeBracketsByZip {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("zipcode"))
      .agg(
        sum(expr("if((income_bracket = 6), num_returns, 0)"))
          .as("high_income_returns"),
        sum(expr("if((income_bracket < 6), num_returns, 0)"))
          .as("low_income_returns"),
        sum(col("num_returns")).as("all_returns"),
        first(col("state")).as("state")
      )

}
