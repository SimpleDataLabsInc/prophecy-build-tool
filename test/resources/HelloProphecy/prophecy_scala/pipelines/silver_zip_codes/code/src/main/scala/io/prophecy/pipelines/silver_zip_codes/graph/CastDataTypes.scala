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

object CastDataTypes {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("zipcode"),
              col("agi_stub").cast(IntegerType).as("income_bracket"),
              col("N1").cast(IntegerType).as("num_returns"),
              col("STATE").as("state")
    )

}
