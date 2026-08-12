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

object IgnoreBadZip {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      (col("zipcode") =!= lit("00000"))
        .and(col("zipcode") =!= lit("99999"))
        .and(col("zipcode").isNotNull)
    )

}
