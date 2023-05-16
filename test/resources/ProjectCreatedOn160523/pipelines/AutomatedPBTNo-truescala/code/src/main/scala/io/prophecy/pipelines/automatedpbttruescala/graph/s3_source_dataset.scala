package io.prophecy.pipelines.automatedpbttruescala.graph

import io.prophecy.libs._
import io.prophecy.pipelines.automatedpbttruescala.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object s3_source_dataset {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("parquet")
      .load("s3a://qa-prophecy/datasets/parquet/customers")

}
