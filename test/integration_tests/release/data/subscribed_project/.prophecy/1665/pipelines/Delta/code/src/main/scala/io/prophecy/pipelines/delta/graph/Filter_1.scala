package io.prophecy.pipelines.delta.graph

import io.prophecy.libs._
import io.prophecy.pipelines.delta.udfs.PipelineInitCode._
import io.prophecy.pipelines.delta.udfs.UDFs._
import io.prophecy.pipelines.delta.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_1 {
  def apply(context: Context, in: DataFrame): DataFrame = in.filter(lit(true))
}
