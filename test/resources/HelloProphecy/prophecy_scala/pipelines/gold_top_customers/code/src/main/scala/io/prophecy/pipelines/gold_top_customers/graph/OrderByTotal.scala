package io.prophecy.pipelines.gold_top_customers.graph

import io.prophecy.libs._
import io.prophecy.pipelines.gold_top_customers.udfs.PipelineInitCode._
import io.prophecy.pipelines.gold_top_customers.udfs.UDFs._
import io.prophecy.pipelines.gold_top_customers.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object OrderByTotal {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("total_spend").desc)

}
