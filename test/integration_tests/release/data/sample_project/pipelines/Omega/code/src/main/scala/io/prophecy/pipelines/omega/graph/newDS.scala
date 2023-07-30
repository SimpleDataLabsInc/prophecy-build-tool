package io.prophecy.pipelines.omega.graph

import io.prophecy.libs._
import io.prophecy.pipelines.omega.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object newDS {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header",                                  true)
      .option("sep",                                     ",")
      .schema(StructType(Array(StructField("first_name", StringType, true))))
      .load("dbfs:/DatabricksSession/Output2.csv/")

}
