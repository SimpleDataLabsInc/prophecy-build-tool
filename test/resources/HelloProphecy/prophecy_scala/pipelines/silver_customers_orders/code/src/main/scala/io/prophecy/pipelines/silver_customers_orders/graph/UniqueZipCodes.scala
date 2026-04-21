package io.prophecy.pipelines.silver_customers_orders.graph

import io.prophecy.libs._
import io.prophecy.pipelines.silver_customers_orders.udfs.PipelineInitCode._
import io.prophecy.pipelines.silver_customers_orders.udfs.UDFs._
import io.prophecy.pipelines.silver_customers_orders.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object UniqueZipCodes {

  def apply(context: Context, in0: DataFrame): DataFrame = {
    in0.createOrReplaceTempView("in0")
    context.spark.sql(
      """SELECT 
    row_number() OVER (PARTITION BY 1 ORDER BY zipcode ASC) AS row_number, 
    zipcode 
FROM (
    SELECT distinct zipcode from in0 order by zipcode asc
)"""
    )
  }

}
