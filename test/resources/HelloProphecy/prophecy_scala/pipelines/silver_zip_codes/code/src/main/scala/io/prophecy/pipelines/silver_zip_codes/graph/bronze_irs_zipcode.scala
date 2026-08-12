package io.prophecy.pipelines.silver_zip_codes.graph

import io.prophecy.libs._
import io.prophecy.pipelines.silver_zip_codes.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object bronze_irs_zipcode {

  def apply(context: Context): DataFrame =
    context.spark.read.table("`scottdemoscala`.`bronze_irs_zipcode`")

}
