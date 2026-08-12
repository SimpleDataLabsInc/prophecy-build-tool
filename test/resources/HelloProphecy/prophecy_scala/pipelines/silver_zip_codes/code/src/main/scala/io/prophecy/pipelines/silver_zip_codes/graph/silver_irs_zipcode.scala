package io.prophecy.pipelines.silver_zip_codes.graph

import io.prophecy.libs._
import io.prophecy.pipelines.silver_zip_codes.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object silver_irs_zipcode {

  def apply(context: Context, in: DataFrame): Unit =
    in.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("`scottdemoscala`.`silver_irs_zipcode`")

}
