package io.prophecy.pipelines.raw_bronze.graph

import io.prophecy.libs._
import io.prophecy.pipelines.raw_bronze.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object raw_irs_zipcode {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("STATEFIPS", StringType, true),
            StructField("STATE",     StringType, true),
            StructField("zipcode",   StringType, true),
            StructField("agi_stub",  StringType, true),
            StructField("N1",        StringType, true),
            StructField("MARS1",     StringType, true),
            StructField("MARS2",     StringType, true),
            StructField("MARS4",     StringType, true),
            StructField("PREP",      StringType, true),
            StructField("N2",        StringType, true),
            StructField("NUMDEP",    StringType, true),
            StructField("A00100",    StringType, true),
            StructField("N02650",    StringType, true),
            StructField("A02650",    StringType, true),
            StructField("N00200",    StringType, true),
            StructField("A00200",    StringType, true),
            StructField("N00300",    StringType, true),
            StructField("A00300",    StringType, true),
            StructField("N00600",    StringType, true),
            StructField("A00600",    StringType, true),
            StructField("N00650",    StringType, true),
            StructField("A00650",    StringType, true),
            StructField("N00700",    StringType, true),
            StructField("A00700",    StringType, true),
            StructField("N00900",    StringType, true),
            StructField("A00900",    StringType, true),
            StructField("N01000",    StringType, true),
            StructField("A01000",    StringType, true),
            StructField("N01400",    StringType, true),
            StructField("A01400",    StringType, true),
            StructField("N01700",    StringType, true),
            StructField("A01700",    StringType, true),
            StructField("SCHF",      StringType, true),
            StructField("N02300",    StringType, true),
            StructField("A02300",    StringType, true),
            StructField("N02500",    StringType, true),
            StructField("A02500",    StringType, true),
            StructField("N26270",    StringType, true),
            StructField("A26270",    StringType, true),
            StructField("N02900",    StringType, true),
            StructField("A02900",    StringType, true),
            StructField("N03220",    StringType, true),
            StructField("A03220",    StringType, true),
            StructField("N03300",    StringType, true),
            StructField("A03300",    StringType, true),
            StructField("N03270",    StringType, true),
            StructField("A03270",    StringType, true),
            StructField("N03150",    StringType, true),
            StructField("A03150",    StringType, true),
            StructField("N03210",    StringType, true),
            StructField("A03210",    StringType, true),
            StructField("N03230",    StringType, true),
            StructField("A03230",    StringType, true),
            StructField("N03240",    StringType, true),
            StructField("A03240",    StringType, true),
            StructField("N04470",    StringType, true),
            StructField("A04470",    StringType, true),
            StructField("A00101",    StringType, true),
            StructField("N18425",    StringType, true),
            StructField("A18425",    StringType, true),
            StructField("N18450",    StringType, true),
            StructField("A18450",    StringType, true),
            StructField("N18500",    StringType, true),
            StructField("A18500",    StringType, true),
            StructField("N18300",    StringType, true),
            StructField("A18300",    StringType, true),
            StructField("N19300",    StringType, true),
            StructField("A19300",    StringType, true),
            StructField("N19700",    StringType, true),
            StructField("A19700",    StringType, true),
            StructField("N04800",    StringType, true),
            StructField("A04800",    StringType, true),
            StructField("N05800",    StringType, true),
            StructField("A05800",    StringType, true),
            StructField("N09600",    StringType, true),
            StructField("A09600",    StringType, true),
            StructField("N07100",    StringType, true),
            StructField("A07100",    StringType, true),
            StructField("N07300",    StringType, true),
            StructField("A07300",    StringType, true),
            StructField("N07180",    StringType, true),
            StructField("A07180",    StringType, true),
            StructField("N07230",    StringType, true),
            StructField("A07230",    StringType, true),
            StructField("N07240",    StringType, true),
            StructField("A07240",    StringType, true),
            StructField("N07220",    StringType, true),
            StructField("A07220",    StringType, true),
            StructField("N07260",    StringType, true),
            StructField("A07260",    StringType, true),
            StructField("N09400",    StringType, true),
            StructField("A09400",    StringType, true),
            StructField("N10600",    StringType, true),
            StructField("A10600",    StringType, true),
            StructField("N59660",    StringType, true),
            StructField("A59660",    StringType, true),
            StructField("N59720",    StringType, true),
            StructField("A59720",    StringType, true),
            StructField("N11070",    StringType, true),
            StructField("A11070",    StringType, true),
            StructField("N10960",    StringType, true),
            StructField("A10960",    StringType, true),
            StructField("N06500",    StringType, true),
            StructField("A06500",    StringType, true),
            StructField("N10300",    StringType, true),
            StructField("A10300",    StringType, true),
            StructField("N85330",    StringType, true),
            StructField("A85330",    StringType, true),
            StructField("N85300",    StringType, true),
            StructField("A85300",    StringType, true),
            StructField("N11901",    StringType, true),
            StructField("A11901",    StringType, true),
            StructField("N11902",    StringType, true),
            StructField("A11902",    StringType, true)
          )
        )
      )
      .load(
        "dbfs:/databricks-datasets/data.gov/irs_zip_code_data/data-001/2013_soi_zipcode_agi.csv"
      )

}
