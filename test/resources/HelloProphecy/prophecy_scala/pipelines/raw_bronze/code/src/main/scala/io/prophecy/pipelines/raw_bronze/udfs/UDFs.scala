package io.prophecy.pipelines.raw_bronze.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("rand_zip_index", rand_zip_index)
    try registerAllUDFs(spark)
    catch {
      case _ => ()
    }
  }

  def rand_zip_index = {
    udf { (value: Int) =>
      import scala.util.Random
      val r = new Random(value)
      r.nextInt(27689)
    }
  }

}

object PipelineInitCode extends Serializable
