package io.prophecy.pipelines.silver_zip_codes.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
