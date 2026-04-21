package io.prophecy.pipelines.raw_bronze.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
