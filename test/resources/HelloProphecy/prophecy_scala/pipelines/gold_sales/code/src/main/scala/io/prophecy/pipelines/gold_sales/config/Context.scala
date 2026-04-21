package io.prophecy.pipelines.gold_sales.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
