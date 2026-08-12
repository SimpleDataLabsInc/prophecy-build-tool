package io.prophecy.pipelines.gold_top_customers.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
