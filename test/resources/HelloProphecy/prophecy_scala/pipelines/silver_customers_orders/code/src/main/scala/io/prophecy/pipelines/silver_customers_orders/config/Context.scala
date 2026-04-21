package io.prophecy.pipelines.silver_customers_orders.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
