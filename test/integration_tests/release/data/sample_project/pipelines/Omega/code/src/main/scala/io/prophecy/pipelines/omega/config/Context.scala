package io.prophecy.pipelines.omega.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
