package io.prophecy.pipelines.delta.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
