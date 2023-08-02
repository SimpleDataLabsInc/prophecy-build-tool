package io.prophecy.pipelines.onlyone.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
