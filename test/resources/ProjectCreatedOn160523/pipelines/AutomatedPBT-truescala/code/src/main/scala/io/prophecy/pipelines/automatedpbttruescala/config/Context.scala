package io.prophecy.pipelines.automatedpbttruescala.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
