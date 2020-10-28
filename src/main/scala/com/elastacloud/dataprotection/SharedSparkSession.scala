package com.elastacloud.dataprotection

import org.apache.spark.sql.SparkSession

trait SharedSparkSession {
  val spark: SparkSession = SparkSession.builder
    .appName("Data-Protection-Engine")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
}