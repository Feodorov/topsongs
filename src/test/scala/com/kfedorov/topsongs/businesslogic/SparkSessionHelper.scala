package com.kfedorov.topsongs.businesslogic

import org.apache.spark.sql.SparkSession

trait SparkSessionHelper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("test")
      .getOrCreate()
  }
}
