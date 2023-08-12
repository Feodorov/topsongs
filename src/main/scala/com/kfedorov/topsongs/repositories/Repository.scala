package com.kfedorov.topsongs.repositories

import com.kfedorov.topsongs.model.{Track, TrackLogEntry}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

trait Repository {
  def read(path: String): Dataset[TrackLogEntry]
  def write(path: String, dataset: Dataset[Track]): Unit
}

case class SparkRepository(sparkSession: SparkSession) extends Repository {
  import sparkSession.implicits._

  override def read(path: String): Dataset[TrackLogEntry] =
    sparkSession.read
      .option("sep", "\t")
      .schema(Encoders.product[TrackLogEntry].schema)
      .csv(path)
      .as[TrackLogEntry]

  override def write(path: String, dataset: Dataset[Track]): Unit =
    dataset
      .write
      .option("sep", "\t")
      .option("header", true)
      .csv(path)
}
