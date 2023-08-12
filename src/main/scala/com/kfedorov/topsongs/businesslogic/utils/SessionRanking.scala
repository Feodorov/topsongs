package com.kfedorov.topsongs.businesslogic.utils

import com.kfedorov.topsongs.model.{Track, TrackLogEntryWithSession}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank}

private[businesslogic] object SessionRanking {

  /** @return all tracks (with possible duplicates) within `topN` sessions by track count.
   *          Selects random session in case of ties */
  def tracksFromTopSessions(
      dataset: Dataset[TrackLogEntryWithSession],
      topN: Int = 50
  ): Dataset[Track] = {
    import dataset.sparkSession.implicits._

    val datasetWithSessionLength = dataset.withColumn(
      "sessionLength",
      count(col("sessionId")).over(Window.partitionBy("sessionId"))
    )

    val datasetWithRank = datasetWithSessionLength
      .withColumn(
        "sessionRank",
        dense_rank().over(Window.orderBy(col("sessionLength").desc))
      )

    datasetWithRank
      .where(col("sessionRank") <= topN)
      .select(col("artistName"), col("trackName"))
      .as[Track]
  }
}
