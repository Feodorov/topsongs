package com.kfedorov.topsongs.businesslogic.utils

import com.kfedorov.topsongs.model.Track
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

private[businesslogic] object TrackRanking {

  /** @return top tracks by frequency. Tracks are selected randomly in case of ties. */
  def mostPopularTracks(
      dataset: Dataset[Track],
      topNSongs: Int = 10
  ): Dataset[Track] = {
    import dataset.sparkSession.implicits._

    dataset
      .groupBy(col("artistName"), col("trackName"))
      .count()
      .orderBy(col("count").desc)
      .limit(topNSongs)
      .select(col("artistName"), col("trackName"))
      .as[Track]
  }
}
