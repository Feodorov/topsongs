package com.kfedorov.topsongs.businesslogic

import com.kfedorov.topsongs.businesslogic.utils._
import com.kfedorov.topsongs.model.{Track, TrackLogEntry}
import org.apache.spark.sql.Dataset

object TopSongs {

  /** @return `topNSongs` from `topNSessions` in the dataset. Tracks are selected randomly in case of ties. */
  def topSongs(
      dataset: Dataset[TrackLogEntry],
      topNSongs: Int = 10,
      topNSessions: Int = 50
  ): Dataset[Track] = {
    val sanitizedDataset = Sanitizer.sanitize(dataset)
    val datasetWithSessionId = SessionMatcher.assignSessionId(sanitizedDataset)
    val allTracksFromTopSessions =
      SessionRanking.tracksFromTopSessions(datasetWithSessionId, topNSessions)
    TrackRanking.mostPopularTracks(allTracksFromTopSessions, topNSongs)
  }
}
